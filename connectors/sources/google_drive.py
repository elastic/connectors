#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import json
import os
from functools import cached_property, partial

import aiofiles
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiogoogle import Aiogoogle, HTTPError
from aiogoogle.auth.creds import ServiceAccountCreds
from aiogoogle.sessions.aiohttp_session import AiohttpSession

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    RetryStrategy,
    convert_to_b64,
    retryable,
)

RETRIES = 3
RETRY_INTERVAL = 2
FILE_SIZE_LIMIT = 10485760  # ~ 10 Megabytes

DRIVE_API_TIMEOUT = 1 * 60  # 1 min

ACCESS_CONTROL = "_allow_access_control"

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

DRIVE_ITEMS_FIELDS = "id,createdTime,driveId,modifiedTime,name,size,mimeType,fileExtension,webViewLink,permissions,owners,parents"

# Google Service Account JSON includes "universe_domain" key. That argument is not
# supported in aiogoogle library in version 5.3.0. The "universe_domain" key is allowed in
# service account JSON but will be dropped before being passed to aiogoogle.auth.creds.ServiceAccountCreds.
SERVICE_ACCOUNT_JSON_ALLOWED_KEYS = set(dict(ServiceAccountCreds()).keys()) | {
    "universe_domain"
}

# Export Google Workspace documents to TIKA compatible format, prefer 'text/plain' where possible to be
# mindful of the content extraction service resources
GOOGLE_MIME_TYPES_MAPPING = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.presentation": "text/plain",
    "application/vnd.google-apps.spreadsheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

GOOGLE_DRIVE_EMULATOR_HOST = os.environ.get("GOOGLE_DRIVE_EMULATOR_HOST")
RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.


class RetryableAiohttpSession(AiohttpSession):
    """A modified version of AiohttpSession from the aiogoogle library:
    (https://github.com/omarryhan/aiogoogle/blob/master/aiogoogle/sessions/aiohttp_session.py)

    The low-level send() method is wrapped with @retryable decorator that allows for retries
    with exponential backoff before failing the request.
    """

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def send(self, *args, **kwargs):
        return await super().send(*args, **kwargs)


class GoogleAPIClient:
    """A google client to handle api calls made to Google API."""

    def __init__(self, json_credentials, api_name, api_version, scopes, subject=None):
        """Initialize the ServiceAccountCreds class using which api calls will be made.

        Args:
            json_credentials (dict): Service account credentials json.
            api_name (str): Google API name.
            api_version (str): Google API version.
            scopes (list): Credential scopes.
            subject (str): For service accounts with domain-wide delegation enabled. A user
                           account to impersonate - e.g "admin@your-organization.com"
        """
        self.service_account_credentials = ServiceAccountCreds(
            scopes=scopes,
            subject=subject,
            **json_credentials,
        )
        self.api_name = api_name
        self.api_version = api_version
        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

    async def api_call_paged(
        self,
        resource,
        method,
        **kwargs,
    ):
        """Make a paged GET call to Google Drive API.

        Args:
            resource (aiogoogle.resource.Resource): Resource name for which the API call will be made.
            method (aiogoogle.resource.Method): Method available for the resource.

        Raises:
            exception: An instance of an exception class.

        Yields:
            async generator: Paginated response returned by the resource method.
        """

        async def _call_api(google_client, method_object, kwargs):
            page_with_next_attached = await google_client.as_service_account(
                method_object(**kwargs),
                full_res=True,
                timeout=DRIVE_API_TIMEOUT,
            )
            async for page_items in page_with_next_attached:
                yield page_items

        async for item in self._execute_api_call(resource, method, _call_api, kwargs):
            yield item

    async def api_call(
        self,
        resource,
        method,
        **kwargs,
    ):
        """Make a non-paged GET call to Google Drive API.

        Args:
            resource (aiogoogle.resource.Resource): Resource name for which the API call will be made.
            method (aiogoogle.resource.Method): Method available for the resource.

        Raises:
            exception: An instance of an exception class.

        Yields:
            dict: Response returned by the resource method.
        """

        async def _call_api(google_client, method_object, kwargs):
            yield await google_client.as_service_account(
                method_object(**kwargs), timeout=DRIVE_API_TIMEOUT
            )

        return await anext(self._execute_api_call(resource, method, _call_api, kwargs))

    async def _execute_api_call(self, resource, method, call_api_func, kwargs):
        """Execute the API call with common try/except logic.

        Args:
            resource (aiogoogle.resource.Resource): Resource name for which the API call will be made.
            method (aiogoogle.resource.Method): Method available for the resource.
            call_api_func (function): Function to call the API with specific logic.
            kwargs: Additional arguments for the API call.

        Raises:
            exception: An instance of an exception class.

        Yields:
            async generator: Response returned by the resource method.
        """
        try:
            async with Aiogoogle(
                service_account_creds=self.service_account_credentials,
                session_factory=RetryableAiohttpSession,
            ) as google_client:
                drive_client = await google_client.discover(
                    api_name=self.api_name, api_version=self.api_version
                )
                if RUNNING_FTEST and GOOGLE_DRIVE_EMULATOR_HOST:
                    drive_client.discovery_document["rootUrl"] = (
                        GOOGLE_DRIVE_EMULATOR_HOST + "/"
                    )

                resource_object = getattr(drive_client, resource)
                method_object = getattr(resource_object, method)

                async for item in call_api_func(google_client, method_object, kwargs):
                    yield item

        except AttributeError as exception:
            self._logger.error(
                f"Error occurred while generating the resource/method object for an API call. Error: {exception}"
            )
            raise
        except HTTPError as exception:
            self._logger.warning(
                f"Response code: {exception.res.status_code} Exception: {exception}."
            )
            raise
        except Exception as exception:
            self._logger.warning(f"Exception: {exception}.")
            raise


class GoogleDriveClient(GoogleAPIClient):
    """A google drive client to handle api calls made to Google Drive API."""

    def __init__(self, json_credentials):
        """Initialize the GoogleApiClient superclass.

        Args:
            json_credentials (dict): Service account credentials json.
        """
        super().__init__(
            json_credentials=json_credentials,
            api_name="drive",
            api_version="v3",
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )

    async def ping(self):
        return await self.api_call(resource="about", method="get", fields="kind")

    async def get_drives(self):
        """Fetch all shared drive (id, name) from Google Drive

        Yields:
            dict: Shared drive metadata.
        """

        async for drive in self.api_call_paged(
            resource="drives",
            method="list",
            fields="nextPageToken,drives(id,name)",
            pageSize=100,
        ):
            yield drive

    async def retrieve_all_drives(self):
        """Retrieves all shared drives from Google Drive

        Returns:
            dict: mapping between drive id and its name
        """
        drives = {}
        async for page in self.get_drives():
            drives_chunk = page.get("drives", [])
            for drive in drives_chunk:
                drives[drive["id"]] = drive["name"]

        return drives

    async def get_folders(self):
        """Fetch all folders (id, name, parent) from Google Drive

        Yields:
            dict: Folder metadata.
        """
        async for folder in self.api_call_paged(
            resource="files",
            method="list",
            corpora="allDrives",
            fields="nextPageToken,files(id,name,parents)",
            q=f"mimeType='{FOLDER_MIME_TYPE}' and trashed=false",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=1000,
        ):
            yield folder

    async def retrieve_all_folders(self):
        """Retrieves all folders from Google Drive

        Returns:
            dict: mapping between folder id and its (name, parents)
        """
        folders = {}
        async for page in self.get_folders():
            folders_chunk = page.get("files", [])
            for folder in folders_chunk:
                folders[folder["id"]] = {
                    "name": folder["name"],
                    "parents": folder.get("parents", None),
                }

        return folders

    async def list_files(self):
        """Get files from Google Drive. Files can have any type.

        Yields:
            dict: Documents from Google Drive.
        """
        async for file in self.api_call_paged(
            resource="files",
            method="list",
            corpora="allDrives",
            q="trashed=false",
            orderBy="modifiedTime desc",
            fields=f"files({DRIVE_ITEMS_FIELDS}),incompleteSearch,nextPageToken",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=100,
        ):
            yield file

    async def list_permissions(self, file_id):
        """Get permissions for a given file ID from Google Drive.

        Yields:
            dictionary: Permissions from Google Drive for a file.
        """
        async for permission in self.api_call_paged(
            resource="permissions",
            method="list",
            fileId=file_id,
            fields="permissions(type,emailAddress,domain),nextPageToken",
            supportsAllDrives=True,
            pageSize=100,
        ):
            yield permission


class GoogleAdminDirectoryClient(GoogleAPIClient):
    """A google admin directory client to handle api calls made to Google Admin API."""

    def __init__(self, json_credentials, subject):
        """Initialize the GoogleApiClient superclass.

        Args:
            json_credentials (dict): Service account credentials json.
            subject (str): For service accounts with domain-wide delegation enabled. A user
                           account to impersonate - e.g "admin@your-organization.com"
        """
        super().__init__(
            json_credentials=json_credentials,
            api_name="admin",
            api_version="directory_v1",
            scopes=[
                "https://www.googleapis.com/auth/admin.directory.group.readonly",
                "https://www.googleapis.com/auth/admin.directory.user.readonly",
            ],
            subject=subject,
        )
        self.domain = _get_domain_from_email(subject)

    async def list_users(self):
        """Get files from Google Drive. Files can have any type.

        Yields:
            dict: Documents from Google Drive.
        """
        async for user in self.api_call_paged(
            resource="users",
            method="list",
            domain=self.domain,
            fields="users(id,name,primaryEmail),nextPageToken",
        ):
            yield user

    async def list_groups_for_user(self, user_id):
        """Get files from Google Drive. Files can have any type.

        Yields:
            dict: Documents from Google Drive.
        """
        async for group in self.api_call_paged(
            resource="groups",
            method="list",
            userKey=user_id,
            fields="groups(email),nextPageToken",
        ):
            yield group


def _prefix_identity(prefix, identity):
    if prefix is None or identity is None:
        return None

    return f"{prefix}:{identity}"


def _prefix_group(group):
    return _prefix_identity("group", group)


def _prefix_user(user):
    return _prefix_identity("user", user)


def _prefix_domain(domain):
    return _prefix_identity("domain", domain)


def _is_user_permission(permission_type):
    return permission_type == "user"


def _is_group_permission(permission_type):
    return permission_type == "group"


def _is_domain_permission(permission_type):
    return permission_type == "domain"


def _is_anyone_permission(permission_type):
    return permission_type == "anyone"


def _get_domain_from_email(email):
    return email.split("@")[-1]


class GoogleDriveDataSource(BaseDataSource):
    """Google Drive"""

    name = "Google Drive"
    service_type = "google_drive"
    dls_enabled = True

    def __init__(self, configuration):
        """Set up the data source.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)

    def _set_internal_logger(self):
        self._google_drive_client.set_logger(self._logger)
        self._google_admin_directory_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Google Drive.

        Returns:
            dict: Default configuration.
        """
        return {
            "service_account_credentials": {
                "display": "textarea",
                "label": "Google Drive service account JSON",
                "order": 1,
                "type": "str",
                "value": "",
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 2,
                "tooltip": "Document level security ensures identities and permissions set in Google Drive are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
            "google_workspace_admin_email": {
                "depends_on": [{"field": "use_document_level_security", "value": True}],
                "display": "text",
                "label": "Google Workspace admin email",
                "order": 4,
                "tooltip": "In order to use Document Level Security you need to enable Google Workspace domain-wide delegation of authority for your service account. A service account with delegated authority can impersonate admin user with sufficient permissions to fetch all users and their corresponding permissions.",
                "type": "str",
                "value": "admin@your-organization.com",
            },
        }

    @cached_property
    def _google_drive_client(self):
        """Initialize and return the GoogleDriveClient

        Returns:
            GoogleDriveClient: An instance of the GoogleDriveClient.
        """
        self._validate_service_account_json()

        json_credentials = json.loads(self.configuration["service_account_credentials"])

        # Google Service Account JSON includes "universe_domain" key. That argument is not
        # supported in aiogoogle library, therefore we are skipping it from the credentials payload
        if "universe_domain" in json_credentials:
            json_credentials.pop("universe_domain")

        return GoogleDriveClient(json_credentials=json_credentials)

    @cached_property
    def _google_admin_directory_client(self):
        """Initialize and return the GoogleAdminDirectoryClient

        Returns:
            GoogleAdminDirectoryClient: An instance of the GoogleAdminDirectoryClient.
        """
        self._validate_service_account_json()

        json_credentials = json.loads(self.configuration["service_account_credentials"])

        # Google Service Account JSON includes "universe_domain" key. That argument is not
        # supported in aiogoogle library, therefore we are skipping it from the credentials payload
        if "universe_domain" in json_credentials:
            json_credentials.pop("universe_domain")

        return GoogleAdminDirectoryClient(
            json_credentials=json_credentials,
            subject=self.configuration["google_workspace_admin_email"],
        )

    async def validate_config(self):
        """Validates whether user inputs are valid or not for configuration field.

        Raises:
            Exception: The format of service account json is invalid.
        """
        self.configuration.check_valid()

        self._validate_service_account_json()

        if self._dls_enabled():
            if self.configuration["google_workspace_admin_email"] is None:
                raise ConfigurableFieldValueError(
                    "Google Workspace admin email cannot be empty when Document Level Security is enabled."
                )

    def _validate_service_account_json(self):
        """Validates whether service account JSON is a valid JSON string and
        checks for unexpected keys.

        Raises:
            ConfigurableFieldValueError: The service account json is ininvalid.
        """

        try:
            json_credentials = json.loads(
                self.configuration["service_account_credentials"]
            )
        except ValueError as e:
            raise ConfigurableFieldValueError(
                f"Google Drive service account is not a valid JSON. Exception: {e}"
            ) from e

        for key in json_credentials.keys():
            if key not in SERVICE_ACCOUNT_JSON_ALLOWED_KEYS:
                raise ConfigurableFieldValueError(
                    f"Google Drive service account JSON contains an unexpected key: '{key}'. Allowed keys are: {SERVICE_ACCOUNT_JSON_ALLOWED_KEYS}"
                )

    async def ping(self):
        """Verify the connection with Google Drive"""
        try:
            await self._google_drive_client.ping()
            self._logger.info("Successfully connected to the Google Drive.")
        except Exception:
            self._logger.exception("Error while connecting to the Google Drive.")
            raise

    def _dls_enabled(self):
        """Check if Document Level Security is enabled"""
        if self._features is None:
            return False

        if not self._features.document_level_security_enabled():
            return False

        return self.configuration.get("use_document_level_security", False)

    def access_control_query(self, access_control):
        return {
            "query": {
                "template": {"params": {"access_control": access_control}},
                "source": {
                    "bool": {
                        "filter": {
                            "bool": {
                                "should": [
                                    {
                                        "bool": {
                                            "must_not": {
                                                "exists": {"field": ACCESS_CONTROL}
                                            }
                                        }
                                    },
                                    {
                                        "terms": {
                                            f"{ACCESS_CONTROL}.enum": access_control
                                        }
                                    },
                                ]
                            }
                        }
                    }
                },
            }
        }

    async def _prepare_single_access_control_document(self, user):
        """Generate access control document for a single user. Fetch group memberships for a given user.
        Generate a user_access_control query that includes information about user email, groups and domain.

        Args:
            user (dict): User object.

        Yields:
            dict: Access control doc.
        """
        user_id = user.get("id")
        user_email = user.get("primaryEmail")
        user_domain = _get_domain_from_email(user_email)
        user_groups = []
        async for groups_page in self._google_admin_directory_client.list_groups_for_user(
            user_id
        ):
            for group in groups_page.get("groups", []):
                user_groups.append(group.get("email"))

        user_access_control = [
            _prefix_user(user_email),
            _prefix_domain(user_domain),
        ] + [_prefix_group(group) for group in user_groups]

        return {
            "_id": user_email,
            "identity": {
                "name": user.get("name").get("fullName"),
                "email": user_email,
            },
        } | self.access_control_query(access_control=user_access_control)

    async def _prepare_access_control_documents(self, users_page):
        """Generate access control document.

        Args:
            users_page (list): List with user objects.

        Yields:
            dict: Access control doc.
        """
        users = users_page.get("users", [])

        async def process_user(user, semaphore):
            async with semaphore:
                return await self._prepare_single_access_control_document(user=user)

        # Create the shared semaphore, it controls how many concurrent
        # groups/list requests can be open at any given time
        semaphore = asyncio.Semaphore(20)

        # Fetch user groups concurrently, it speeds up access control sync
        tasks = [process_user(user, semaphore) for user in users]
        prepared_ac_docs = await asyncio.gather(*tasks)

        for ac_doc in prepared_ac_docs:
            yield ac_doc

    async def get_access_control(self):
        """Yields an access control document for every user of Google Workspace organization.

        Yields:
             dict: dictionary representing a user access control document
        """

        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping access controls sync.")
            return

        async for user_page in self._google_admin_directory_client.list_users():
            async for access_control_doc in self._prepare_access_control_documents(
                users_page=user_page
            ):
                yield access_control_doc

    async def resolve_paths(self):
        """Builds a lookup between a folder id and its absolute path in Google Drive structure

        Returns:
            dict: mapping between folder id and its (name, parents, path)
        """
        folders = await self._google_drive_client.retrieve_all_folders()
        drives = await self._google_drive_client.retrieve_all_drives()

        # for paths let's treat drives as top level folders
        for id, drive_name in drives.items():
            folders[id] = {"name": drive_name, "parents": []}

        self._logger.info(f"Resolving folder paths for {len(folders)} folders")

        for folder in folders.values():
            path = [folder["name"]]  # Start with the folder name

            parents = folder["parents"]
            parent_id = parents[0] if parents else None

            # Traverse the parents until reaching the root or a missing parent
            while parent_id and parent_id in folders:
                parent_folder = folders[parent_id]
                # break the loop early if the path is resolved for the parent folder
                if "path" in parent_folder:
                    path.insert(0, parent_folder["path"])
                    break
                path.insert(
                    0, parent_folder["name"]
                )  # Insert parent name at the beginning
                parents = parent_folder["parents"]
                parent_id = parents[0] if parents else None

            folder["path"] = "/".join(path)  # Join path elements with '/'

        return folders

    async def _download_content(self, file, download_func):
        """Downloads the file from Google Drive and returns the encoded file content.

        Args:
            file (dict): Formatted file document.
            download_func (partial func): Partial function that gets the file content from Google Drive API.

        Returns:
            attachment, file_size (tuple): base64 encoded contnet of the file and size in bytes of the attachment
        """

        temp_file_name = ""
        file_name = file["name"]
        attachment, file_size = None, 0

        self._logger.debug(f"Downloading {file_name}")

        try:
            async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
                await download_func(
                    pipe_to=async_buffer,
                )

                temp_file_name = async_buffer.name

            await asyncio.to_thread(
                convert_to_b64,
                source=temp_file_name,
            )

            file_size = os.stat(temp_file_name).st_size

            async with aiofiles.open(file=temp_file_name, mode="r") as target_file:
                attachment = (await target_file.read()).strip()

            self._logger.debug(
                f"Downloaded {file_name} with the size of {file_size} bytes "
            )
        except Exception as e:
            self._logger.error(
                f"Exception encountered when processing file: {file_name}. Exception: {e}"
            )
        finally:
            if temp_file_name:
                await remove(str(temp_file_name))

        return attachment, file_size

    async def get_google_workspace_content(self, file, timestamp=None):
        """Exports Google Workspace documents to an allowed file type and extracts its text content.

        Shared Google Workspace documents are different than regular files. When shared from
        a different account they don't count against the user storage quota and therefore have size 0.
        They need to be exported to a supported file type before the content extraction phase.

        Args:
            file (dict): Formatted file document.
            timestamp (timestamp, optional): Timestamp of file last modified. Defaults to None.

        Returns:
            dict: Content document with id, timestamp & text
        """

        file_name, file_id, file_mime_type = file["name"], file["id"], file["mime_type"]

        document = {
            "_id": file_id,
            "_timestamp": file["_timestamp"],
        }

        attachment, file_size = await self._download_content(
            file=file,
            download_func=partial(
                self._google_drive_client.api_call,
                resource="files",
                method="export",
                fileId=file_id,
                mimeType=GOOGLE_MIME_TYPES_MAPPING[file_mime_type],
            ),
        )

        # We need to do sanity size after downloading the file because:
        # 1. We use files/export endpoint which converts large media-rich google slides/docs
        #    into text/plain format. We usually we end up with tiny .txt files.
        # 2. Google will ofter report the Google Workspace shared documents to have size 0
        #    as they don't count against user's storage quota.
        if file_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {file_size} of file {file_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding the file content"
            )
            return

        document["_attachment"] = attachment
        return document

    async def get_generic_file_content(self, file, timestamp=None):
        """Extracts the content from allowed file types supported by Apache Tika.

        Args:
            file (dict): Formatted file document .
            timestamp (timestamp, optional): Timestamp of file last modified. Defaults to None.

        Returns:
            dict: Content document with id, timestamp & text
        """

        file_size = int(file["size"])

        if file_size == 0:
            return

        file_name, file_id, file_extension = (
            file["name"],
            file["id"],
            f".{file['file_extension']}",
        )

        if file_extension not in TIKA_SUPPORTED_FILETYPES:
            self._logger.debug(f"{file_name} can't be extracted")
            return

        if file_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {file_size} of file {file_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding the file content"
            )
            return

        document = {
            "_id": file_id,
            "_timestamp": file["_timestamp"],
        }

        attachment, _ = await self._download_content(
            file=file,
            download_func=partial(
                self._google_drive_client.api_call,
                resource="files",
                method="get",
                fileId=file_id,
                supportsAllDrives=True,
                alt="media",
            ),
        )

        document["_attachment"] = attachment
        return document

    async def get_content(self, file, timestamp=None, doit=None):
        """Extracts the content from a file file.

        Args:
            file (dict): Formatted file document.
            timestamp (timestamp, optional): Timestamp of file last_modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to None.

        Returns:
            dict: Content document with id, timestamp & text
        """

        if not doit:
            return

        file_mime_type = file["mime_type"]

        if file_mime_type in GOOGLE_MIME_TYPES_MAPPING:
            # Get content from native google workspace files (docs, slides, sheets)
            return await self.get_google_workspace_content(file, timestamp=timestamp)
        else:
            # Get content from all other file types
            return await self.get_generic_file_content(file, timestamp=timestamp)

    async def _get_permissions_on_shared_drive(self, file_id):
        """Retrieves the access permissions on a shared drive for the given file ID.

        Args:
            file_id (str): The ID of the file.

        Returns:
            list: A list of access permissions on the shared drive for a file.
        """

        access_controls = []

        async for permissions_page in self._google_drive_client.list_permissions(
            file_id
        ):
            permissions = permissions_page.get("permissions", [])
            access_controls_page = self._process_permissions(permissions)
            access_controls.extend(access_controls_page)

        return access_controls

    def _get_permissions_on_my_drive(self, file):
        """Formats the access permissions on a my drive for the given object.

        Args:
            file (dict): The metadata of Google Drive file.

        Returns:
            list: A list of access permissions on my drive for a given file.
        """

        permissions = file.get("permissions", [])
        access_controls = self._process_permissions(permissions)
        return access_controls

    def _process_permissions(self, permissions):
        """Formats the access permission list for Google Drive object.

        Args:
            permissions (list): List of permissions of Google Drive file returned from API.

        Returns:
            list: A list of processed access permissions for a given file.
        """
        processed_permissions = []

        for permission in permissions:
            permission_type = permission["type"]
            access_permission = None

            if _is_user_permission(permission_type):
                access_permission = _prefix_user(permission.get("emailAddress"))
            elif _is_group_permission(permission_type):
                access_permission = _prefix_group(permission.get("emailAddress"))
            elif _is_domain_permission(permission_type):
                access_permission = _prefix_domain(permission.get("domain"))
            elif _is_anyone_permission(permission_type):
                access_permission = "anyone"
            else:
                self._logger.warning(
                    f"Unknown Google Drive permission type: {permission_type}."
                )

            processed_permissions.append(access_permission)

        return processed_permissions

    async def prepare_file(self, file, paths):
        """Apply key mappings to the file document.

        Args:
            file (dict): File metadata returned from the Drive.

        Returns:
            dict: Formatted file metadata.
        """

        file_id = file.get("id")

        file_document = {
            "_id": file_id,
            "created_at": file.get("createdTime"),
            "last_updated": file.get("modifiedTime"),
            "name": file.get("name"),
            "size": file.get("size") or 0,  # handle folders and shortcuts
            "_timestamp": file.get("modifiedTime"),
            "mime_type": file.get("mimeType"),
            "file_extension": file.get("fileExtension"),
            "url": file.get("webViewLink"),
        }

        # record "file" or "folder" type
        file_document["type"] = (
            "folder" if file.get("mimeType") == FOLDER_MIME_TYPE else "file"
        )

        # populate owner-related fields if owner is present in the response from the Drive API
        owners = file.get("owners", None)
        if owners:
            first_owner = file["owners"][0]
            file_document["author"] = ",".join(
                [owner["displayName"] for owner in owners]
            )
            file_document["created_by"] = first_owner["displayName"]
            file_document["created_by_email"] = first_owner["emailAddress"]

        # handle last modifying user metadata
        last_modifying_user = file.get("lastModifyingUser", None)
        if last_modifying_user:
            file_document["updated_by"] = last_modifying_user.get("displayName", None)
            file_document["updated_by_email"] = last_modifying_user.get(
                "emailAddress", None
            )
            file_document["updated_by_photo_url"] = last_modifying_user.get(
                "photoLink", None
            )

        # determine the path on google drive, note that google workspace files won't have a path
        file_parents = file.get("parents", None)
        if file_parents and file_parents[0] in paths:
            file_document["path"] = f"{paths[file_parents[0]]['path']}/{file['name']}"

        # mark the document if it is on shared drive
        file_drive_id = file.get("driveId", None)
        shared_drive = paths.get(file_drive_id, None)
        if shared_drive:
            file_document["shared_drive"] = shared_drive.get("name")

        if self._dls_enabled():
            # Getting permissions works differenty for files on my drive and files on shared drives.
            # Read more: https://developers.google.com/drive/api/guides/shared-drives-diffs
            if shared_drive:
                file_document[
                    ACCESS_CONTROL
                ] = await self._get_permissions_on_shared_drive(file_id=file_id)
            else:
                file_document[ACCESS_CONTROL] = self._get_permissions_on_my_drive(
                    file=file
                )

        return file_document

    async def prepare_files(self, files_page, paths):
        """Generate file document.

        Args:
            files_page (dict): Dictionary contains files list.

        Yields:
            dict: File with formatted metadata.
        """
        files = files_page.get("files", [])

        async def process_file(file, semaphore):
            async with semaphore:
                return await self.prepare_file(file=file, paths=paths)

        # Create the shared semaphore, it controls how many concurrent
        # permissions/list requests can be open at any given time
        semaphore = asyncio.Semaphore(20)

        # Fetch file permissions concurrently
        tasks = [process_file(file, semaphore) for file in files]
        prepared_files = await asyncio.gather(*tasks)

        for file in prepared_files:
            yield file

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch Google Drive objects in an async manner.

        Args:
            filtering (optional): Advenced filtering rules. Defaults to None.

        Yields:
            dict, partial: dict containing meta-data of the Google Drive objects,
                                partial download content function
        """

        # Build a path lookup, parentId -> parent path
        resolved_paths = await self.resolve_paths()

        async for files_page in self._google_drive_client.list_files():
            async for file in self.prepare_files(
                files_page=files_page, paths=resolved_paths
            ):
                yield file, partial(self.get_content, file)
