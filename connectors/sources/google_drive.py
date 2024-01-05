#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from functools import cached_property, partial

from aiogoogle import HTTPError

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.sources.google import (
    GoogleServiceAccountClient,
    UserFields,
    load_service_account_json,
    remove_universe_domain,
    validate_service_account_json,
)
from connectors.utils import (
    EMAIL_REGEX_PATTERN,
    validate_email_address,
)

GOOGLE_DRIVE_SERVICE_NAME = "Google Drive"
GOOGLE_ADMIN_DIRECTORY_SERVICE_NAME = "Google Admin Directory"

RETRIES = 3
RETRY_INTERVAL = 2

GOOGLE_API_MAX_CONCURRENCY = 25  # Max open connections to Google API

DRIVE_API_TIMEOUT = 1 * 60  # 1 min

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

DRIVE_ITEMS_FIELDS = "id,createdTime,driveId,modifiedTime,name,size,mimeType,fileExtension,webViewLink,owners,parents"
DRIVE_ITEMS_FIELDS_WITH_PERMISSIONS = f"{DRIVE_ITEMS_FIELDS},permissions"

# Export Google Workspace documents to TIKA compatible format, prefer 'text/plain' where possible to be
# mindful of the content extraction service resources
GOOGLE_MIME_TYPES_MAPPING = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.presentation": "text/plain",
    "application/vnd.google-apps.spreadsheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


class GoogleDriveClient(GoogleServiceAccountClient):
    """A google drive client to handle api calls made to Google Drive API."""

    def __init__(self, json_credentials, subject=None):
        """Initialize the GoogleApiClient superclass.

        Args:
            json_credentials (dict): Service account credentials json.
        """

        remove_universe_domain(json_credentials)
        if subject:
            json_credentials["subject"] = subject

        super().__init__(
            json_credentials=json_credentials,
            api="drive",
            api_version="v3",
            scopes=[
                "https://www.googleapis.com/auth/drive.readonly",
                "https://www.googleapis.com/auth/drive.metadata.readonly",
            ],
            api_timeout=DRIVE_API_TIMEOUT,
        )

    async def ping(self):
        return await self.api_call(resource="about", method="get", fields="kind")

    async def list_drives(self):
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

    async def get_all_drives(self):
        """Retrieves all shared drives from Google Drive

        Returns:
            dict: mapping between drive id and its name
        """
        drives = {}
        async for page in self.list_drives():
            drives_chunk = page.get("drives", [])
            for drive in drives_chunk:
                drives[drive["id"]] = drive["name"]

        return drives

    async def list_folders(self):
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

    async def get_all_folders(self):
        """Retrieves all folders from Google Drive

        Returns:
            dict: mapping between folder id and its (name, parents)
        """
        folders = {}
        async for page in self.list_folders():
            folders_chunk = page.get("files", [])
            for folder in folders_chunk:
                folders[folder["id"]] = {
                    "name": folder["name"],
                    "parents": folder.get("parents", None),
                }

        return folders

    async def list_files(self, fetch_permissions=False):
        """Get files from Google Drive. Files can have any type.

        Args:
            include_permissions (bool): flag to select permissions in the request query

        Yields:
            dict: Documents from Google Drive.
        """

        files_fields = (
            DRIVE_ITEMS_FIELDS_WITH_PERMISSIONS
            if fetch_permissions
            else DRIVE_ITEMS_FIELDS
        )

        async for file in self.api_call_paged(
            resource="files",
            method="list",
            corpora="allDrives",
            q="trashed=false",
            orderBy="modifiedTime desc",
            fields=f"files({files_fields}),incompleteSearch,nextPageToken",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=100,
        ):
            yield file

    async def list_files_from_my_drive(self, fetch_permissions=False):
        """Retrieves files from Google Drive, with an option to fetch permissions (DLS).

        This function optimizes the retrieval process based on the 'fetch_permissions' flag.
        If 'fetch_permissions' is True, the function filters for files the user can edit
        ("trashed=false and 'me' in writers") as permission fetching requires write access.
        If 'fetch_permissions' is False, it simply filters out trashed files ("trashed=false"),
        allowing a broader file retrieval.

        Args:
            include_permissions (bool): flag to select permissions in the request query

        Yields:
            dict: Documents from Google Drive.
        """

        if fetch_permissions:
            files_fields = DRIVE_ITEMS_FIELDS_WITH_PERMISSIONS
            # Google Drive API required write access to fetch file's permissions
            list_query = "trashed=false and 'me' in writers"
        else:
            files_fields = DRIVE_ITEMS_FIELDS
            list_query = "trashed=false"

        async for file in self.api_call_paged(
            resource="files",
            method="list",
            corpora="user",
            q=list_query,
            orderBy="modifiedTime desc",
            fields=f"files({files_fields}),incompleteSearch,nextPageToken",
            includeItemsFromAllDrives=False,
            supportsAllDrives=False,
            pageSize=100,
        ):
            yield file

    async def list_permissions(self, file_id):
        """Get permissions for a given file ID from Google Drive.

        Args:
            file_id (str): File ID

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


class GoogleAdminDirectoryClient(GoogleServiceAccountClient):
    """A google admin directory client to handle api calls made to Google Admin API."""

    def __init__(self, json_credentials, subject):
        """Initialize the GoogleApiClient superclass.

        Args:
            json_credentials (dict): Service account credentials json.
            subject (str): For service accounts with domain-wide delegation enabled. A user
                           account to impersonate - e.g "admin@your-organization.com"
        """

        remove_universe_domain(json_credentials)
        if subject:
            json_credentials["subject"] = subject

        super().__init__(
            json_credentials=json_credentials,
            api="admin",
            api_version="directory_v1",
            scopes=[
                "https://www.googleapis.com/auth/admin.directory.group.readonly",
                "https://www.googleapis.com/auth/admin.directory.user.readonly",
            ],
            api_timeout=DRIVE_API_TIMEOUT,
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
            fields="kind,users(id,name,primaryEmail),nextPageToken",
        ):
            yield user

    async def users(self):
        async for users_page in self.list_users():
            for user in users_page.get("users", []):
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
            fields="kind,groups(email),nextPageToken",
        ):
            yield group


def _prefix_group(group):
    return prefix_identity("group", group)


def _prefix_user(user):
    return prefix_identity("user", user)


def _prefix_domain(domain):
    return prefix_identity("domain", domain)


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
        if self._domain_wide_delegation_sync_enabled() or self._dls_enabled():
            self.google_admin_directory_client.set_logger(self._logger)

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
                "sensitive": True,
                "order": 1,
                "tooltip": "This connectors authenticates as a service account to synchronize content from Google Drive.",
                "type": "str",
            },
            "use_domain_wide_delegation_for_sync": {
                "display": "toggle",
                "label": "Use domain-wide delegation for data sync",
                "order": 2,
                "tooltip": "Enable domain-wide delegation to automatically sync content from all shared and personal drives in the Google workspace. This eliminates the need to manually share Google Drive data with your service account, though it may increase sync time. If disabled, only items and folders manually shared with the service account will be synced. Please refer to the connector documentation to ensure domain-wide delegation is correctly configured and has the appropriate scopes.",
                "type": "bool",
                "value": False,
            },
            "google_workspace_admin_email_for_data_sync": {
                "depends_on": [
                    {"field": "use_domain_wide_delegation_for_sync", "value": True}
                ],
                "display": "text",
                "label": "Google Workspace admin email",
                "order": 3,
                "tooltip": "Provide the admin email to be used with domain-wide delegation for data sync. This email enables the connector to utilize the Admin Directory API for listing organization users. Please refer to the connector documentation to ensure domain-wide delegation is correctly configured and has the appropriate scopes.",
                "type": "str",
                "validations": [{"type": "regex", "constraint": EMAIL_REGEX_PATTERN}],
            },
            "google_workspace_email_for_shared_drives_sync": {
                "depends_on": [
                    {"field": "use_domain_wide_delegation_for_sync", "value": True}
                ],
                "display": "text",
                "label": "Google Workspace email for syncing shared drives",
                "order": 4,
                "tooltip": "Provide the Google Workspace user email for discovery and syncing of shared drives. Only the shared drives this user has access to will be synced.",
                "type": "str",
                "validations": [{"type": "regex", "constraint": EMAIL_REGEX_PATTERN}],
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 5,
                "tooltip": "Document level security ensures identities and permissions set in Google Drive are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
            "google_workspace_admin_email": {
                "depends_on": [
                    {"field": "use_document_level_security", "value": True},
                    {"field": "use_domain_wide_delegation_for_sync", "value": False},
                ],
                "display": "text",
                "label": "Google Workspace admin email",
                "order": 6,
                "tooltip": "In order to use Document Level Security you need to enable Google Workspace domain-wide delegation of authority for your service account. A service account with delegated authority can impersonate admin user with sufficient permissions to fetch all users and their corresponding permissions. Please refer to the connector documentation to ensure domain-wide delegation is correctly configured and has the appropriate scopes.",
                "type": "str",
                "validations": [{"type": "regex", "constraint": EMAIL_REGEX_PATTERN}],
            },
            "max_concurrency": {
                "default_value": GOOGLE_API_MAX_CONCURRENCY,
                "display": "numeric",
                "label": "Maximum concurrent HTTP requests",
                "order": 7,
                "required": False,
                "tooltip": "This setting determines the maximum number of concurrent HTTP requests sent to the Google API to fetch data. Increasing this value can improve data retrieval speed, but it may also place higher demands on system resources and network bandwidth.",
                "type": "int",
                "ui_restrictions": ["advanced"],
                "validations": [{"type": "greater_than", "constraint": 0}],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 8,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }

    def google_drive_client(self, impersonate_email=None):
        """
        Initialize and return an instance of the GoogleDriveClient.

        This method sets up a Google Drive client using service account credentials.
        If an impersonate_email is provided, the client will be set up for domain-wide
        delegation, allowing it to impersonate the provided user account within
        a Google Workspace domain.

        GoogleDriveClient needs to be reinstantiated for different values of impersonate_email,
        therefore the client is not cached.

        Args:
            impersonate_email (str, optional): The email of the user account to impersonate.
                Defaults to None, in which case no impersonation is set up (in case domain-wide delegation is disabled).

        Returns:
            GoogleDriveClient: An initialized instance of the GoogleDriveClient.
        """

        service_account_credentials = self.configuration["service_account_credentials"]

        validate_service_account_json(
            service_account_credentials, GOOGLE_DRIVE_SERVICE_NAME
        )

        json_credentials = load_service_account_json(
            service_account_credentials, GOOGLE_DRIVE_SERVICE_NAME
        )

        # handle domain-wide delegation
        user_account_impersonation = (
            {"subject": impersonate_email} if impersonate_email else {}
        )

        drive_client = GoogleDriveClient(
            json_credentials=json_credentials, **user_account_impersonation
        )

        drive_client.set_logger(self._logger)

        return drive_client

    @cached_property
    def google_admin_directory_client(self):
        """Initialize and return the GoogleAdminDirectoryClient

        Returns:
            GoogleAdminDirectoryClient: An instance of the GoogleAdminDirectoryClient.
        """
        service_account_credentials = self.configuration["service_account_credentials"]

        validate_service_account_json(
            service_account_credentials, GOOGLE_ADMIN_DIRECTORY_SERVICE_NAME
        )

        self._validate_google_workspace_admin_email()

        json_credentials = load_service_account_json(
            service_account_credentials, GOOGLE_ADMIN_DIRECTORY_SERVICE_NAME
        )

        directory_client = GoogleAdminDirectoryClient(
            json_credentials=json_credentials,
            subject=self._get_google_workspace_admin_email(),
        )

        directory_client.set_logger(self._logger)

        return directory_client

    async def validate_config(self):
        """Validates whether user inputs are valid or not for configuration field.

        Raises:
            Exception: The format of service account json is invalid.
        """
        await super().validate_config()

        validate_service_account_json(
            self.configuration["service_account_credentials"], GOOGLE_DRIVE_SERVICE_NAME
        )
        self._validate_google_workspace_admin_email()
        self._validate_google_workspace_email_for_shared_drives_sync()

    def _validate_google_workspace_admin_email(self):
        """
        This method is used to validate the Google Workspace admin email address when Document Level Security (DLS) is enabled
        for the current configuration. The email address should not be empty, and it should have a valid email format (no
        whitespace and a valid domain).

        Raises:
            ConfigurableFieldValueError: If the Google Workspace admin email is empty when DLS is enabled,
                or if the email is malformed or contains whitespace characters.

        Note:
            - This function assumes that `_dls_enabled()` is used to determine whether Document Level Security is enabled.
            - The email address is validated using a basic regular expression pattern which might not cover all
            possible valid email formats. For more accurate validation, consider using a comprehensive email validation
            library or service.

        """
        if self._dls_enabled():
            google_workspace_admin_email = self._get_google_workspace_admin_email()

            if google_workspace_admin_email is None:
                msg = "Google Workspace admin email cannot be empty."
                raise ConfigurableFieldValueError(msg)

            if not validate_email_address(google_workspace_admin_email):
                msg = "Google Workspace admin email is malformed or contains whitespace characters."
                raise ConfigurableFieldValueError(msg)

    def _validate_google_workspace_email_for_shared_drives_sync(self):
        """
        Validates the Google Workspace email address specified for shared drives synchronization.

        When 'Use domain-wide delegation for data sync' is enabled, this method ensures that the
        email address provided for syncing shared drives is neither empty nor malformed.

        Raises:
            ConfigurableFieldValueError:
                - If the Google Workspace email for shared drives sync is empty when the domain-wide delegation sync is enabled.
                - If the provided email address is malformed or contains whitespace characters.
        """
        if self._domain_wide_delegation_sync_enabled():
            google_workspace_email = self.configuration[
                "google_workspace_email_for_shared_drives_sync"
            ]

            if google_workspace_email is None:
                msg = "Google Workspace admin email for shared drives sync cannot be empty when 'Use domain-wide delegation for data sync' is enabled."
                raise ConfigurableFieldValueError(msg)

            if not validate_email_address(google_workspace_email):
                msg = "Google Workspace email for shared drives sync is malformed or contains whitespace characters."
                raise ConfigurableFieldValueError(msg)

    async def ping(self):
        """Verify the connection with Google Drive"""
        try:
            if self._domain_wide_delegation_sync_enabled():
                admin_email = self._get_google_workspace_admin_email()
                await self.google_drive_client(impersonate_email=admin_email).ping()
            else:
                await self.google_drive_client().ping()
            self._logger.info("Successfully connected to the Google Drive.")
        except Exception:
            self._logger.exception("Error while connecting to the Google Drive.")
            raise

    def _get_google_workspace_admin_email(self):
        """
        Retrieves the Google Workspace admin email based on the current configuration.

        If domain-wide delegation for data sync is enabled, this method will return the admin email
        provided for shared drives sync. If Document Level Security (DLS) is enabled but not domain-wide
        delegation, it will return the the admin email specified for DLS.

        This ensures that if the admin email for domain-wide delegation is provided, it is utilized
        for both sync and DLS without requiring the same email to be provided again for DLS.

        Returns:
            str or None: The Google Workspace admin email based on the current configuration or None if
            neither domain-wide delegation nor DLS is enabled.
        """

        if self._domain_wide_delegation_sync_enabled():
            return self.configuration["google_workspace_admin_email_for_data_sync"]
        elif self._dls_enabled():
            return self.configuration["google_workspace_admin_email"]
        else:
            return None

    def _google_google_workspace_email_for_shared_drives_sync(self):
        return self.configuration.get("google_workspace_email_for_shared_drives_sync")

    def _dls_enabled(self):
        """Check if Document Level Security is enabled"""
        if self._features is None:
            return False

        if not self._features.document_level_security_enabled():
            return False

        return bool(self.configuration.get("use_document_level_security", False))

    def _domain_wide_delegation_sync_enabled(self):
        """Check if Domain Wide delegation sync is enabled"""

        return bool(
            self.configuration.get("use_domain_wide_delegation_for_sync", False)
        )

    def _max_concurrency(self):
        """Get maximum concurrent open connections from the user config"""
        return self.configuration.get("max_concurrency") or GOOGLE_API_MAX_CONCURRENCY

    def access_control_query(self, access_control):
        return es_access_control_query(access_control)

    async def _process_items_concurrently(self, items, process_item_func):
        """Process a list of items concurrently using a semaphore for concurrency control.

        This function applies the `process_item_func` to each item in the `items` list
        using a semaphore to control the level of concurrency.

        Args:
            items (list): List of items to process.
            process_item_func (function): The function to be called for each item.
                This function should be asynchronous.

        Returns:
            list: A list containing the results of processing each item.

        Note:
            The `process_item_func` should be an asynchronous function that takes
            one argument (item) and returns a coroutine.

        """

        async def process_item(item, semaphore):
            async with semaphore:
                return await process_item_func(item)

        # Create a semaphore with a concurrency limit of max_concurrency in the config
        semaphore = asyncio.Semaphore(self._max_concurrency())

        # Create tasks for each item, processing them concurrently with the semaphore
        tasks = [process_item(item, semaphore) for item in items]

        # Gather the results of all tasks concurrently
        return await asyncio.gather(*tasks)

    async def prepare_single_access_control_document(self, user):
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
        async for groups_page in self.google_admin_directory_client.list_groups_for_user(
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

    async def prepare_access_control_documents(self, users_page):
        """Generate access control document.

        Args:
            users_page (list): List with user objects.

        Yields:
            dict: Access control doc.
        """
        users = users_page.get("users", [])
        prepared_ac_docs = await self._process_items_concurrently(
            users, self.prepare_single_access_control_document
        )

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

        async for user_page in self.google_admin_directory_client.list_users():
            async for access_control_doc in self.prepare_access_control_documents(
                users_page=user_page
            ):
                yield access_control_doc

    async def resolve_paths(self, google_drive_client=None):
        """Builds a lookup between a folder id and its absolute path in Google Drive structure

        Returns:
            dict: mapping between folder id and its (name, parents, path)
        """
        if not google_drive_client:
            google_drive_client = self.google_drive_client()

        folders = await google_drive_client.get_all_folders()
        drives = await google_drive_client.get_all_drives()

        # for paths let's treat drives as top level folders
        for id_, drive_name in drives.items():
            folders[id_] = {"name": drive_name, "parents": []}

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

    async def _download_content(self, file, file_extension, download_func):
        """Downloads the file from Google Drive and returns the encoded file content.

        Args:
            file (dict): Formatted file document.
            download_func (partial func): Partial function that gets the file content from Google Drive API.

        Returns:
            attachment, file_size (tuple): base64 encoded contnet of the file and size in bytes of the attachment
        """

        file_name = file["name"]
        attachment, body, file_size = None, None, 0

        async with self.create_temp_file(file_extension) as async_buffer:
            await download_func(
                pipe_to=async_buffer,
            )
            await async_buffer.close()

            doc = await self.handle_file_content_extraction(
                {}, file_name, async_buffer.name
            )
            attachment = doc.get("_attachment")
            body = doc.get("body")

        return attachment, body, file_size

    async def get_google_workspace_content(self, client, file, timestamp=None):
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

        file_name, file_id, file_mime_type, file_extension = (
            file["name"],
            file["id"],
            file["mime_type"],
            f".{file['file_extension']}",
        )

        document = {
            "_id": file_id,
            "_timestamp": file["_timestamp"],
        }
        attachment, body, file_size = await self._download_content(
            file=file,
            file_extension=file_extension,
            download_func=partial(
                client.api_call,
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
        if not self.is_file_size_within_limit(file_size, file_name):
            return

        if attachment is not None:
            document["_attachment"] = attachment
        elif body is not None:
            document["body"] = body

        return document

    async def get_generic_file_content(self, client, file, timestamp=None):
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

        if not self.can_file_be_downloaded(file_extension, file_name, file_size):
            return

        document = {
            "_id": file_id,
            "_timestamp": file["_timestamp"],
        }
        attachment, body, _ = await self._download_content(
            file=file,
            file_extension=file_extension,
            download_func=partial(
                client.api_call,
                resource="files",
                method="get",
                fileId=file_id,
                supportsAllDrives=True,
                alt="media",
            ),
        )

        if attachment is not None:
            document["_attachment"] = attachment
        elif body is not None:
            document["body"] = body

        return document

    async def get_content(self, client, file, timestamp=None, doit=None):
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
            return await self.get_google_workspace_content(
                client, file, timestamp=timestamp
            )
        else:
            # Get content from all other file types
            return await self.get_generic_file_content(
                client, file, timestamp=timestamp
            )

    async def _get_permissions_on_shared_drive(self, client, file_id):
        """Retrieves the permissions on a shared drive for the given file ID.

        Args:
            file_id (str): The ID of the file.

        Returns:
            list: A list of permissions on the shared drive for a file.
        """

        permissions = []

        async for permissions_page in client.list_permissions(file_id):
            permissions.extend(permissions_page.get("permissions", []))

        return permissions

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
                # Continue so that 'None' permission is not appended to processed_permissions list
                continue

            processed_permissions.append(access_permission)

        return processed_permissions

    async def prepare_file(self, client, file, paths):
        """Apply key mappings to the file document.

        Args:
            file (dict): File metadata returned from the Drive.

        Returns:
            dict: Formatted file metadata.
        """

        file_id, file_name = file.get("id"), file.get("name")

        file_document = {
            "_id": file_id,
            "created_at": file.get("createdTime"),
            "last_updated": file.get("modifiedTime"),
            "name": file_name,
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
            permissions = file.get("permissions", [])
            if not permissions:
                try:
                    permissions = await self._get_permissions_on_shared_drive(
                        client=client, file_id=file_id
                    )
                except HTTPError as exception:
                    # Gracefully handle scenario when the service account does not
                    # have permission to fetch ACL for a file.
                    exception_log_msg = f"Unable to fetch permission list for the file {file_name}. Exception: {exception}."
                    if exception.res.status_code == 403:
                        self._logger.warning(exception_log_msg)
                    else:
                        self._logger.error(exception_log_msg)

            file_document[ACCESS_CONTROL] = self._process_permissions(permissions)

        return file_document

    async def prepare_files(self, client, files_page, paths, seen_ids):
        """Generate file document.

        Args:
            files_page (dict): Dictionary contains files list.

        Yields:
            dict: File with formatted metadata.
        """
        files = files_page.get("files", [])

        # Filter out files that have been already processed
        new_files = [file for file in files if file.get("id") not in seen_ids]

        prepared_files = await self._process_items_concurrently(
            new_files, lambda f: self.prepare_file(client=client, file=f, paths=paths)
        )

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

        # Keep track of seen file ids. If a file is shared directly
        # with google workspace users it can be discovered multiple times.
        # This is an optimization to process unique files only once.
        seen_ids = set()

        if self._domain_wide_delegation_sync_enabled():
            # sync personal drives first
            async for user in self.google_admin_directory_client.users():
                email = user.get(UserFields.EMAIL.value)
                self._logger.debug(f"Syncing personal drive content for: {email}")
                google_drive_client = self.google_drive_client(impersonate_email=email)
                async for files_page in google_drive_client.list_files_from_my_drive(
                    fetch_permissions=self._dls_enabled()
                ):
                    async for file in self.prepare_files(
                        client=google_drive_client,
                        files_page=files_page,
                        paths={},
                        seen_ids=seen_ids,
                    ):
                        yield file, partial(self.get_content, google_drive_client, file)

            email_for_shared_drives_sync = (
                self._google_google_workspace_email_for_shared_drives_sync()
            )

            shared_drives_client = self.google_drive_client(
                impersonate_email=email_for_shared_drives_sync
            )

            # Build a path lookup, parentId -> parent path
            resolved_paths = await self.resolve_paths(
                google_drive_client=shared_drives_client
            )

            # sync shared drives
            self._logger.debug(
                f"Syncing shared drives using admin account: {email_for_shared_drives_sync}"
            )
            async for files_page in shared_drives_client.list_files(
                fetch_permissions=self._dls_enabled()
            ):
                async for file in self.prepare_files(
                    client=shared_drives_client,
                    files_page=files_page,
                    paths=resolved_paths,
                    seen_ids=seen_ids,
                ):
                    yield file, partial(self.get_content, shared_drives_client, file)

        else:
            # Build a path lookup, parentId -> parent path
            resolved_paths = await self.resolve_paths()

            google_drive_client = self.google_drive_client()

            # sync anything shared with the service account
            async for files_page in google_drive_client.list_files(
                fetch_permissions=self._dls_enabled()
            ):
                async for file in self.prepare_files(
                    client=google_drive_client,
                    files_page=files_page,
                    paths=resolved_paths,
                    seen_ids=seen_ids,
                ):
                    yield file, partial(self.get_content, google_drive_client, file)
