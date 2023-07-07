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

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

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


class GoogleDriveClient:
    """A google client to handle api calls made to Google Drive."""

    def __init__(self, json_credentials):
        """Initialize the ServiceAccountCreds class using which api calls will be made.

        Args:
            retry_count (int): Maximum retries for the failed requests.
            json_credentials (dict): Service account credentials json.
        """
        self.service_account_credentials = ServiceAccountCreds(
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
            **json_credentials,
        )
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
                    api_name="drive", api_version="v3"
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


class GoogleDriveDataSource(BaseDataSource):
    """Google Drive"""

    name = "Google Drive"
    service_type = "google_drive"

    def __init__(self, configuration):
        """Set up the connection to the Google Drive Client.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)

    def _set_internal_logger(self):
        self._google_drive_client.set_logger(self._logger)

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
            }
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

        return GoogleDriveClient(
            json_credentials=json_credentials,
        )

    async def validate_config(self):
        """Validates whether user inputs are valid or not for configuration field.

        Raises:
            Exception: The format of service account json is invalid.
        """
        self.configuration.check_valid()

        self._validate_service_account_json()

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
        """""Verify the connection with Google Drive""" ""
        try:
            await self._google_drive_client.api_call(
                resource="about", method="get", fields="kind"
            )
            self._logger.info("Successfully connected to the Google Drive.")
        except Exception:
            self._logger.exception("Error while connecting to the Google Drive.")
            raise

    async def get_drives(self):
        """Fetch all shared drive (id, name) from Google Drive

        Yields:
            dict: Shared drive metadata.
        """

        async for drive in self._google_drive_client.api_call_paged(
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
        async for folder in self._google_drive_client.api_call_paged(
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

    async def resolve_paths(self):
        """Builds a lookup between a folder id and its absolute path in Google Drive structure

        Returns:
            dict: mapping between folder id and its (name, parents, path)
        """
        folders = await self.retrieve_all_folders()
        drives = await self.retrieve_all_drives()

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

    async def _download_content(self, blob, download_func):
        """Downloads the file from Google Drive and returns the encoded file content.

        Args:
            blob (dict): Formatted blob document.
            download_func (partial func): Partial function that gets the file content from Google Drive API.

        Returns:
            attachment, blob_size (tuple): base64 encoded contnet of the file and size in bytes of the attachment
        """

        temp_file_name = ""
        blob_name = blob["name"]
        attachment, blob_size = None, 0

        self._logger.debug(f"Downloading {blob_name}")

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

            blob_size = os.stat(temp_file_name).st_size

            async with aiofiles.open(file=temp_file_name, mode="r") as target_file:
                attachment = (await target_file.read()).strip()

            self._logger.debug(
                f"Downloaded {blob_name} with the size of {blob_size} bytes "
            )
        except Exception as e:
            self._logger.error(
                f"Exception encountered when processing file: {blob_name}. Exception: {e}"
            )
        finally:
            if temp_file_name:
                await remove(str(temp_file_name))

        return attachment, blob_size

    async def get_google_workspace_content(self, blob, timestamp=None):
        """Exports Google Workspace documents to an allowed file type and extracts its text content.

        Shared Google Workspace documents are different than regular files. When shared from
        a different account they don't count against the user storage quota and therefore have size 0.
        They need to be exported to a supported file type before the content extraction phase.

        Args:
            blob (dict): Formatted blob document.
            timestamp (timestamp, optional): Timestamp of blob last modified. Defaults to None.

        Returns:
            dict: Content document with id, timestamp & text
        """

        blob_name, blob_id, blob_mime_type = blob["name"], blob["id"], blob["mime_type"]

        document = {
            "_id": blob_id,
            "_timestamp": blob["_timestamp"],
        }

        attachment, blob_size = await self._download_content(
            blob=blob,
            download_func=partial(
                self._google_drive_client.api_call,
                resource="files",
                method="export",
                fileId=blob_id,
                mimeType=GOOGLE_MIME_TYPES_MAPPING[blob_mime_type],
            ),
        )

        # We need to do sanity size after downloading the file because:
        # 1. We use files/export endpoint which converts large media-rich google slides/docs
        #    into text/plain format. We usually we end up with tiny .txt files.
        # 2. Google will ofter report the Google Workspace shared documents to have size 0
        #    as they don't count against user's storage quota.
        if blob_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {blob_size} of file {blob_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding the file content"
            )
            return

        document["_attachment"] = attachment
        return document

    async def get_generic_file_content(self, blob, timestamp=None):
        """Extracts the content from allowed file types supported by Apache Tika.

        Args:
            blob (dict): Formatted blob document.
            timestamp (timestamp, optional): Timestamp of blob last modified. Defaults to None.

        Returns:
            dict: Content document with id, timestamp & text
        """

        blob_size = int(blob["size"])

        if blob_size == 0:
            return

        blob_name, blob_id, blob_extension = (
            blob["name"],
            blob["id"],
            f".{blob['file_extension']}",
        )

        if blob_extension not in TIKA_SUPPORTED_FILETYPES:
            self._logger.debug(f"{blob_name} can't be extracted")
            return

        if blob_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {blob_size} of file {blob_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding the file content"
            )
            return

        document = {
            "_id": blob_id,
            "_timestamp": blob["_timestamp"],
        }

        attachment, _ = await self._download_content(
            blob=blob,
            download_func=partial(
                self._google_drive_client.api_call,
                resource="files",
                method="get",
                fileId=blob_id,
                supportsAllDrives=True,
                alt="media",
            ),
        )

        document["_attachment"] = attachment
        return document

    async def get_content(self, blob, timestamp=None, doit=None):
        """Extracts the content from a blob file.

        Args:
            blob (dict): Formatted blob document.
            timestamp (timestamp, optional): Timestamp of blob last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to None.

        Returns:
            dict: Content document with id, timestamp & text
        """

        if not doit:
            return

        blob_mime_type = blob["mime_type"]

        if blob_mime_type in GOOGLE_MIME_TYPES_MAPPING:
            # Get content from native google workspace files (docs, slides, sheets)
            return await self.get_google_workspace_content(blob, timestamp=timestamp)
        else:
            # Get content from all other file types
            return await self.get_generic_file_content(blob, timestamp=timestamp)

    async def fetch_files(self):
        """Get files from Google Drive. Files can have any type.

        Yields:
            dict: Documents from Google Drive.
        """
        async for file in self._google_drive_client.api_call_paged(
            resource="files",
            method="list",
            corpora="allDrives",
            q="trashed=false",
            orderBy="modifiedTime desc",
            fields="files,nextPageToken",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=100,
        ):
            yield file

    def prepare_blob_document(self, blob, paths):
        """Apply key mappings to the blob document.

        Args:
            blob (dict): Blob's metadata returned from the Drive.

        Returns:
            dict: Blobs metadata mapped with the keys of `BLOB_ADAPTER`.
        """

        blob_document = {
            "_id": blob.get("id"),
            "created_at": blob.get("createdTime"),
            "last_updated": blob.get("modifiedTime"),
            "name": blob.get("name"),
            "size": blob.get("size") or 0,  # handle folders and shortcuts
            "_timestamp": blob.get("modifiedTime"),
            "mime_type": blob.get("mimeType"),
            "file_extension": blob.get("fileExtension"),
            "url": blob.get("webViewLink"),
        }

        # record "file" or "folder" type
        blob_document["type"] = (
            "folder" if blob.get("mimeType") == FOLDER_MIME_TYPE else "file"
        )

        # populate owner-related fields if owner is present in the response from the Drive API
        owners = blob.get("owners", None)
        if owners:
            first_owner = blob["owners"][0]
            blob_document["author"] = ",".join(
                [owner["displayName"] for owner in owners]
            )
            blob_document["created_by"] = first_owner["displayName"]
            blob_document["created_by_email"] = first_owner["emailAddress"]

        # handle last modifying user metadata
        last_modifying_user = blob.get("lastModifyingUser", None)
        if last_modifying_user:
            blob_document["updated_by"] = last_modifying_user.get("displayName", None)
            blob_document["updated_by_email"] = last_modifying_user.get(
                "emailAddress", None
            )
            blob_document["updated_by_photo_url"] = last_modifying_user.get(
                "photoLink", None
            )

        # determine the path on google drive, note that google workspace files won't have a path
        blob_parents = blob.get("parents", None)
        if blob_parents and blob_parents[0] in paths:
            blob_document["path"] = f"{paths[blob_parents[0]]['path']}/{blob['name']}"

        return blob_document

    def get_blob_document(self, blobs, paths):
        """Generate blob document.

        Args:
            blobs (dict): Dictionary contains blobs list.

        Yields:
            dict: Blobs metadata mapped with the keys of `BLOB_ADAPTER`.
        """
        for blob in blobs.get("files", []):
            yield self.prepare_blob_document(blob=blob, paths=paths)

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

        async for files in self.fetch_files():
            for blob_document in self.get_blob_document(
                blobs=files, paths=resolved_paths
            ):
                yield blob_document, partial(self.get_content, blob_document)
