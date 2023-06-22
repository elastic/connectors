#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
import json
from functools import cached_property, partial

import aiofiles
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds


from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import TIKA_SUPPORTED_FILETYPES, convert_to_b64, get_pem_format

DEFAULT_RETRY_COUNT = 3
DEFAULT_WAIT_MULTIPLIER = 3
DEFAULT_FILE_SIZE_LIMIT = 10485760

GOOGLE_DRIVE_READ_ONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
API_NAME = "drive"
API_VERSION = "v3"

CORPORA = 'allDrives'
FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'


# Export Google Workspace documents to TIKA compatible format, prefer 'text/plain' where possible to be
# mindful of the content extraction service resources
GOOGLE_MIME_TYPES_MAPPING = {
    'application/vnd.google-apps.document': 'text/plain',
    'application/vnd.google-apps.presentation': 'text/plain',
    'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
}

GOOGLE_DRIVE_EMULATOR_HOST = os.environ.get("GOOGLE_DRIVE_EMULATOR_HOST")
RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.
DEFAULT_PEM_FILE = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "tests",
    "sources",
    "fixtures",
    "google_drive",
    "service_account_dummy_cert.pem",
)

class GoogleDriveClient:
    """A google client to handle api calls made to Google Drive."""

    def __init__(self, retry_count, json_credentials):
        """Initialize the ServiceAccountCreds class using which api calls will be made.

        Args:
            retry_count (int): Maximum retries for the failed requests.
            json_credentials (dict): Service account credentials json.
        """
        self.retry_count = retry_count
        self.service_account_credentials = ServiceAccountCreds(
            scopes=[GOOGLE_DRIVE_READ_ONLY_SCOPE],
            **json_credentials,
        )

    async def api_call(
        self,
        resource,
        method,
        sub_method=None,
        full_response=False,
        **kwargs,
    ):
        """Make a GET call to Google Drive API with retry for the failed calls.

        Args:
            resource (aiogoogle.resource.Resource): Resource name for which api call will be made.
            method (aiogoogle.resource.Method): Method available for the resource.
            sub_method (aiogoogle.resource.Method, optional): Sub-method available for the method. Defaults to None.
            full_response (bool, optional): Specifies whether the response is paginated or not. Defaults to False.

        Raises:
            exception: A instance of an exception class.

        Yields:
            Dictionary: Response returned by the resource method.
        """
        retry_counter = 0
        while True:
            try:
                async with Aiogoogle(
                    service_account_creds=self.service_account_credentials
                ) as google_client:
                    drive_client = await google_client.discover(
                        api_name=API_NAME, api_version=API_VERSION
                    )
                    if RUNNING_FTEST and not sub_method and GOOGLE_DRIVE_EMULATOR_HOST:
                        logger.debug(
                            f"Using the google drive emulator at {GOOGLE_DRIVE_EMULATOR_HOST}"
                        )
                        # Redirecting calls to fake Google Drive server for e2e test.
                        drive_client.discovery_document["rootUrl"] = (
                            GOOGLE_DRIVE_EMULATOR_HOST + "/"
                        )
                    resource_object = getattr(drive_client, resource)
                    method_object = getattr(resource_object, method)
                    if full_response:
                        first_page_with_next_attached = (
                            await google_client.as_service_account(
                                method_object(**kwargs),
                                full_res=True
                            )
                        )

                        async for page_items in first_page_with_next_attached:
                            yield page_items
                            retry_counter = 0
                    else:
                        if sub_method:
                            method_object = getattr(method_object, sub_method)
                        yield await google_client.as_service_account(
                            method_object(**kwargs),
                        )
                    break
            except AttributeError as error:
                logger.error(
                    f"Error occurred while generating the resource/method object for an API call. Error: {error}"
                )
                raise
            except Exception as exception:
                retry_counter += 1
                if retry_counter > self.retry_count:
                    raise exception
                logger.warning(
                    f"Retry count: {retry_counter} out of {self.retry_count}. Response code: {exception.res.status_code} Exception: {exception}."
                )
                await asyncio.sleep(DEFAULT_WAIT_MULTIPLIER**retry_counter)


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


    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Google Drive.

        Returns:
            dictionary: Default configuration.
        """
        default_credentials = {
            "type": "service_account",
            "project_id": "project_id",
            "private_key_id": "abc",
            "private_key": open(
                os.path.abspath(DEFAULT_PEM_FILE)
            ).read(),  # TODO: change this and provide meaningful defaults
            "client_email": "123-abc@developer.gserviceaccount.com",
            "client_id": "123-abc.apps.googleusercontent.com",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "http://localhost:443/token",
        }
        return {
            "service_account_credentials": {
                "display": "textarea",
                "label": "Google Drive service account JSON",
                "order": 1,
                "type": "str",
                "value": json.dumps(default_credentials),
            },
            "retry_count": {
                "default_value": DEFAULT_RETRY_COUNT,
                "display": "numeric",
                "label": "Maximum retries for failed requests",
                "order": 2,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": DEFAULT_RETRY_COUNT,
            },
        }


    @cached_property
    def _google_drive_client(self):
        """Initialize and return the GoogleDriveClient

        Returns:
            GoogleDriveClient: An instance of the GoogleDriveClient.
        """
        json_credentials = json.loads(self.configuration["service_account_credentials"])

        if (
            json_credentials.get("private_key")
            and "\n" not in json_credentials["private_key"]
        ):
            json_credentials["private_key"] = get_pem_format(
                key=json_credentials["private_key"].strip(),
                max_split=2,
            )

        return GoogleDriveClient(
            json_credentials=json_credentials,
            retry_count=self.configuration["retry_count"],
        )


    async def ping(self):
        """Verify the connection with Google Drive"""
        if RUNNING_FTEST:
            return

        try:
            await anext(
                self._google_drive_client.api_call(
                    resource="about",
                    method="get",
                    fields="kind"
                )
            )
            logger.info("Successfully connected to the Google Drive.")
        except Exception:
            logger.exception("Error while connecting to the Google Drive.")
            raise


    async def validate_config(self):
        """Validates whether user inputs are valid or not for configuration field.

        Raises:
            Exception: The format of service account json is invalid.
        """
        self.configuration.check_valid()

        try:
            json.loads(self.configuration["service_account_credentials"])
        except ValueError as e:
            raise ConfigurableFieldValueError(
                "Google Cloud service account is not a valid JSON."
            ) from e


    async def get_drives(self):
        """Fetch all shared drive ids and names from the Googl Drive API.

        Yields:
            Dictionary: Contains the list of fetched buckets from Google Cloud Storage.
        """

        async for drive in self._google_drive_client.api_call(
            resource="drives",
            method="list",
            fields="nextPageToken,drives(id,name)",
            full_response=True,
            pageSize=100,
        ):
            yield drive


    async def retrieve_all_drives(self):
        drives = {}
        async for chunk in self.get_drives():
          drives_chunk = chunk.get('drives', [])
          for drive in drives_chunk:
              drives[drive['id']] = drive['name']

        return drives


    async def get_folders(self):
        async for folder in self._google_drive_client.api_call(
            resource="files",
            method="list",
            corpora='allDrives',
            fields="nextPageToken,files(id,name,parents)",
            q=f"mimeType='{FOLDER_MIME_TYPE}' and trashed=false",
            full_response=True,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=1000,
        ):
            yield folder


    async def retrieve_all_folders(self):
        folders = {}
        async for chunk in self.get_folders():
            folders_chunk = chunk.get('files', [])
            for folder in folders_chunk:
              folders[folder['id']] = {
                  'name': folder['name'],
                  'parents': folder.get('parents', None)
              }

        return folders


    async def resolve_paths(self):
        folders = await self.retrieve_all_folders()
        drives = await self.retrieve_all_drives()

        # for paths let's treat drives as top level folders
        for id, drive_name in drives.items():
            folders[id] = {'name': drive_name, 'parents': []}

        logger.info(f"Resolving folder paths for {len(folders)} folders")

        for folder in folders.values():
            path = [folder['name']]  # Start with the folder name

            parents = folder['parents']
            parent_id = parents[0] if parents else None

            # Traverse the parents until reaching the root or a missing parent
            while parent_id and parent_id in folders:
                parent_folder = folders[parent_id]
                path.insert(0, parent_folder['name'])  # Insert parent name at the beginning
                parents = parent_folder['parents']
                parent_id = parents[0] if parents else None

            folder['path'] = '/'.join(path)  # Join path elements with '/'


        return folders


    async def _download_content(self, blob, download_func):
        '''Downloads the file from Google Drive and returns the encoded file content.

        Args:
            blob (dictionary): Formatted blob document.
            download_func (partial func): Partial function that gets the file content from Google Drive API.

        Returns:
            attachment, blob_size (tuple): base64 encoded contnet of the file and size in bytes of the attachment
        '''

        temp_file_name = ""
        blob_name = blob["name"]

        logger.debug(f"Downloading {blob_name}")

        async with NamedTemporaryFile(
            mode="wb", delete=False
        ) as async_buffer:
            await anext(download_func(pipe_to=async_buffer,))
            temp_file_name = async_buffer.name

        await asyncio.to_thread(
            convert_to_b64,
            source=temp_file_name,
        )

        blob_size = os.stat(temp_file_name).st_size

        async with aiofiles.open(file=temp_file_name, mode="r") as target_file:
            attachment = (await target_file.read()).strip()

        logger.debug(f"Downloaded {blob_name} for {blob_size} bytes ")

        await remove(str(temp_file_name))

        return attachment, blob_size


    async def get_google_workspace_content(self, blob, timestamp=None):
        """Exports Google Workspace documents to an allowed file type and extracts its text content.

        Shared Google Workspace documents are different than regular files. When shared from
        a different account they don't count against the user storage quota and therefore have size 0.
        They need to be exported to a supported file type before the content extraction phase.

        Args:
            blob (dictionary): Formatted blob document.
            timestamp (timestamp, optional): Timestamp of blob last modified. Defaults to None.

        Returns:
            dictionary: Content document with id, timestamp & text
        """

        blob_name, blob_id, blob_mime_type = blob["name"], blob["id"], blob['mime_type']

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
            )
        )

        if blob_size > DEFAULT_FILE_SIZE_LIMIT:
            logger.warning(
                f"File size {blob_size} of file {blob_name} is larger than {DEFAULT_FILE_SIZE_LIMIT} bytes. Discarding the file content"
            )
            return

        document["_attachment"] = attachment
        return document


    async def get_generic_file_content(self, blob, timestamp=None):
        """Extracts the content from allowed file types supported by Apache Tika.

        Args:
            blob (dictionary): Formatted blob document.
            timestamp (timestamp, optional): Timestamp of blob last modified. Defaults to None.

        Returns:
            dictionary: Content document with id, timestamp & text
        """

        blob_size = int(blob["size"])

        if blob_size == 0:
            return

        blob_name, blob_id, blob_extension = \
            blob["name"], blob["id"], f".{blob['file_extension']}"

        if blob_extension not in TIKA_SUPPORTED_FILETYPES:
            logger.debug(f"{blob_name} can't be extracted")
            return

        if blob_size > DEFAULT_FILE_SIZE_LIMIT:
            logger.warning(
                f"File size {blob_size} of file {blob_name} is larger than {DEFAULT_FILE_SIZE_LIMIT} bytes. Discarding the file content"
            )
            return

        document = {
            "_id": blob_id,
            "_timestamp": blob["_timestamp"],
        }

        attachment, blob_size = await self._download_content(
            blob=blob,
            download_func=partial(
                self._google_drive_client.api_call,
                resource="files",
                method="get",
                fileId=blob_id,
                supportsAllDrives=True,
                alt="media",
            )
        )

        document["_attachment"] = attachment
        return document


    async def get_content(self, blob, timestamp=None, doit=None):
        """Extracts the content from a blob file.

        Args:
            blob (dictionary): Formatted blob document.
            timestamp (timestamp, optional): Timestamp of blob last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to None.

        Returns:
            dictionary: Content document with id, timestamp & text
        """

        if not doit:
            return

        blob_mime_type = blob['mime_type']

        if blob_mime_type in GOOGLE_MIME_TYPES_MAPPING:
            # Get content from native google workspace files (docs, slides, sheets)
            return await self.get_google_workspace_content(blob, timestamp=timestamp)
        else:
            # Get content from all other file types
            return await self.get_generic_file_content(blob, timestamp=timestamp)



    async def fetch_files(self):
      """Get files from Google Drive. Files can have any type.

      Yields:
          dictionary: Documents from Google Drive.
      """
      async for file in self._google_drive_client.api_call(
          resource="files",
          method="list",
          full_response=True,
          corpora=CORPORA,
          q="trashed=false",
          orderBy='modifiedTime desc',
          fields='files,nextPageToken',
          includeItemsFromAllDrives=True,
          supportsAllDrives=True,
          pageSize=100
      ):
          yield file


    def prepare_blob_document(self, blob, paths):
        """Apply key mappings to the blob document.

        Args:
            blob (dictionary): Blob's metadata returned from the Drive.

        Returns:
            dictionary: Blobs metadata mapped with the keys of `BLOB_ADAPTER`.
        """

        blob_document = {
            "_id": blob.get("id"),
            "created_at": blob.get("createdTime"),
            "last_updated": blob.get("modifiedTime"),
            "name": blob.get("name"),
            "size": blob.get("size"),
            "_timestamp": blob.get("modifiedTime"),
            "mime_type": blob.get("mimeType"),
            "file_extension": blob.get("fileExtension"),
            "url": blob.get("webViewLink"),
        }

        # record "file" or "folder" type
        blob_document["type"] = "folder" if blob.get("mimeType") == FOLDER_MIME_TYPE else "file"

        # populate owner-related fields if owner is present in the response from the Drive API
        owners = blob.get('owners', None)
        if owners:
                first_owner = blob['owners'][0]
                blob_document['author'] = ','.join([owner['displayName'] for owner in owners])
                blob_document['created_by'] = first_owner['displayName']
                blob_document['created_by_email'] = first_owner['emailAddress']

        # handle last modifying user metadata
        last_modifying_user = blob.get('lastModifyingUser', None)
        if last_modifying_user:
            blob_document['updated_by'] = last_modifying_user.get('displayName', None)
            blob_document['updated_by_email'] = last_modifying_user.get('emailAddress', None)
            blob_document['updated_by_photo_url'] = last_modifying_user.get('photoLink', None)

        # handle folders and shortcuts
        if blob_document['size'] is None:
            blob_document['size'] = 0

        # determine the path on google drive, note that google workspace files won't have a path
        blob_parents = blob.get('parents', None)
        if blob_parents and blob_parents[0] in paths:
            blob_document['path'] = f"{paths[blob_parents[0]]['path']}/{blob['name']}"

        return blob_document

    def get_blob_document(self, blobs, paths):
        """Generate blob document.

        Args:
            blobs (dictionary): Dictionary contains blobs list.

        Yields:
            dictionary: Blobs metadata mapped with the keys of `BLOB_ADAPTER`.
        """
        for blob in blobs.get("files", []):
            yield self.prepare_blob_document(blob=blob, paths=paths)


    async def get_docs(self, filtering=None):

      # Build a path lookup, parentId -> parent path
      resolved_paths = await self.resolve_paths()

      async for files in self.fetch_files():
          for blob_document in self.get_blob_document(blobs=files, paths=resolved_paths):
              yield blob_document, partial(self.get_content, blob_document)
