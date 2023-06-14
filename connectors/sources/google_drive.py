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
DEFAULT_WAIT_MULTIPLIER = 2
DEFAULT_FILE_SIZE_LIMIT = 10485760

GOOGLE_DRIVE_READ_ONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
API_NAME = "drive"
API_VERSION = "v3"
BLOB_ADAPTER = {
    "_id": "id",
    "created_at": "createdTime",
    "created_by": "",
    "last_updated": "modifiedTime",
    "name": "name",
    "size": "size",
    "_timestamp": "modifiedTime",
    "mime_type": "mimeType",
    "file_extension": "fileExtension",
    "url": "webViewLink",
}
DEFAULT_RETRY_COUNT = 3
DEFAULT_WAIT_MULTIPLIER = 2
DEFAULT_FILE_SIZE_LIMIT = 10485760
STORAGE_EMULATOR_HOST = os.environ.get("STORAGE_EMULATOR_HOST")
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
    "google_cloud_storage",
    "service_account_dummy_cert.pem",
)


DOMAIN_CORPORA = 'domain'
USER_CORPORA = 'user'




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
        """Make a GET call for Google Cloud Storage with retry for the failed API calls.

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
                    storage_client = await google_client.discover(
                        api_name=API_NAME, api_version=API_VERSION
                    )
                    if RUNNING_FTEST and not sub_method and STORAGE_EMULATOR_HOST:
                        logger.debug(
                            f"Using the storage emulator at {STORAGE_EMULATOR_HOST}"
                        )
                        # Redirecting calls to fake Google Cloud Storage server for e2e test.
                        storage_client.discovery_document["rootUrl"] = (
                            STORAGE_EMULATOR_HOST + "/"
                        )
                    resource_object = getattr(storage_client, resource)
                    method_object = getattr(resource_object, method)
                    if full_response:
                        first_page_with_next_attached = (
                            await google_client.as_service_account(
                                method_object(**kwargs),
                                full_res=True,
                            )
                        )
                        async for page_items in first_page_with_next_attached:
                            yield page_items
                            retry_counter = 0
                    else:
                        if sub_method:
                            method_object = getattr(method_object, sub_method)
                        yield await google_client.as_service_account(
                            method_object(**kwargs)
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
                    f"Retry count: {retry_counter} out of {self.retry_count}. Exception: {exception}"
                )
                await asyncio.sleep(DEFAULT_WAIT_MULTIPLIER**retry_counter)


class GoogleDriveDataSource(BaseDataSource):
    """Google Cloud Storage"""

    name = "Google Cloud Storage"
    service_type = "google_cloud_storage"

    def __init__(self, configuration):
        """Set up the connection to the Google Cloud Storage Client.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)


    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Google Cloud Storage.

        Returns:
            dictionary: Default configuration.
        """
        default_credentials = {
            "type": "service_account",
            "project_id": "dummy_project_id",
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
                "label": "Google Cloud service account JSON",
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
        """Initialize and return the GoogleCloudStorageClient

        Returns:
            GoogleCloudStorageClient: An instance of the GoogleCloudStorageClient.
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
        """Verify the connection with Google Cloud Storage"""
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
            logger.info("Successfully connected to the Google Cloud Storage.")
        except Exception:
            logger.exception("Error while connecting to the Google Cloud Storage.")
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


    async def get_content(self, blob, timestamp=None, doit=None):
        """Extracts the content for allowed file types.

        Args:
            blob (dictionary): Formatted blob document.
            timestamp (timestamp, optional): Timestamp of blob last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to None.

        Returns:
            dictionary: Content document with id, timestamp & text
        """

        blob_size = int(blob["size"])
        if not (doit and blob_size > 0):
            return

        blob_name, blob_id, blob_extension = blob["name"], blob["id"], f".{blob['file_extension']}"

        if blob_extension not in TIKA_SUPPORTED_FILETYPES:
            logger.debug(f"{blob_name} can't be extracted")
            return

        if blob_size > DEFAULT_FILE_SIZE_LIMIT:
            logger.warning(
                f"File size {blob_size} of file {blob_name} is larger than {DEFAULT_FILE_SIZE_LIMIT} bytes. Discarding the file content"
            )
            return
        logger.debug(f"Downloading {blob_name}")
        document = {
            "_id": blob_id,
            "_timestamp": blob["_timestamp"],
        }
        source_file_name = ""
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            await anext(
                self._google_drive_client.api_call(
                    resource="files",
                    method="get",
                    fileId=blob_id,
                    alt="media",
                    pipe_to=async_buffer,
                )
            )
            source_file_name = async_buffer.name

        logger.debug(f"Calling convert_to_b64 for file : {blob_name}")
        await asyncio.to_thread(
            convert_to_b64,
            source=source_file_name,
        )
        async with aiofiles.open(file=source_file_name, mode="r") as target_file:
            # base64 on macOS will add a EOL, so we strip() here
            document["_attachment"] = (await target_file.read()).strip()
        await remove(str(source_file_name))
        logger.debug(f"Downloaded {blob_name} for {blob_size} bytes ")
        return document



    async def fetch_files(self):
      """Get files from Google Drive. File can have any type.

      Yields:
          dictionary: Documents from Google Cloud Storage.
      """
      async for file in self._google_drive_client.api_call(
          resource="files",
          method="list",
          full_response=True,
          corpora=USER_CORPORA,
          orderBy='modifiedTime desc',
          fields='files,nextPageToken',
          includeItemsFromAllDrives=True,
          supportsAllDrives=True,
          pageSize=100
      ):
          yield file


    def prepare_blob_document(self, blob):
        """Apply key mappings to the blob document.

        Args:
            blob (dictionary): Blob's metadata returned from the Google Cloud Storage.

        Returns:
            dictionary: Blobs metadata mapped with the keys of `BLOB_ADAPTER`.
        """

        blob_document = {}
        for elasticsearch_field, google_cloud_storage_field in BLOB_ADAPTER.items():
            blob_document[elasticsearch_field] = blob.get(google_cloud_storage_field)

        # handle folders and shortcuts
        if blob_document['size'] is None:
          blob_document['size'] = 0

        return blob_document

    def get_blob_document(self, blobs):
        """Generate blob document.

        Args:
            blobs (dictionary): Dictionary contains blobs list.

        Yields:
            dictionary: Blobs metadata mapped with the keys of `BLOB_ADAPTER`.
        """
        for blob in blobs.get("files", []):
            yield self.prepare_blob_document(blob=blob)


    async def get_docs(self, filtering=None):
      async for files in self.fetch_files():
          for blob_document in self.get_blob_document(blobs=files):
              yield blob_document, partial(self.get_content, blob_document)
