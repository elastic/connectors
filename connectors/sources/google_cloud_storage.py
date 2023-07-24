#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Google Cloud Storage source module responsible to fetch documents from Google Cloud Storage.
"""
import asyncio
import json
import os
import urllib.parse
from functools import cached_property, partial

import aiofiles
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import TIKA_SUPPORTED_FILETYPES, convert_to_b64, get_pem_format

CLOUD_STORAGE_READ_ONLY_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"
CLOUD_STORAGE_BASE_URL = "https://console.cloud.google.com/storage/browser/_details/"
API_NAME = "storage"
API_VERSION = "v1"
BLOB_ADAPTER = {
    "_id": "id",
    "component_count": "componentCount",
    "content_encoding": "contentEncoding",
    "content_language": "contentLanguage",
    "created_at": "timeCreated",
    "last_updated": "updated",
    "metadata": "metadata",
    "name": "name",
    "size": "size",
    "storage_class": "storageClass",
    "_timestamp": "updated",
    "type": "contentType",
    "url": "selfLink",
    "version": "generation",
    "bucket_name": "bucket",
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
REQUIRED_CREDENTIAL_KEYS = [
    "type",
    "project_id",
    "private_key_id",
    "private_key",
    "client_email",
    "client_id",
    "auth_uri",
    "token_uri",
]


class GoogleCloudStorageClient:
    """A google client to handle api calls made to Google Cloud Storage."""

    def __init__(self, retry_count, json_credentials):
        """Initialize the ServiceAccountCreds class using which api calls will be made.

        Args:
            retry_count (int): Maximum retries for the failed requests.
            json_credentials (dict): Service account credentials json.
        """
        self.retry_count = retry_count
        self.service_account_credentials = ServiceAccountCreds(
            scopes=[CLOUD_STORAGE_READ_ONLY_SCOPE],
            **json_credentials,
        )
        self.user_project_id = self.service_account_credentials.project_id
        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

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
                        self._logger.debug(
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
                self._logger.error(
                    f"Error occurred while generating the resource/method object for an API call. Error: {error}"
                )
                raise
            except Exception as exception:
                retry_counter += 1
                if retry_counter > self.retry_count:
                    raise exception
                self._logger.warning(
                    f"Retry count: {retry_counter} out of {self.retry_count}. Exception: {exception}"
                )
                await asyncio.sleep(DEFAULT_WAIT_MULTIPLIER**retry_counter)


class GoogleCloudStorageDataSource(BaseDataSource):
    """Google Cloud Storage"""

    name = "Google Cloud Storage"
    service_type = "google_cloud_storage"

    def __init__(self, configuration):
        """Set up the connection to the Google Cloud Storage Client.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)

    def _set_internal_logger(self):
        self._google_storage_client.set_logger(self._logger)

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
            "private_key": None,
            "client_email": "123-abc@developer.gserviceaccount.com",
            "client_id": "123-abc.apps.googleusercontent.com",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "http://localhost:4444/token",
        }
        with open(os.path.abspath(DEFAULT_PEM_FILE)) as f:
            default_credentials["private_key"] = f.read()

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

    @cached_property
    def _google_storage_client(self):
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
                postfix="-----END PRIVATE KEY-----",
            )

        required_credentials = {
            key: value
            for key, value in json_credentials.items()
            if key in REQUIRED_CREDENTIAL_KEYS
        }

        return GoogleCloudStorageClient(
            json_credentials=required_credentials,
            retry_count=self.configuration["retry_count"],
        )

    async def ping(self):
        """Verify the connection with Google Cloud Storage"""
        if RUNNING_FTEST:
            return

        try:
            await anext(
                self._google_storage_client.api_call(
                    resource="projects",
                    method="serviceAccount",
                    sub_method="get",
                    projectId=self._google_storage_client.user_project_id,
                )
            )
            self._logger.info("Successfully connected to the Google Cloud Storage.")
        except Exception:
            self._logger.exception(
                "Error while connecting to the Google Cloud Storage."
            )
            raise

    async def fetch_buckets(self):
        """Fetch the buckets from the Google Cloud Storage.

        Yields:
            Dictionary: Contains the list of fetched buckets from Google Cloud Storage.
        """
        async for bucket in self._google_storage_client.api_call(
            resource="buckets",
            method="list",
            full_response=True,
            project=self._google_storage_client.user_project_id,
            userProject=self._google_storage_client.user_project_id,
        ):
            yield bucket

    async def fetch_blobs(self, buckets):
        """Fetches blobs stored in the bucket from Google Cloud Storage.

        Args:
            buckets (Dictionary): Contains the list of fetched buckets from Google Cloud Storage.

        Yields:
            Dictionary: Contains the list of fetched blobs from Google Cloud Storage.
        """
        for bucket in buckets.get("items", []):
            async for blob in self._google_storage_client.api_call(
                resource="objects",
                method="list",
                full_response=True,
                bucket=bucket["id"],
                userProject=self._google_storage_client.user_project_id,
            ):
                yield blob

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
        blob_name = urllib.parse.quote(blob_document["name"], safe="'")
        blob_document[
            "url"
        ] = f"{CLOUD_STORAGE_BASE_URL}{blob_document['bucket_name']}/{blob_name};tab=live_object?project={self._google_storage_client.user_project_id}"
        return blob_document

    def get_blob_document(self, blobs):
        """Generate blob document.

        Args:
            blobs (dictionary): Dictionary contains blobs list.

        Yields:
            dictionary: Blobs metadata mapped with the keys of `BLOB_ADAPTER`.
        """
        for blob in blobs.get("items", []):
            yield self.prepare_blob_document(blob=blob)

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
        if not (doit and blob_size):
            return

        blob_name = blob["name"]
        if (os.path.splitext(blob_name)[-1]).lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.debug(f"{blob_name} can't be extracted")
            return

        if blob_size > DEFAULT_FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {blob_size} of file {blob_name} is larger than {DEFAULT_FILE_SIZE_LIMIT} bytes. Discarding the file content"
            )
            return
        self._logger.debug(f"Downloading {blob_name}")
        document = {
            "_id": blob["id"],
            "_timestamp": blob["_timestamp"],
        }
        source_file_name = ""
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            await anext(
                self._google_storage_client.api_call(
                    resource="objects",
                    method="get",
                    bucket=blob["bucket_name"],
                    object=blob_name,
                    alt="media",
                    userProject=self._google_storage_client.user_project_id,
                    pipe_to=async_buffer,
                )
            )
            source_file_name = async_buffer.name

        self._logger.debug(f"Calling convert_to_b64 for file : {blob_name}")
        await asyncio.to_thread(
            convert_to_b64,
            source=source_file_name,
        )
        async with aiofiles.open(file=source_file_name, mode="r") as target_file:
            # base64 on macOS will add a EOL, so we strip() here
            document["_attachment"] = (await target_file.read()).strip()
        await remove(str(source_file_name))
        self._logger.debug(f"Downloaded {blob_name} for {blob_size} bytes ")
        return document

    async def get_docs(self, filtering=None):
        """Get buckets & blob documents from Google Cloud Storage.

        Yields:
            dictionary: Documents from Google Cloud Storage.
        """
        async for buckets in self.fetch_buckets():
            if not buckets.get("items"):
                continue
            async for blobs in self.fetch_blobs(
                buckets=buckets,
            ):
                for blob_document in self.get_blob_document(blobs=blobs):
                    yield blob_document, partial(self.get_content, blob_document)
