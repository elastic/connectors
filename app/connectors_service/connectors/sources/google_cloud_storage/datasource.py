#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import urllib.parse
from functools import cached_property, partial

from aiogoogle import HTTPError
from connectors_sdk.source import BaseDataSource

from connectors.sources.google_cloud_storage.client import GoogleCloudStorageClient
from connectors.sources.shared.google.google import (
    load_service_account_json,
    validate_service_account_json,
)
from connectors.utils import get_pem_format

CLOUD_STORAGE_BASE_URL = "https://console.cloud.google.com/storage/browser/_details/"

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

RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.


class GoogleCloudStorageDataSource(BaseDataSource):
    """Google Cloud Storage"""

    name = "Google Cloud Storage"
    service_type = "google_cloud_storage"
    incremental_sync_enabled = True

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

        return {
            "buckets": {
                "display": "textarea",
                "label": "Google Cloud Storage buckets",
                "order": 1,
                "type": "list",
            },
            "service_account_credentials": {
                "display": "textarea",
                "label": "Google Cloud service account JSON",
                "sensitive": True,
                "order": 2,
                "type": "str",
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 3,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }

    async def validate_config(self):
        """Validates whether user inputs are valid or not for configuration field.

        Raises:
            Exception: The format of service account json is invalid.
        """
        await super().validate_config()

        validate_service_account_json(
            self.configuration["service_account_credentials"], "Google Cloud Storage"
        )

    @cached_property
    def _google_storage_client(self):
        """Initialize and return the GoogleCloudStorageClient

        Returns:
            GoogleCloudStorageClient: An instance of the GoogleCloudStorageClient.
        """
        json_credentials = load_service_account_json(
            self.configuration["service_account_credentials"], "Google Cloud Storage"
        )

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

        return GoogleCloudStorageClient(json_credentials=required_credentials)

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
        except Exception:
            self._logger.exception(
                "Error while connecting to the Google Cloud Storage."
            )
            raise

    async def fetch_buckets(self, buckets):
        """Fetch the buckets from the Google Cloud Storage.
        Args:
            buckets (List): List of buckets.

        Yields:
            Dictionary: Contains the list of fetched buckets from Google Cloud Storage.
        """
        if "*" in buckets:
            self._logger.info(
                "Fetching all buckets as the configuration field 'buckets' is set to '*'"
            )
            async for bucket in self._google_storage_client.api_call(
                resource="buckets",
                method="list",
                full_response=True,
                project=self._google_storage_client.user_project_id,
                userProject=self._google_storage_client.user_project_id,
            ):
                yield bucket
        else:
            self._logger.info(f"Fetching user configured buckets: {buckets}")
            for bucket in buckets:
                yield {"items": [{"id": bucket, "name": bucket}]}

    async def fetch_blobs(self, buckets):
        """Fetches blobs stored in the bucket from Google Cloud Storage.

        Args:
            buckets (Dictionary): Contains the list of fetched buckets from Google Cloud Storage.

        Yields:
            Dictionary: Contains the list of fetched blobs from Google Cloud Storage.
        """
        for bucket in buckets.get("items", []):
            blob_count = 0
            self._logger.info(f"Fetching blobs for '{bucket['id']}' bucket")
            try:
                async for blob in self._google_storage_client.api_call(
                    resource="objects",
                    method="list",
                    full_response=True,
                    bucket=bucket["id"],
                    userProject=self._google_storage_client.user_project_id,
                ):
                    blob_count += 1
                    yield blob
                self._logger.info(
                    f"Total {blob_count} blobs fetched for bucket '{bucket.get('name')}'"
                )
            except HTTPError as exception:
                exception_log_msg = f"Permission denied for {bucket['name']} while fetching blobs. Exception: {exception}."
                if exception.res.status_code == 403:
                    self._logger.warning(exception_log_msg)
                else:
                    self._logger.error(
                        f"Something went wrong while fetching blobs from {bucket['name']}. Error: {exception}"
                    )

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
        blob_document["url"] = (
            f"{CLOUD_STORAGE_BASE_URL}{blob_document['bucket_name']}/{blob_name};tab=live_object?project={self._google_storage_client.user_project_id}"
        )
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
        if not doit:
            self._logger.debug(f"Skipping attachment downloading for {blob['name']}")
            return

        file_size = int(blob["size"])
        if file_size <= 0:
            self._logger.warning(
                f"Skipping file '{blob['name']}' as file size is {file_size}"
            )
            return

        filename = blob["name"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        self._logger.debug(f"Downloading content for file: {filename}")
        document = {
            "_id": blob["id"],
            "_timestamp": blob["_timestamp"],
        }

        # gcs has a unique download method so we can't utilize
        # the generic download_and_extract_file func
        async with self.create_temp_file(file_extension) as async_buffer:
            await anext(
                self._google_storage_client.api_call(
                    resource="objects",
                    method="get",
                    bucket=blob["bucket_name"],
                    object=filename,
                    alt="media",
                    userProject=self._google_storage_client.user_project_id,
                    pipe_to=async_buffer,
                    path_params_safe_chars={"object": "'"},
                )
            )
            await async_buffer.close()

            document = await self.handle_file_content_extraction(
                document, filename, async_buffer.name
            )

        return document

    async def get_docs(self, filtering=None):
        """Get buckets & blob documents from Google Cloud Storage.

        Yields:
            dictionary: Documents from Google Cloud Storage.
        """
        self._logger.info("Successfully connected to the Google Cloud Storage.")
        async for bucket in self.fetch_buckets(self.configuration["buckets"]):
            async for blobs in self.fetch_blobs(
                buckets=bucket,
            ):
                for blob_document in self.get_blob_document(blobs=blobs):
                    yield blob_document, partial(self.get_content, blob_document)
