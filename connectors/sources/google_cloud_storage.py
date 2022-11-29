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
from functools import partial

from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds

from connectors.logger import logger
from connectors.source import BaseDataSource

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
SUPPORTED_FILETYPE = [".py", ".rst", ".rb", ".sh", ".md", ".txt"]
DEFAULT_RETRY_COUNT = 3
DEFAULT_WAIT_MULTIPLIER = 2
DEFAULT_CONTENT_EXTRACTION = True
DEFAULT_FILE_SIZE_LIMIT = 10485760


class GoogleCloudStorageDataSource(BaseDataSource):
    """Class to fetch documents from Google Cloud Storage."""

    def __init__(self, connector):
        """Setup connection to the Google Cloud Storage Client.

        Args:
            connector (BYOConnector): Object of the BYOConnector class.
        """
        super().__init__(connector=connector)
        self.credentials_path = self.configuration["credentials_path"]
        self.service_account_credentials = ServiceAccountCreds(
            scopes=[CLOUD_STORAGE_READ_ONLY_SCOPE],
            **json.load(fp=open(file=self.credentials_path)),
        )
        self.user_project_id = self.service_account_credentials.project_id
        self.retry_count = int(
            self.configuration.get("retry_count", DEFAULT_RETRY_COUNT)
        )
        self.enable_content_extraction = self.configuration.get(
            "enable_content_extraction", DEFAULT_CONTENT_EXTRACTION
        )

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Google Cloud Storage.

        Returns:
            dictionary: Default configuration.
        """
        return {
            "credentials_path": {
                "value": "gcs_dummy_credentials.json",
                "label": "Google cloud service account file path (.json file)",
                "type": "str",
            },
            "connector_name": {
                "value": "Google Cloud Storage Connector",
                "label": "Friendly name for the connector",
                "type": "str",
            },
            "retry_count": {
                "value": DEFAULT_RETRY_COUNT,
                "label": "Retry count for failed requests",
                "type": "int",
            },
            "enable_content_extraction": {
                "value": DEFAULT_CONTENT_EXTRACTION,
                "label": "Flag to check if content extraction is enabled or not",
                "type": "bool",
            },
        }

    async def _connect(
        self,
        resource_name,
        method,
        sub_method=None,
        full_response=False,
        **kwargs,
    ):
        """Method for adding retries whenever exception raised during an api calls

        Args:
            resource_name (aiogoogle.resource.Resource): Resource name for which api call will be made.
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
                    resource_object = getattr(storage_client, resource_name)
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
                    else:
                        if sub_method:
                            method_object = getattr(method_object, sub_method)
                        yield await google_client.as_service_account(
                            method_object(**kwargs)
                        )
                    break
            except Exception as exception:
                retry_counter += 1
                if retry_counter > self.retry_count:
                    raise exception
                logger.warning(
                    f"Retry count: {retry_counter} out of {self.retry_count}. Exception: {exception}"
                )
                await asyncio.sleep(DEFAULT_WAIT_MULTIPLIER**retry_counter)

    async def ping(self):
        """Verify the connection with Google Cloud Storage"""
        try:
            await self._connect(
                resource_name="projects",
                method="serviceAccount",
                sub_method="get",
                projectId=self.user_project_id,
            ).__anext__()

            logger.info("Successfully connected to the Google Cloud Storage.")
        except Exception:
            logger.exception("Error while connecting to the Google Cloud Storage.")
            raise

    async def fetch_buckets(self):
        """Fetch the buckets from the Google Cloud Storage.

        Yields:
            Dictionary: Contains the list of fetched buckets from Google Cloud Storage.
        """
        async for bucket in self._connect(
            resource_name="buckets",
            method="list",
            full_response=True,
            project=self.user_project_id,
            userProject=self.user_project_id,
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
            async for blob in self._connect(
                resource_name="objects",
                method="list",
                full_response=True,
                bucket=bucket["id"],
                userProject=self.user_project_id,
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
        ] = f"{CLOUD_STORAGE_BASE_URL}{blob_document['bucket_name']}/{blob_name};tab=live_object?project={self.user_project_id}"
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
        if not (
            self.enable_content_extraction
            and doit
            and os.path.splitext(blob["name"])[-1] in SUPPORTED_FILETYPE
            and int(blob["size"])
        ):
            return

        if int(blob["size"]) > DEFAULT_FILE_SIZE_LIMIT:
            logger.warning(
                f"File size {int(blob['size'])} of file {blob['name']} is larger than {DEFAULT_FILE_SIZE_LIMIT} bytes. Discarding the file content"
            )
            return

        content_text = await self._connect(
            resource_name="objects",
            method="get",
            bucket=blob["bucket_name"],
            object=blob["name"],
            alt="media",
            userProject=self.user_project_id,
        ).__anext__()

        return {
            "_id": blob["id"],
            "_timestamp": blob["_timestamp"],
            "text": content_text,
        }

    async def get_docs(self):
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
