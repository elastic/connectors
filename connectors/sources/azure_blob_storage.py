#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Azure Blob Storage source module responsible to fetch documents from Azure Blob Storage"""
import asyncio
import os
from functools import partial

import aiofiles
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from azure.storage.blob.aio import BlobClient, BlobServiceClient, ContainerClient

from connectors.source import BaseDataSource
from connectors.utils import TIKA_SUPPORTED_FILETYPES, convert_to_b64

BLOB_SCHEMA = {
    "title": "name",
    "tier": "blob_tier",
    "size": "size",
    "container": "container",
}
DEFAULT_FILE_SIZE_LIMIT = 10485760
DEFAULT_RETRY_COUNT = 3
MAX_CONCURRENT_DOWNLOADS = (
    100  # Max concurrent download supported by Azure Blob Storage
)


class AzureBlobStorageDataSource(BaseDataSource):
    """Azure Blob Storage"""

    name = "Azure Blob Storage"
    service_type = "azure_blob_storage"

    def __init__(self, configuration):
        """Set up the connection to the azure base client

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.connection_string = None
        self.retry_count = self.configuration["retry_count"]
        self.concurrent_downloads = self.configuration["concurrent_downloads"]

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by azure blob storage

        Args:
            options (dictionary): Config bulker options

        Raises:
            Exception: Invalid configured concurrent_downloads
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Azure Blob Storage

        Returns:
            dictionary: Default configuration
        """
        return {
            "account_name": {
                "label": "Azure Blob Storage account name",
                "order": 1,
                "type": "str",
            },
            "account_key": {
                "label": "Azure Blob Storage account key",
                "order": 2,
                "type": "str",
            },
            "blob_endpoint": {
                "label": "Azure Blob Storage blob endpoint",
                "order": 3,
                "type": "str",
            },
            "retry_count": {
                "default_value": DEFAULT_RETRY_COUNT,
                "display": "numeric",
                "label": "Retries per request",
                "order": 4,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_DOWNLOADS,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "validations": [
                    {"type": "less_than", "constraint": MAX_CONCURRENT_DOWNLOADS + 1}
                ],
            },
        }

    def _configure_connection_string(self):
        """Generates connection string for ABS

        Returns:
            str: Connection string with user input configuration fields
        """

        return f'AccountName={self.configuration["account_name"]};AccountKey={self.configuration["account_key"]};BlobEndpoint={self.configuration["blob_endpoint"]}'

    async def ping(self):
        """Verify the connection with Azure Blob Storage"""
        self._logger.info("Generating connection string...")
        self.connection_string = self._configure_connection_string()
        try:
            async with BlobServiceClient.from_connection_string(
                conn_str=self.connection_string, retry_total=self.retry_count
            ) as azure_base_client:
                await azure_base_client.get_account_information()
                self._logger.info("Successfully connected to the Azure Blob Storage.")
        except Exception:
            self._logger.exception("Error while connecting to the Azure Blob Storage.")
            raise

    def prepare_blob_doc(self, blob, container_metadata):
        """Prepare key mappings to blob document

        Args:
            blob (dictionary): Blob document from Azure Blob Storage
            container_metadata (string): Blob container meta data

        Returns:
            dictionary: Modified document with the help of adapter schema
        """
        document = {
            "_id": f"{blob['container']}/{blob['name']}",
            "_timestamp": blob["last_modified"].isoformat(),
            "created at": blob["creation_time"].isoformat(),
            "content type": blob["content_settings"]["content_type"],
            "container metadata": str(container_metadata),
            "metadata": str(blob["metadata"]),
            "leasedata": str(blob["lease"]),
        }
        for elasticsearch_field, azure_blob_storage_field in BLOB_SCHEMA.items():
            document[elasticsearch_field] = blob[azure_blob_storage_field]
        return document

    async def get_content(self, blob, timestamp=None, doit=None):
        """Get blob content via specific blob client

        Args:
            blob (dictionary): Modified blob document
            timestamp (timestamp, optional): Timestamp of blob last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to None.

        Returns:
            dictionary: Content document with id, timestamp & text
        """
        blob_size = int(blob["size"])
        if not (doit and blob_size > 0):
            return

        blob_name = blob["title"]
        if (os.path.splitext(blob_name)[-1]).lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.warning(f"{blob_name} can't be extracted")
            return

        if blob["tier"] == "Archive":
            self._logger.warning(
                f"{blob_name} can't be downloaded as blob tier is archive"
            )
            return

        if blob_size > DEFAULT_FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {blob_size} of file {blob_name} is larger than {DEFAULT_FILE_SIZE_LIMIT} bytes. Discarding the file content"
            )
            return

        document = {"_id": blob["id"], "_timestamp": blob["_timestamp"]}

        async with BlobClient.from_connection_string(
            conn_str=self.connection_string,
            container_name=blob["container"],
            blob_name=blob_name,
            retry_total=self.retry_count,
        ) as blob_client:
            data = await blob_client.download_blob()
            temp_filename = ""
            async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
                temp_filename = async_buffer.name
                async for content in data.chunks():
                    await async_buffer.write(content)
            self._logger.debug(f"Calling convert_to_b64 for file : {blob_name}")
            await asyncio.to_thread(convert_to_b64, source=temp_filename)
            async with aiofiles.open(file=temp_filename, mode="r") as async_buffer:
                # base64 on macOS will add a EOL, so we strip() here
                document["_attachment"] = (await async_buffer.read()).strip()
            await remove(str(temp_filename))
        return document

    async def get_container(self):
        """Get containers from Azure Blob Storage via azure base client

        Yields:
            dictionary: Container document with name & meta data
        """
        async with BlobServiceClient.from_connection_string(
            conn_str=self.connection_string, retry_total=self.retry_count
        ) as azure_base_client:
            async for container in azure_base_client.list_containers(
                include_metadata=True
            ):
                yield {
                    "name": container["name"],
                    "metadata": container["metadata"],
                }

    async def get_blob(self, container):
        """Get blobs for a specific container from Azure Blob Storage via container client

        Args:
            container (dictionary): Container for creating the container specific client & blob document

        Yields:
            dictionary: Formatted blob document
        """
        async with ContainerClient.from_connection_string(
            conn_str=self.connection_string,
            container_name=container["name"],
            retry_total=self.retry_count,
        ) as container_client:
            async for blob in container_client.list_blobs(include=["metadata"]):
                yield self.prepare_blob_doc(
                    blob=blob, container_metadata=container["metadata"]
                )

    async def get_docs(self, filtering=None):
        """Get documents from Azure Blob Storage

        Yields:
            dictionary: Documents from Azure Blob Storage
        """
        async for container in self.get_container():
            async for blob in self.get_blob(container=container):
                yield blob, partial(self.get_content, blob)
