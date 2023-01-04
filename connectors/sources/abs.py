#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Azure Blob Storage source module responsible to fetch documents from Azure Blob Storage"""

import os
from functools import partial

from azure.storage.blob.aio import BlobClient, BlobServiceClient, ContainerClient

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import TIKA_SUPPORTED_FILETYPES, get_base64_value

BLOB_SCHEMA = {
    "title": "name",
    "tier": "blob_tier",
    "size": "size",
    "container": "container",
}
DEFAULT_CONTENT_EXTRACTION = True
DEFAULT_FILE_SIZE_LIMIT = 10485760
DEFAULT_RETRY_COUNT = 3


class AzureBlobStorageDataSource(BaseDataSource):
    """Class to fetch documents from Azure Blob Storage"""

    def __init__(self, connector):
        """Setup the connection to the azure base client

        Args:
            connector (BYOConnector): Object of the BYOConnector class
        """
        super().__init__(connector=connector)
        self.connection_string = self.configuration["connection_string"]
        self.enable_content_extraction = self.configuration.get(
            "enable_content_extraction", DEFAULT_CONTENT_EXTRACTION
        )
        self.retry_count = int(
            self.configuration.get("retry_count", DEFAULT_RETRY_COUNT)
        )

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Azure Blob Storage

        Returns:
            dictionary: Default configuration
        """
        return {
            "connection_string": {
                "value": "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
                "label": "Azure Blob Storage Connection String",
                "type": "str",
            },
            "connector_name": {
                "value": "Azure Blob Storage Connector",
                "label": "Friendly name for the connector",
                "type": "str",
            },
            "enable_content_extraction": {
                "value": DEFAULT_CONTENT_EXTRACTION,
                "label": "Flag to check if content extraction is enabled or not",
                "type": "bool",
            },
            "retry_count": {
                "value": DEFAULT_RETRY_COUNT,
                "label": "How many retry count for fetching rows on each call",
                "type": "int",
            },
        }

    def _validate_connection_string(self):
        """Validates user input connection string for empty and multiple input

        Raises:
            Exception: Empty or Multiple connection string
        """
        if not self.connection_string:
            raise Exception("No connection string provided.")

        connection_fields, multiple_connection_fields = [
            "AccountName",
            "AccountKey",
        ], []
        for field in connection_fields:
            if self.connection_string.count(f"{field}=") > 1:
                multiple_connection_fields.append(field)

        if multiple_connection_fields:
            raise Exception(
                f"Configured connection string can't include: {multiple_connection_fields} multiple values."
            )

    async def ping(self):
        """Verify the connection with Azure Blob Storage"""
        logger.info("Validating ABS connection string...")
        self._validate_connection_string()
        try:
            async with BlobServiceClient.from_connection_string(
                conn_str=self.connection_string, retry_total=self.retry_count
            ) as azure_base_client:
                await azure_base_client.get_account_information()
                logger.info("Successfully connected to the Azure Blob Storage.")
        except Exception:
            logger.exception("Error while connecting to the Azure Blob Storage.")
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
        if not (self.enable_content_extraction and doit and blob_size):
            return

        blob_name = blob["title"]
        if os.path.splitext(blob_name)[-1] not in TIKA_SUPPORTED_FILETYPES:
            logger.warning(f"{blob_name} can't be extracted")
            return

        if blob_size > DEFAULT_FILE_SIZE_LIMIT:
            logger.warning(
                f"File size {blob_size} of file {blob_name} is larger than {DEFAULT_FILE_SIZE_LIMIT} bytes. Discarding the file content"
            )
            return

        async with BlobClient.from_connection_string(
            conn_str=self.connection_string,
            container_name=blob["container"],
            blob_name=blob_name,
            retry_total=self.retry_count,
        ) as blob_client:
            data = await blob_client.download_blob()
            content = await data.readall()
            return {
                "_id": blob["id"],
                "_timestamp": blob["_timestamp"],
                "_attachment": get_base64_value(content=content),
            }

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

    async def get_docs(self):
        """Get documents from Azure Blob Storage

        Yields:
            dictionary: Documents from Azure Blob Storage
        """
        async for container in self.get_container():
            async for blob in self.get_blob(container=container):
                yield blob, partial(self.get_content, blob)
