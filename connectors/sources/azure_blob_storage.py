#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Azure Blob Storage source module responsible to fetch documents from Azure Blob Storage"""
from functools import partial

from azure.storage.blob.aio import BlobServiceClient, ContainerClient

from connectors.source import BaseDataSource

BLOB_SCHEMA = {
    "title": "name",
    "tier": "blob_tier",
    "size": "size",
    "container": "container",
}
DEFAULT_RETRY_COUNT = 3
MAX_CONCURRENT_DOWNLOADS = (
    100  # Max concurrent download supported by Azure Blob Storage
)


class AzureBlobStorageDataSource(BaseDataSource):
    """Azure Blob Storage"""

    name = "Azure Blob Storage"
    service_type = "azure_blob_storage"
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Set up the connection to the azure base client

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.connection_string = None
        self.retry_count = self.configuration["retry_count"]
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.containers = self.configuration["containers"]
        self.container_clients = {}

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
            "containers": {
                "display": "textarea",
                "label": "Azure Blob Storage containers",
                "order": 4,
                "type": "list",
            },
            "retry_count": {
                "default_value": DEFAULT_RETRY_COUNT,
                "display": "numeric",
                "label": "Retries per request",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_DOWNLOADS,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 6,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "validations": [
                    {"type": "less_than", "constraint": MAX_CONCURRENT_DOWNLOADS + 1}
                ],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 7,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
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
            container_metadata (string): Blob container metadata

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
        file_size = int(blob["size"])
        if not (doit and file_size > 0):
            return

        filename = blob["title"]
        if blob["tier"] == "Archive":
            self._logger.warning(
                f"{filename} can't be downloaded as blob tier is archive"
            )
            return

        file_extension = self.get_file_extension(filename).lower()
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        document = {"_id": blob["id"], "_timestamp": blob["_timestamp"]}
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(self.blob_download_func, filename, blob["container"], file_size),
        )

    def _get_container_client(self, container_name):
        if self.container_clients.get(container_name) is None:
            self.container_clients[
                container_name
            ] = ContainerClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=container_name,
                retry_total=self.retry_count,
            )
            return self.container_clients[container_name]
        else:
            return self.container_clients[container_name]

    async def blob_download_func(self, blob_name, container_name, file_size):
        container_client = self._get_container_client(container_name=container_name)
        offset = 0
        length = 100000
        while file_size > 0:
            data = await container_client.download_blob(
                blob=blob_name,
                offset=offset,
                length=length,
                max_concurrency=MAX_CONCURRENT_DOWNLOADS,
            )
            content = await data.read()
            length = data.size
            offset = offset + length
            file_size = file_size - length
            yield content

    async def get_container(self, container_list):
        """Get containers from Azure Blob Storage via azure base client
        Args:
            container_list (list): List of containers

        Yields:
            dictionary: Container document with name & metadata
        """
        container_set = set(container_list)
        async with BlobServiceClient.from_connection_string(
            conn_str=self.connection_string, retry_total=self.retry_count
        ) as azure_base_client:
            try:
                async for container in azure_base_client.list_containers(
                    include_metadata=True
                ):
                    if "*" in container_set or container["name"] in container_set:
                        yield {
                            "name": container["name"],
                            "metadata": container["metadata"],
                        }
                    if "*" not in container_set and container["name"] in container_set:
                        container_set.remove(container["name"])
                        if not container_set:
                            return
                if container_set and "*" not in container_set:
                    self._logger.warning(
                        f"Container(s) {','.join(container_set)} are configured but not found."
                    )
            except Exception as exception:
                self._logger.warning(
                    f"Something went wrong while fetching containers. Error: {exception}"
                )

    async def get_blob(self, container):
        """Get blobs for a specific container from Azure Blob Storage via container client

        Args:
            container (dictionary): Container for creating the container specific client & blob document

        Yields:
            dictionary: Formatted blob document
        """
        container_client = self._get_container_client(container["name"])
        try:
            async for blob in container_client.list_blobs(include=["metadata"]):
                yield self.prepare_blob_doc(
                    blob=blob, container_metadata=container["metadata"]
                )
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching blobs from {container['name']}. Error: {exception}"
            )

    async def get_docs(self, filtering=None):
        """Get documents from Azure Blob Storage

        Yields:
            dictionary: Documents from Azure Blob Storage
        """
        async for container_data in self.get_container(container_list=self.containers):
            if container_data:
                async for blob in self.get_blob(container=container_data):
                    yield blob, partial(self.get_content, blob)
