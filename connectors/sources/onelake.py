"""OneLake connector to retrieve data from datalakes"""

import asyncio
from functools import partial

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from connectors.source import BaseDataSource

ACCOUNT_NAME = "onelake"


class OneLakeDataSource(BaseDataSource):
    """OneLake"""

    name = "OneLake"
    service_type = "onelake"
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Set up the connection to the azure base client

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.tenant_id = self.configuration["tenant_id"]
        self.client_id = self.configuration["client_id"]
        self.client_secret = self.configuration["client_secret"]
        self.workspace_name = self.configuration["workspace_name"]
        self.data_path = self.configuration["data_path"]
        self.account_url = (
            f"https://{self.configuration['account_name']}.dfs.fabric.microsoft.com"
        )
        self.service_client = None
        self.file_system_client = None
        self.directory_client = None

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for OneLake

        Returns:
            dictionary: Default configuration
        """
        return {
            "tenant_id": {
                "label": "OneLake tenant id",
                "order": 1,
                "type": "str",
            },
            "client_id": {
                "label": "OneLake client id",
                "order": 2,
                "type": "str",
            },
            "client_secret": {
                "label": "OneLake client secret",
                "order": 3,
                "type": "str",
                "sensitive": True,
            },
            "workspace_name": {
                "label": "OneLake workspace name",
                "order": 4,
                "type": "str",
            },
            "data_path": {
                "label": "OneLake data path",
                "tooltip": "Path in format <DataLake>.Lakehouse/files/<Folder path> this is uppercase sensitive, make sure to insert the correct path",
                "order": 5,
                "type": "str",
            },
            "account_name": {
                "tooltip": "In the most cases is 'onelake'",
                "default_value": ACCOUNT_NAME,
                "label": "Account name",
                "required": False,
                "order": 6,
                "type": "str",
            },
        }

    async def initialize(self):
        """Initialize the Azure clients asynchronously"""

        if not self.service_client:
            self.service_client = await self._get_service_client()
            self.file_system_client = await self._get_file_system_client()
            self.directory_client = await self._get_directory_client()

    async def ping(self):
        """Verify the connection with OneLake"""

        self._logger.info("Generating file system client...")

        try:
            await self.initialize()  # Initialize the clients

            await self._get_directory_paths(
                self.configuration["data_path"]
            )  # Condition to check if the connection is successful
            self._logger.info(
                f"Connection to OneLake successful to {self.configuration['data_path']}"
            )
        except Exception:
            self._logger.exception("Error while connecting to OneLake.")
            raise

    async def _process_items_concurrently(
        self, items, process_item_func, max_concurrency=50
    ):
        """Process a list of items concurrently using a semaphore for concurrency control.

        This function applies the `process_item_func` to each item in the `items` list
        using a semaphore to control the level of concurrency.

        Args:
            items (list): List of items to process.
            process_item_func (function): The function to be called for each item.
            max_concurrency (int): Maximum number of concurrent items to process.

        Returns:
            list: A list containing the results of processing each item.
        """

        async def process_item(item, semaphore):
            async with semaphore:
                return await process_item_func(item)

        semaphore = asyncio.Semaphore(max_concurrency)
        tasks = [process_item(item, semaphore) for item in items]
        return await asyncio.gather(*tasks)

    def _get_token_credentials(self):
        """Get the token credentials for OneLake

        Returns:
            obj: Token credentials
        """

        tenant_id = self.configuration["tenant_id"]
        client_id = self.configuration["client_id"]
        client_secret = self.configuration["client_secret"]

        try:
            return ClientSecretCredential(tenant_id, client_id, client_secret)
        except Exception as e:
            self._logger.error(f"Error while getting token credentials: {e}")
            raise

    async def _get_service_client(self):
        """Get the service client for OneLake. The service client is the client that allows to interact with the OneLake service.

        Returns:
            obj: Service client
        """

        try:
            return DataLakeServiceClient(
                account_url=self.account_url,
                credential=self._get_token_credentials(),
            )
        except Exception as e:
            self._logger.error(f"Error while getting service client: {e}")
            raise

    async def _get_file_system_client(self):
        """Get the file system client for OneLake. This client is used to interact with the file system of the OneLake service.

        Returns:
            obj: File system client
        """

        try:
            return self.service_client.get_file_system_client(
                self.configuration["workspace_name"]
            )
        except Exception as e:
            self._logger.error(f"Error while getting file system client: {e}")
            raise

    async def _get_directory_client(self):
        """Get the directory client for OneLake

        Returns:
            obj: Directory client
        """

        try:
            return self.file_system_client.get_directory_client(
                self.configuration["data_path"]
            )
        except Exception as e:
            self._logger.error(f"Error while getting directory client: {e}")
            raise

    async def _get_file_client(self, file_name):
        """Get file client from OneLake

        Args:
            file_name (str): name of the file

        Returns:
            obj: File client
        """

        try:
            return self.directory_client.get_file_client(file_name)
        except Exception as e:
            self._logger.error(f"Error while getting file client: {e}")
            raise

    async def get_files_properties(self, file_clients):
        """Get the properties of a list of file clients

        Args:
            file_clients (list): List of file clients

        Returns:
            list: List of file properties
        """

        async def get_properties(file_client):
            return file_client.get_file_properties()

        return await self._process_items_concurrently(file_clients, get_properties)

    async def _get_directory_paths(self, directory_path):
        """List directory paths from data lake

        Args:
            directory_path (str): Directory path

        Returns:
            list: List of paths
        """

        try:
            if not self.file_system_client:
                await self.initialize()

            loop = asyncio.get_running_loop()
            paths = await loop.run_in_executor(
                None, self.file_system_client.get_paths, directory_path
            )

            return paths
        except Exception as e:
            self._logger.error(f"Error while getting directory paths: {e}")
            raise

    async def format_file(self, file_client):
        """Format file_client to be processed

        Args:
            file_client (obj): File object

        Returns:
            dict: Formatted file
        """

        try:
            loop = asyncio.get_running_loop()
            file_properties = await loop.run_in_executor(
                None, file_client.get_file_properties
            )

            return {
                "_id": f"{file_client.file_system_name}_{file_properties.name.split('/')[-1]}",
                "name": file_properties.name.split("/")[-1],
                "created_at": file_properties.creation_time.isoformat(),
                "_timestamp": file_properties.last_modified.isoformat(),
                "size": file_properties.size,
            }
        except Exception as e:
            self._logger.error(
                f"Error while formatting file or getting file properties: {e}"
            )
            raise

    async def download_file(self, file_client):
        """Download file from OneLake

        Args:
            file_client (obj): File client

        Returns:
            generator: File stream
        """

        try:
            loop = asyncio.get_running_loop()
            download = await loop.run_in_executor(None, file_client.download_file)

            stream = download.chunks()
            for chunk in stream:
                yield chunk
        except Exception as e:
            self._logger.error(f"Error while downloading file: {e}")
            raise

    async def get_content(self, file_name, doit=None, timestamp=None):
        """Obtains the file content for the specified file in `file_name`.

        Args:
            file_name (obj): The file name to process to obtain the content.
            timestamp (timestamp, optional): Timestamp of blob last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to None.

        Returns:
            str: Content of the file or None if not applicable.
        """

        if not doit:
            return

        file_client = await self._get_file_client(file_name)
        file_properties = file_client.get_file_properties()
        file_extension = self.get_file_extension(file_name)

        doc = {
            "_id": f"{file_client.file_system_name}_{file_properties.name}",  # id in format <file_system_name>_<file_name>
        }

        can_be_downloaded = self.can_file_be_downloaded(
            file_extension=file_extension,
            filename=file_properties.name,
            file_size=file_properties.size,
        )

        if not can_be_downloaded:
            self._logger.warning(
                f"File {file_properties.name} cannot be downloaded. Skipping."
            )
            return doc

        self._logger.debug(f"Downloading file {file_properties.name}...")
        extracted_doc = await self.download_and_extract_file(
            doc=doc,
            source_filename=file_properties.name.split("/")[-1],
            file_extension=file_extension,
            download_func=partial(self.download_file, file_client),
        )

        return extracted_doc if extracted_doc is not None else doc

    async def prepare_files(self, doc_paths):
        """Prepare files for processing

        Args:
            doc_paths (list): List of paths extracted from OneLake

        Returns:
            list: List of files
        """

        async def prepare_single_file(path):
            file_name = path.name.split("/")[-1]
            field_client = await self._get_file_client(file_name)
            return await self.format_file(field_client)

        files = await self._process_items_concurrently(doc_paths, prepare_single_file)

        for file in files:
            yield file

    async def get_docs(self, filtering=None):
        """Get documents from OneLake and index them

        Yields:
            tuple: dictionary with meta-data of each file and a partial function to get the file content.
        """

        self._logger.info(f"Fetching files from OneLake datalake {self.data_path}")

        directory_paths = await self._get_directory_paths(
            self.configuration["data_path"]
        )
        directory_paths = list(directory_paths)

        self._logger.debug(f"Found {len(directory_paths)} files in {self.data_path}")

        async for file in self.prepare_files(directory_paths):
            file_dict = file

            yield file_dict, partial(self.get_content, file_dict["name"])
