import asyncio

from elasticsearch import ApiError

from connectors.es.cli_client import CLIClient
from connectors.protocol import (
    CONCRETE_CONNECTORS_INDEX,
    CONCRETE_JOBS_INDEX,
    ConnectorIndex,
)


class Index:
    def __init__(self, config):
        self.elastic_config = config
        self.cli_client = CLIClient(self.elastic_config)
        self.connectors_index = ConnectorIndex(self.elastic_config)

    def list_indices(self):
        return asyncio.run(self.__list_indices())["indices"]

    def clean(self, index_name):
        return asyncio.run(self.__clean_index(index_name))

    def delete(self, index_name):
        return asyncio.run(self.__delete_index(index_name))

    def index_or_connector_exists(self, index_name):
        return asyncio.run(self.__index_or_connector_exists(index_name))

    async def __close(self):
        await self.cli_client.close()
        await self.connectors_index.close()

    async def __list_indices(self):
        try:
            return await self.cli_client.list_indices()
        except ApiError as e:
            raise e
        finally:
            await self.__close()

    async def __clean_index(self, index_name):
        try:
            return await self.cli_client.clean_index(index_name)
        except ApiError:
            return False
        finally:
            await self.__close()

    async def __delete_index(self, index_name):
        try:
            await self.cli_client.delete_indices([index_name])
            return True
        except ApiError:
            return False
        finally:
            await self.__close()

    async def __index_or_connector_exists(self, index_name):
        try:
            await self.cli_client.ensure_exists(
                indices=[CONCRETE_CONNECTORS_INDEX, CONCRETE_JOBS_INDEX]
            )

            index_exists, connector_doc = await asyncio.gather(
                self.cli_client.index_exists(index_name),
                self.connectors_index.get_connector_by_index(index_name),
            )
            connector_exists = False if connector_doc is None else True
            return index_exists, connector_exists
        finally:
            await self.__close()
