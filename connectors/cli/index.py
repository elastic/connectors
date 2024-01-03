import asyncio

from elasticsearch import ApiError

from connectors.es.client import ESManagementClient
from connectors.protocol import ConnectorIndex


class Index:
    def __init__(self, config):
        self.elastic_config = config
        self.es_management_client = ESManagementClient(self.elastic_config)
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
        await self.es_management_client.close()
        await self.connectors_index.close()

    async def __list_indices(self):
        try:
            return await self.es_management_client.list_indices()
        except ApiError as e:
            raise e
        finally:
            await self.__close()

    async def __clean_index(self, index_name):
        try:
            return await self.es_management_client.clean_index(index_name)
        except ApiError:
            return False
        finally:
            await self.__close()

    async def __delete_index(self, index_name):
        try:
            await self.es_management_client.delete_indices([index_name])
            return True
        except ApiError:
            return False
        finally:
            await self.__close()

    async def __index_or_connector_exists(self, index_name):
        try:
            index_exists, connector_doc = await asyncio.gather(
                self.es_management_client.index_exists(index_name),
                self.connectors_index.get_connector_by_index(index_name),
            )
            connector_exists = False if connector_doc is None else True
            return index_exists, connector_exists
        finally:
            await self.__close()
