import asyncio
from elasticsearch import ApiError
from connectors.es import ESClient


class Index:
    def __init__(self, config):
        self.elastic_config = config
        self.es_client = ESClient(self.elastic_config)

    def list_indices(self):
        return asyncio.run(self.__list_indices())["indices"]

    def clean(self, index_name):
        return asyncio.run(self.__clean_index(index_name))

    def delete(self, index_name):
        return asyncio.run(self.__delete_index(index_name))

    def index_or_connector_exists(self, index_name):
        return asyncio.run(self.__index_or_connector_exists(index_name))

    async def __list_indices(self):
        try:
            return await self.es_client.list_indices()
        except ApiError as e:
            raise e
        finally:
            await self.es_client.close()

    async def __clean_index(self, index_name):
        try:
            return await self.es_client.clean_index(index_name)
        except ApiError:
            return False
        finally:
            await self.es_client.close()

    async def __delete_index(self, index_name):
        try:
            await self.es_client.delete_indices([index_name])
            return True
        except ApiError:
            return False
        finally:
            await self.es_client.close()

    async def __index_or_connector_exists(self, index_name):
        try:
            index_exists, connector_doc = await asyncio.gather(
                self.es_client.index_exists(index_name),
                self.es_client.get_connector_doc(index_name),
            )
            connector_exists = False if connector_doc is None else True
            return index_exists, connector_exists
        finally:
            await self.es_client.close()
