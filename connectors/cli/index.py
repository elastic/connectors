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
