from connectors.es.client import ESClient
from connectors.protocol import CONCRETE_CONNECTORS_INDEX, CONCRETE_JOBS_INDEX
from connectors.protocol import ConnectorIndex

class Connector:
    def __init__(self, config):
        self.config = config

        # initialize ES client
        self.es_client = ESClient(self.config)

        self.connector_index = ConnectorIndex(self.config)


    async def list_connectors(self):
        # TODO move this on top
        try:
            await self.es_client.ensure_exists(
                indices=[CONCRETE_CONNECTORS_INDEX, CONCRETE_JOBS_INDEX]
            )

            connectors = []
            async for connector in self.connector_index.all_connectors():
                connectors.append(connector)

            return connectors

        # TODO catch exceptions
        finally:

            await self.connector_index.close()
            await self.es_client.close()

    async def ping(self):
        if await self.es_client.ping():
            await self.es_client.close()
            return True
        else:
            return False

