import sys

from elastic_agent_client.client import V2Options, VersionInfo
from elastic_agent_client.reader import new_v2_from_reader
from elastic_agent_client.service.actions import ActionsService
from elastic_agent_client.service.checkin import CheckinV2Service

from connectors.agent.config import ConnectorsAgentConfiguration
from connectors.agent.protocol import ConnectorActionHandler, ConnectorCheckinHandler
from connectors.agent.service_manager import ConnectorServiceManager
from connectors.services.base import MultiService

CONNECTOR_SERVICE = "connector-service"


class ConnectorsAgentComponent:
    def __init__(self):
        self.ver = VersionInfo(
            name=CONNECTOR_SERVICE, meta={"input": CONNECTOR_SERVICE}
        )
        self.opts = V2Options()
        self.buffer = sys.stdin.buffer
        self.agent_config = ConnectorsAgentConfiguration()

    async def run(self):
        client = new_v2_from_reader(self.buffer, self.ver, self.opts)
        action_handler = ConnectorActionHandler()
        self.connector_service_manager = ConnectorServiceManager(self.agent_config)
        checkin_handler = ConnectorCheckinHandler(
            client, self.agent_config, self.connector_service_manager
        )

        self.multi_service = MultiService(
            CheckinV2Service(client, checkin_handler),
            ActionsService(client, action_handler),
            self.connector_service_manager,
        )

        await self.multi_service.run()

    def stop(self, sig):
        self.multi_service.shutdown(sig)
