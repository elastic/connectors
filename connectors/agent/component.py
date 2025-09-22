#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import sys

from elastic_agent_client.client import V2Options, VersionInfo
from elastic_agent_client.reader import new_v2_from_reader
from elastic_agent_client.service.actions import ActionsService
from elastic_agent_client.service.checkin import CheckinV2Service

from connectors.agent.config import ConnectorsAgentConfigurationWrapper
from connectors.agent.logger import get_logger
from connectors.agent.protocol import ConnectorActionHandler, ConnectorCheckinHandler
from connectors.agent.service_manager import ConnectorServiceManager
from connectors.services.base import MultiService
from logging import Logger

logger: Logger = get_logger("component")

CONNECTOR_SERVICE = "connector-service"


class ConnectorsAgentComponent:
    """Entry point into running connectors service in Agent.

    This class provides a simple abstraction over Agent components and Connectors Service manager.

    It instantiates everything needed to read from Agent protocol, creates a wrapper around Connectors Service
    and provides applied interface to be able to run it in 2 simple methods: run and stop.
    """

    def __init__(self) -> None:
        """Inits the class.

        Init should be safe to call without expectations of side effects (connections to Agent, blocking or anything).
        """
        self.ver = VersionInfo(
            name=CONNECTOR_SERVICE, meta={"input": CONNECTOR_SERVICE}
        )
        self.opts = V2Options()
        self.buffer = sys.stdin.buffer
        self.config_wrapper = ConnectorsAgentConfigurationWrapper()

    async def run(self) -> None:
        """Start reading from Agent protocol and run Connectors Service with settings reported by agent.

        This method can block if it's not running from Agent - it expects the client to be able to read messages
        on initialisation. These messages are a handshake and it's a sync handshake.
        If no messages are sent, this method can and will hang.

        However, if ran under agent, this method will read configuration from Agent and attempt to start an
        instance of Connectors Service with this configuration.

        Additionally services for handling Check-in and Actions will be started to implement the protocol correctly.
        """
        logger.info("Starting connectors agent component")
        client = new_v2_from_reader(self.buffer, self.ver, self.opts)
        action_handler = ConnectorActionHandler()
        self.connector_service_manager = ConnectorServiceManager(self.config_wrapper)
        checkin_handler = ConnectorCheckinHandler(
            client,
            self.config_wrapper,
            self.connector_service_manager,
        )

        self.multi_service = MultiService(
            CheckinV2Service(client, checkin_handler),
            ActionsService(client, action_handler),
            self.connector_service_manager,
        )

        await self.multi_service.run()

    def stop(self, sig) -> None:
        """Shutdown everything running in the component.

        Attempts to gracefully shutdown the services that are running under the component.
        """
        logger.info("Shutting down connectors agent component")
        self.multi_service.shutdown(sig)
