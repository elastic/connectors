from elastic_agent_client.generated import elastic_agent_client_pb2 as proto
from elastic_agent_client.handler.action import BaseActionHandler
from elastic_agent_client.handler.checkin import BaseCheckinHandler
from elastic_agent_client.util.logger import logger


class ConnectorActionHandler(BaseActionHandler):
    """Class handling Agent actions.

    As there are no actions that we can respond to, we don't actually do anything here.
    """

    async def handle_action(self, action: proto.ActionRequest):
        """Implementation of BaseActionHandler.handle_action

        Right now does nothing as Connectors Service has no actions to respond to.
        """
        msg = (
            f"This connector component can't handle action requests. Received: {action}"
        )
        raise NotImplementedError(msg)


class ConnectorCheckinHandler(BaseCheckinHandler):
    """Class handling to Agent check-in events.

    Agent sends check-in events from time to time that might contain
    information that's needed to run Connectors Service.

    This class reads the events, sees if there's a reported change to connector-specific settings,
    tries to update the configuration and, if the configuration is updated, restarts the Connectors Service.
    """

    def __init__(self, client, agent_connectors_config_wrapper, service_manager):
        """Inits the class.

        Initing this class should not produce side-effects.
        """
        super().__init__(client)
        self.agent_connectors_config_wrapper = agent_connectors_config_wrapper
        self.service_manager = service_manager

    async def apply_from_client(self):
        """Implementation of BaseCheckinHandler.apply_from_client

        This method is called by the Agent Protocol handlers when there's a check-in event
        coming from Agent. This class reads the event and runs business logic based on the
        content of the event.

        If this class blocks for too long, the component will mark the agent as failed:
        agent expects the components to respond within 30 seconds.
        See comment in https://github.com/elastic/elastic-agent-client/blob/main/elastic-agent-client.proto#L29
        """
        logger.info("There's new information for the components/units!")
        if self.client.units:
            logger.debug("Client reported units")
            outputs = [
                unit
                for unit in self.client.units
                if unit.unit_type == proto.UnitType.OUTPUT
            ]

            if len(outputs) > 0 and outputs[0].config:
                logger.debug("Outputs were found")
                source = outputs[0].config.source

                changed = self.agent_connectors_config_wrapper.try_update(source)
                if changed:
                    logger.info("Updating connector service manager config")
                    self.service_manager.restart()
                else:
                    logger.debug("No changes to connectors config")
