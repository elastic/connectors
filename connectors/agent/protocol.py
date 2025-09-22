#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from elastic_agent_client.generated import elastic_agent_client_pb2 as proto
from elastic_agent_client.handler.action import BaseActionHandler
from elastic_agent_client.handler.checkin import BaseCheckinHandler

from connectors.agent.connector_record_manager import ConnectorRecordManager
from connectors.agent.logger import get_logger
from elastic_agent_client.client import V2
from logging import Logger

logger: Logger = get_logger("protocol")


CONNECTORS_INPUT_TYPE = "connectors-py"
ELASTICSEARCH_OUTPUT_TYPE = "elasticsearch"


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

    If the connector document with given ID doesn't exist, it creates a new one.
    """

    def __init__(
        self,
        client: V2,
        agent_connectors_config_wrapper,
        service_manager,
    ) -> None:
        """Inits the class.

        Initing this class should not produce side-effects.
        """
        super().__init__(client)
        self.agent_connectors_config_wrapper = agent_connectors_config_wrapper
        self.service_manager = service_manager
        self.connector_record_manager = ConnectorRecordManager()

    async def apply_from_client(self) -> None:
        """Implementation of BaseCheckinHandler.apply_from_client

        This method is called by the Agent Protocol handlers when there's a check-in event
        coming from Agent. This class reads the event and runs business logic based on the
        content of the event.

        If this class blocks for too long, the component will mark the agent as failed:
        agent expects the components to respond within 30 seconds.
        See comment in https://github.com/elastic/elastic-agent-client/blob/main/elastic-agent-client.proto#L29
        """
        logger.info("New information available for components/units")

        if self.client.units:
            logger.debug("Client reported units")

            outputs = [
                unit
                for unit in self.client.units
                if unit.unit_type == proto.UnitType.OUTPUT
            ]

            # Filter Elasticsearch outputs from the available outputs
            elasticsearch_outputs = [
                output_unit
                for output_unit in outputs
                if output_unit.config
                and output_unit.config.type == ELASTICSEARCH_OUTPUT_TYPE
            ]

            inputs = [
                unit
                for unit in self.client.units
                if unit.unit_type == proto.UnitType.INPUT
            ]

            # Ensure only the single valid connector input is selected from the inputs
            connector_inputs = [
                input_unit
                for input_unit in inputs
                if input_unit.config.type == CONNECTORS_INPUT_TYPE
            ]

            if connector_inputs:
                if len(connector_inputs) > 1:
                    logger.warning(
                        "Multiple connector inputs detected. The first connector input defined in the agent policy will be used."
                    )

                logger.debug("Connector input found.")

                connector_input = connector_inputs[0]

                def _extract_unit_config_value(unit, field_name):
                    field_value = unit.config.source.fields.get(field_name)
                    return field_value.string_value if field_value else None

                service_type = _extract_unit_config_value(
                    connector_input, "service_type"
                )
                connector_name = _extract_unit_config_value(
                    connector_input, "connector_name"
                )

                connector_id = _extract_unit_config_value(
                    connector_input, "connector_id"
                )
                # If "connector_id" is not explicitly provided as a policy parameter,
                # use the "id" from the fleet policy as a fallback for the connector ID.
                # The connector ID must be encoded in the policy to associate the integration
                # with the connector being managed by the policy.
                if not connector_id:
                    connector_id = _extract_unit_config_value(connector_input, "id")

                logger.info(
                    f"Connector input found. Service type: {service_type}, Connector ID: {connector_id}, Connector Name: {connector_name}"
                )

                if elasticsearch_outputs:
                    if len(elasticsearch_outputs) > 1:
                        logger.warning(
                            "Multiple Elasticsearch outputs detected. The first ES output defined in the agent policy will be used."
                        )

                    logger.debug("Elasticsearch outputs found.")

                    elasticsearch_output = elasticsearch_outputs[0]

                    configuration_changed = (
                        self.agent_connectors_config_wrapper.try_update(
                            connector_id=connector_id,
                            service_type=service_type,
                            output_unit=elasticsearch_output,
                        )
                    )

                    # After updating the configuration, ensure all connector records exist in the connector index
                    await self.connector_record_manager.ensure_connector_records_exist(
                        agent_config=self.agent_connectors_config_wrapper.get_specific_config(),
                        connector_name=connector_name,
                    )

                    if configuration_changed:
                        logger.info(
                            "Connector service manager config updated. Restarting service manager."
                        )
                        self.service_manager.restart()
                    else:
                        logger.debug("No changes to connectors config")
                else:
                    logger.warning("No Elasticsearch output found")
            else:
                logger.warning("No connector integration input found")
