#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from connectors.agent.logger import get_logger
from connectors.protocol import ConnectorIndex
from connectors.utils import generate_random_id

logger = get_logger("agent_connector_record_manager")


class ConnectorRecordManager:
    """
    Manages connector records in Elasticsearch, ensuring that connectors tied to agent components
    exist in the connector index. It creates the connector record if necessary.
    """

    def __init__(self):
        self.connector_index = None

    async def ensure_connector_records_exist(self, agent_config, connector_name=None):
        """
        Ensure that connector records exist for all connectors specified in the agent configuration.

        If the connector record with a given ID doesn't exist, create a new one.
        """

        config_ready, msg = self._check_agent_config_ready(agent_config)

        if not config_ready:
            logger.debug(
                f"Agent configuration is not ready to create a connector record. Skipping. Reason: {msg} "
            )
            return

        # Initialize the ES client if it's not already initialized
        if not self.connector_index:
            self.connector_index = ConnectorIndex(agent_config.get("elasticsearch"))

        for connector_config in agent_config.get("connectors"):
            connector_id, service_type = (
                connector_config["connector_id"],
                connector_config["service_type"],
            )

            if not connector_name:
                logger.debug("Connector name not provided, generating a random one.")
                random_connector_name_id = generate_random_id(length=4)
                connector_name = f"[Elastic-managed] {service_type} connector {random_connector_name_id}"

            if not await self.connector_index.connector_exists(connector_id):
                try:
                    await self.connector_index.connector_put(
                        connector_id=connector_id,
                        service_type=service_type,
                        connector_name=connector_name,
                        is_native=True,
                    )
                    logger.info(f"Created connector record for {connector_id}")
                except Exception as e:
                    logger.error(
                        f"Failed to create connector record for {connector_id}: {e}"
                    )
                    raise e

    def _check_agent_config_ready(self, agent_config):
        """
        Validates the agent configuration to check if all info is present to create a connector record.

        Returns:
            tuple: (bool, str or None) - True and None if valid, otherwise False and an error message.
        """

        connectors = agent_config.get("connectors")
        if connectors is None:
            return False, "No 'connectors' key found in the service configuration."

        if len(connectors) == 0:
            return False, "Empty 'connectors' array found in the service configuration."

        for connector in connectors:
            if "connector_id" not in connector:
                return (
                    False,
                    "No 'connector_id' key found in the connector object.",
                )

            if "service_type" not in connector:
                return (
                    False,
                    "No 'service_type' key found in the connector object.",
                )

        elasticsearch_config = agent_config.get("elasticsearch")
        if not elasticsearch_config:
            return False, "No 'elasticsearch' key found in the service configuration."

        if "host" not in elasticsearch_config:
            return False, "No 'host' key found in the elasticsearch configuration."

        if "api_key" not in elasticsearch_config:
            return False, "No 'api_key' key found in the elasticsearch configuration."

        return True, None
