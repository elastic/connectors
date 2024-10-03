#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.agent.logger import get_logger
from connectors.es.index import DocumentNotFoundError
from connectors.protocol import ConnectorIndex

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

        if not self._agent_config_ready(agent_config):
            return

        # Initialize the ES client if it's not already initialized
        if not self.connector_index:
            self.connector_index = ConnectorIndex(agent_config.get("elasticsearch"))

        for connector_config in self._get_connectors(agent_config):
            connector_id, service_type = (
                connector_config["connector_id"],
                connector_config["service_type"],
            )

            if not connector_name:
                connector_name = f"[Agentless] {service_type} connector"

            if not await self._connector_exists(connector_id):
                try:
                    await self.connector_index.connector_put(
                        connector_id=connector_id,
                        service_type=service_type,
                        connector_name=connector_name,
                    )
                    logger.info(f"Created connector record for {connector_id}")
                except Exception as e:
                    logger.error(
                        f"Failed to create connector record for {connector_id}: {e}"
                    )
                    raise e

    def _agent_config_ready(self, agent_config):
        """
        Validates the agent configuration to check if all info is present to create a connector record.
        """
        connectors = agent_config.get("connectors")
        if connectors is None or len(connectors) == 0:
            return False

        for connector in connectors:
            if "connector_id" not in connector or "service_type" not in connector:
                return False

        elasticsearch_config = agent_config.get("elasticsearch")
        if not elasticsearch_config:
            return False

        if "host" not in elasticsearch_config or "api_key" not in elasticsearch_config:
            return False

        return True

    async def _connector_exists(self, connector_id):
        try:
            doc = await self.connector_index.fetch_by_id(connector_id)
            return doc is not None
        except DocumentNotFoundError:
            return False
        except Exception as e:
            logger.error(f"Error checking existence of connector '{connector_id}': {e}")
            return False

    def _get_connectors(self, agent_config):
        return agent_config.get("connectors")
