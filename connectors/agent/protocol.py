from es_agent_client.generated import elastic_agent_client_pb2 as proto
from es_agent_client.handler.action import BaseActionHandler
from es_agent_client.handler.checkin import BaseCheckinHandler
from es_agent_client.util.logger import logger


class ConnectorActionHandler(BaseActionHandler):
    async def handle_action(self, action: proto.ActionRequest):
        msg = (
            f"This connector component can't handle action requests. Received: {action}"
        )
        raise NotImplementedError(msg)


class ConnectorCheckinHandler(BaseCheckinHandler):
    def __init__(self, client, agent_config, service_manager):
        super().__init__(client)
        self.agent_config = agent_config
        self.service_manager = service_manager

    async def apply_from_client(self):
        logger.info("There's new information for the components/units!")
        if self.client.units:
            outputs = [
                unit
                for unit in self.client.units
                if unit.unit_type == proto.UnitType.OUTPUT
            ]
            [
                unit
                for unit in self.client.units
                if unit.unit_type == proto.UnitType.INPUT
            ]

            if len(outputs) > 0 and outputs[0].config:
                source = outputs[0].config.source
                if source.fields.get("hosts") and (
                    source.fields.get("api_key")
                    or source.fields.get("username")
                    and source.fields.get("password")
                ):
                    logger.info("Updating connector service manager config")

                    es_creds = {
                        "host": source["hosts"][0],
                    }

                    if source.fields.get("api_key"):
                        es_creds["api_key"] = source["api_key"]
                    elif source.fields.get("username") and source.fields.get(
                        "password"
                    ):
                        es_creds["username"] = source["username"]
                        es_creds["password"] = source["password"]
                    else:
                        msg = "Invalid Elasticsearch credentials"
                        raise ValueError(msg)

                    new_config = {
                        "elasticsearch": es_creds,
                    }
                    # this restarts all connector services
                    # this should happen only when user changes the target elasticsearch output
                    # in agent policy

                    self.agent_config.update(new_config)
                    self.service_manager.restart()
