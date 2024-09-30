#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64

from connectors.agent.logger import get_logger
from connectors.config import add_defaults
from connectors.utils import nested_get_from_dict

logger = get_logger("config")


class ConnectorsAgentConfigurationWrapper:
    """A wrapper that facilitates passing configuration from Agent to Connectors Service.

    This class is responsible for:
    - Storing in-memory configuration of Connectors Service running on Agent
    - Transforming configuration reported by Agent to valid Connectors Service configuration
    - Indicating that configuration has changed so that the user of the class can trigger the restart
    """

    def __init__(self):
        """Inits the class.

        There's default config that allows us to run connectors natively (see _force_allow_native flag),
        when final configuration is reported these defaults will be merged with defaults from Connectors
        Service config and specific config coming from Agent.
        """
        self._default_config = {
            "service": {
                "log_level": "INFO",
                "_use_native_connector_api_keys": False,
            },
            "_force_allow_native": True,
            "native_service_types": [
                "azure_blob_storage",
                "box",
                "confluence",
                "dropbox",
                "github",
                "gmail",
                "google_cloud_storage",
                "google_drive",
                "jira",
                "mongodb",
                "mssql",
                "mysql",
                "notion",
                "onedrive",
                "oracle",
                "outlook",
                "network_drive",
                "postgresql",
                "s3",
                "salesforce",
                "servicenow",
                "sharepoint_online",
                "slack",
                "microsoft_teams",
                "zoom",
            ],
        }

        self.specific_config = {}

    def try_update(self, unit):
        """Try update the configuration and see if it changed.

        This method takes the check-in event coming from Agent and checks if config needs an update.

        If update is needed, configuration is updated and method returns True. If no update is needed
        the method returns False.
        """

        source = unit.config.source

        # TODO: find a good link to what this object is.
        has_hosts = source.fields.get("hosts")
        has_api_key = source.fields.get("api_key")
        has_basic_auth = source.fields.get("username") and source.fields.get("password")

        assumed_configuration = {}

        # Log-related
        assumed_configuration["service"] = {}
        assumed_configuration["service"]["log_level"] = unit.log_level

        # Auth-related
        if has_hosts and (has_api_key or has_basic_auth):
            es_creds = {"host": source["hosts"][0]}

            if source.fields.get("api_key"):
                logger.debug("Found api_key")
                api_key = source["api_key"]
                # if beats_logstash_format we need to base64 the key
                if ":" in api_key:
                    api_key = base64.b64encode(api_key.encode()).decode()

                es_creds["api_key"] = api_key
            elif source.fields.get("username") and source.fields.get("password"):
                logger.debug("Found username and passowrd")
                es_creds["username"] = source["username"]
                es_creds["password"] = source["password"]
            else:
                msg = "Invalid Elasticsearch credentials"
                raise ValueError(msg)

            assumed_configuration["elasticsearch"] = es_creds

        if self.config_changed(assumed_configuration):
            logger.debug("Changes detected for connectors-relevant configurations")
            # This is a partial update.
            # Agent can send different data in updates.
            # For example, updating only log_level will not send credentials.
            # Thus we don't overwrite configuration, we only update fields that
            # were received
            self.specific_config.update(assumed_configuration)
            return True

        logger.debug("No changes detected for connectors-relevant configurations")
        return False

    def config_changed(self, new_config):
        """See if configuration passed in new_config will update currently stored configuration

        This method takes the new configuration received from the agent and see if there are any changes
        to existing configuration.

        If new_config contains new values for relevant fields, then True is returned, otherwise it returns False.
        """
        # TODO: For now manually check, need to think of a better way?
        # Not super proud of this function, but hey it's tested
        logger.debug("Checking if config changed")
        current_config = self._default_config.copy()
        current_config.update(self.specific_config)

        def _log_level_changed():
            new_config_log_level = nested_get_from_dict(
                new_config, ("service", "log_level")
            )
            current_config_log_level = nested_get_from_dict(
                current_config, ("service", "log_level")
            )

            if new_config_log_level is None:
                return False

            return current_config_log_level != new_config_log_level

        def _elasticsearch_config_changed():
            return current_config.get("elasticsearch") != new_config.get(
                "elasticsearch"
            )

        if _log_level_changed():
            logger.debug("log_level changed")
            return True

        if _elasticsearch_config_changed():
            logger.debug("elasticsearch changed")
            return True

        return False

    def get(self):
        """Get current Connectors Service configuration.

        This method combines three configs with higher ones taking precedence:
        - Config reported from Agent
        - Default config stored in this class
        - Default config of Connectors Service

        Resulting config should be sufficient to run Connectors Service with.
        """
        # First take "default config"
        config = self._default_config.copy()
        # Then merge with what we get from Agent
        config = dict(add_defaults(self.specific_config, default_config=config))
        # Then merge with default connectors config
        configuration = dict(add_defaults(config))

        return configuration
