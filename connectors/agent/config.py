#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.config import add_defaults


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

    def try_update(self, source):
        """Try update the configuration and see if it changed.

        This method takes the check-in event coming from Agent and checks if config needs an update.

        If update is needed, configuration is updated and method returns True. If no update is needed
        the method returns False.
        """

        # TODO: find a good link to what this object is.
        has_hosts = source.fields.get("hosts")
        has_api_key = source.fields.get("api_key")
        has_basic_auth = source.fields.get("username") and source.fields.get("password")
        if has_hosts and (has_api_key or has_basic_auth):
            es_creds = {
                "host": source["hosts"][0],
            }

            if source.fields.get("api_key"):
                es_creds["api_key"] = source["api_key"]
            elif source.fields.get("username") and source.fields.get("password"):
                es_creds["username"] = source["username"]
                es_creds["password"] = source["password"]
            else:
                msg = "Invalid Elasticsearch credentials"
                raise ValueError(msg)

            new_config = {
                "elasticsearch": es_creds,
            }
            self.specific_config = new_config
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
        # Then override with what we get from Agent
        config.update(self.specific_config)
        # Then merge with default connectors config
        configuration = dict(add_defaults(config))

        return configuration
