from connectors.config import add_defaults


class ConnectorsAgentConfiguration:
    def __init__(self):
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

    def update(self, new_config):
        self.specific_config = new_config

    def get(self):
        # First take "default config"
        config = self._default_config.copy()
        # Then override with what we get from Agent
        config.update(self.specific_config)

        # Then merge with default connectors config
        configuration = dict(add_defaults(config))

        return configuration
