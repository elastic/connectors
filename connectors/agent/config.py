from connectors.config import add_defaults


class ConnectorsAgentConfigurationWrapper:
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

    def try_update(self, source):
        if source.fields.get("hosts") and (
            source.fields.get("api_key")
            or source.fields.get("username")
            and source.fields.get("password")
        ):
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
        # First take "default config"
        config = self._default_config.copy()
        # Then override with what we get from Agent
        config.update(self.specific_config)

        # Then merge with default connectors config
        configuration = dict(add_defaults(config))

        return configuration
