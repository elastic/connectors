import asyncio
import os

import yaml
from elasticsearch import ApiError

from connectors.es.client import ESManagementClient

CONFIG_FILE_PATH = ".cli/config.yml"


class Auth:
    def __init__(self, host, username, password):
        self.elastic_config = {"host": host, "username": username, "password": password}
        self.es_client = ESManagementClient(self.elastic_config)

    def authenticate(self):
        if asyncio.run(self.__ping_es_client()):
            self.__save_config()
            return True
        else:
            return False

    def is_config_present(self):
        return os.path.isfile(CONFIG_FILE_PATH)

    async def __ping_es_client(self):
        try:
            return await self.es_client.ping()
        except ApiError:
            return False
        finally:
            await self.es_client.close()

    def __save_config(self):
        yaml_content = yaml.dump({"elasticsearch": self.elastic_config})
        os.makedirs(os.path.dirname(CONFIG_FILE_PATH), exist_ok=True)

        with open(CONFIG_FILE_PATH, "w") as f:
            f.write(yaml_content)
