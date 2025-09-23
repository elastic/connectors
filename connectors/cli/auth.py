#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
from typing import Optional

import yaml
from elasticsearch import ApiError

from connectors.es.cli_client import CLIClient

CONFIG_FILE_PATH = ".cli/config.yml"


class Auth:
    def __init__(
        self,
        host: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> None:
        elastic_config = {
            "host": host,
            "username": username,
            "password": password,
            "api_key": api_key,
        }

        # remove empty values
        self.elastic_config = {k: v for k, v in elastic_config.items() if v is not None}

        self.cli_client = CLIClient(self.elastic_config)

    def authenticate(self) -> bool:
        if asyncio.run(self.__ping_es_client()):
            self.__save_config()
            return True
        else:
            return False

    def is_config_present(self) -> bool:
        return os.path.isfile(CONFIG_FILE_PATH)

    async def __ping_es_client(self):
        try:
            return await self.cli_client.ping()
        except ApiError:
            return False
        finally:
            await self.cli_client.close()

    def __save_config(self):
        yaml_content = yaml.dump({"elasticsearch": self.elastic_config})
        os.makedirs(os.path.dirname(CONFIG_FILE_PATH), exist_ok=True)

        with open(CONFIG_FILE_PATH, "w") as f:
            f.write(yaml_content)
