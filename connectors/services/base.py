#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
from envyaml import EnvYAML
from connectors.logger import logger


class BaseService:
    def __init__(self, args):
        self.args = args
        config_file = args.config_file
        self.config_file = config_file
        if not os.path.exists(config_file):
            raise IOError(f"{config_file} does not exist")
        self.config = EnvYAML(config_file)
        self.ent_search_config()

    def ent_search_config(self):
        if "ENT_SEARCH_CONFIG_PATH" not in os.environ:
            return
        logger.info("Found ENT_SEARCH_CONFIG_PATH, loading ent-search config")
        ent_search_config = EnvYAML(os.environ["ENT_SEARCH_CONFIG_PATH"])
        for field in (
            "elasticsearch.host",
            "elasticsearch.username",
            "elasticsearch.password",
        ):
            sub = field.split(".")[-1]
            if field not in ent_search_config:
                continue
            logger.debug(f"Overriding {field}")
            self.config["elasticsearch"][sub] = ent_search_config[field]

    def stop(self):
        raise NotImplementedError()

    async def run(self):
        raise NotImplementedError()

    def shutdown(self, sig):
        logger.info(f"Caught {sig.name}. Graceful shutdown.")
        self.stop()
