#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os

from envyaml import EnvYAML

from connectors.logger import logger

config = None


def load_config(config_file):
    global config
    logger.info(
        f"{'Loading' if config is None else 'Reloading'} config from {config_file}"
    )
    config = EnvYAML(config_file)
    _ent_search_config()
    return config


def _ent_search_config():
    if "ENT_SEARCH_CONFIG_PATH" not in os.environ:
        return
    global config
    logger.info("Found ENT_SEARCH_CONFIG_PATH, loading ent-search config")
    ent_search_config = EnvYAML(os.environ["ENT_SEARCH_CONFIG_PATH"])
    for field in (
        "elasticsearch.host",
        "elasticsearch.username",
        "elasticsearch.password",
        "elasticsearch.headers",
    ):
        sub = field.split(".")[-1]
        if field not in ent_search_config:
            continue
        logger.debug(f"Overriding {field}")
        config["elasticsearch"][sub] = ent_search_config[field]
