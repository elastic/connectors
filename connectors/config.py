#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from envyaml import EnvYAML

from connectors.logger import logger

config = None


def load_config(config_file):
    global config
    logger.info(
        f"{'Loading' if config is None else 'Reloading'} config from {config_file}"
    )
    config = EnvYAML(config_file)
    return config
