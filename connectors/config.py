#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os

from envyaml import EnvYAML

from connectors.logger import logger


def load_config(config_file):
    logger.info(f"Loading config from {config_file}")
    configuration = EnvYAML(config_file)
    _ent_search_config(configuration)
    return configuration


# Left - in Enterprise Search; Right - in Connectors
config_mappings = {
    "elasticsearch.host": "elasticsearch.host",
    "elasticsearch.username": "elasticsearch.username",
    "elasticsearch.password": "elasticsearch.password",
    "elasticsearch.headers": "elasticsearch.headers",
    "log_level": "service.log_level",
}

# Enterprise Search uses Ruby and is in lower case always, so hacking it here for now
# Ruby-supported log levels: 'debug', 'info', 'warn', 'error', 'fatal', 'unknown'
# Python-supported log levels: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'NOTSET'
log_level_mappings = {
    "debug": "DEBUG",
    "info": "INFO",
    "warn": "WARNING",
    "error": "ERROR",
    "fatal": "CRITICAL",
    "unknown": "NOTSET",
}


def _ent_search_config(configuration):
    if "ENT_SEARCH_CONFIG_PATH" not in os.environ:
        return
    logger.info("Found ENT_SEARCH_CONFIG_PATH, loading ent-search config")
    ent_search_config = EnvYAML(os.environ["ENT_SEARCH_CONFIG_PATH"])
    for es_field in config_mappings.keys():
        if es_field not in ent_search_config:
            continue

        connector_field = config_mappings[es_field]
        es_field_value = ent_search_config[es_field]

        if es_field == "log_level":
            if es_field_value not in log_level_mappings:
                raise ValueError(
                    f"Unexpected log level: {es_field_value}. Allowed values: {', '.join(log_level_mappings.keys())}"
                )
            es_field_value = log_level_mappings[es_field_value]

        _update_config_field(configuration, connector_field, es_field_value)

        logger.debug(f"Overridden {connector_field}")


def _update_config_field(configuration, field, value):
    """
    Update configuration field value taking into account the nesting.

    Configuration is a hash of hashes, so we need to dive inside to do proper assignment.

    E.g. _update_config({}, "elasticsearch.bulk.queuesize", 20) will result in the following config:
    {
        "elasticsearch": {
            "bulk": {
                "queuesize": 20
            }
        }
    }
    """
    subfields = field.split(".")

    current_leaf = configuration
    for subfield in subfields[:-1]:
        if subfield not in current_leaf:
            current_leaf[subfield] = {}
        current_leaf = current_leaf[subfield]

    current_leaf[subfields[-1]] = value
