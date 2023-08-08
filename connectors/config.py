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
    configuration = dict(_merge_dicts(_default_config(),  EnvYAML(config_file).export()))
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

def _default_config():
    return {
        "elasticsearch": {
            "host": "http://localhost:9200",
            "ssl": True,
            "bulk": {
                "queue_max_size": 1024,
                "queue_max_mem_size": 25,
                "display_every": 100,
                "chunk_size": 1000,
                "max_concurrency": 5,
                "chunk_max_mem_size": 5,
                "concurrent_downloads": 10,
            },
            "request_timeout": 120,
            "max_wait_duration": 120,
            "initial_backoff_duration": 1,
            "backoff_multiplier": 2,
            "log_level": "info",
        },
        "service": {
            "idling": 30,
            "heartbeat": 300,
            "max_errors": 20,
            "max_errors_span": 600,
            "max_concurrent_content_syncs": 1,
            "max_concurrent_access_control_syncs": 1,
            "job_cleanup_interval": 300,
            "log_level": "INFO"
        },
        "sources": {
            "mongodb": "connectors.sources.mongo:MongoDataSource",
            "s3": "connectors.sources.s3:S3DataSource",
            "dir": "connectors.sources.directory:DirectoryDataSource",
            "mysql": "connectors.sources.mysql:MySqlDataSource",
            "network_drive": "connectors.sources.network_drive:NASDataSource",
            "google_cloud_storage": "connectors.sources.google_cloud_storage:GoogleCloudStorageDataSource",
            "google_drive": "connectors.sources.google_drive:GoogleDriveDataSource",
            "azure_blob_storage": "connectors.sources.azure_blob_storage:AzureBlobStorageDataSource",
            "postgresql": "connectors.sources.postgresql:PostgreSQLDataSource",
            "oracle": "connectors.sources.oracle:OracleDataSource",
            "sharepoint_server": "connectors.sources.sharepoint_server:SharepointServerDataSource",
            "mssql": "connectors.sources.mssql:MSSQLDataSource",
            "jira": "connectors.sources.jira:JiraDataSource",
            "confluence": "connectors.sources.confluence:ConfluenceDataSource",
            "dropbox": "connectors.sources.dropbox:DropboxDataSource",
            "servicenow": "connectors.sources.servicenow:ServiceNowDataSource",
            "sharepoint_online": "connectors.sources.sharepoint_online:SharepointOnlineDataSource",
            "github": "connectors.sources.github:GitHubDataSource",
            "slack": "connectors.sources.slack:SlackDataSource",
        }
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

def _merge_dicts(hsh1, hsh2):
    for k in set(hsh1.keys()).union(hsh2.keys()):
        if k in hsh1 and k in hsh2:
            if isinstance(hsh1[k], dict) and isinstance(hsh2[k], dict): # only merge objects
                yield (k, dict(_merge_dicts(hsh1[k], hsh2[k])))
            else:
                yield (k, hsh2[k])
        elif k in hsh1:
            yield (k, hsh1[k])
        else:
            yield (k, hsh2[k])