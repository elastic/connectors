#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#


from connectors_sdk.logger import logger
from envyaml import EnvYAML

# --- Elasticsearch connection defaults -----------------------------------------------
DEFAULT_ELASTICSEARCH_HOST = "http://localhost:9200"
DEFAULT_ELASTICSEARCH_REQUEST_TIMEOUT = 240
DEFAULT_ELASTICSEARCH_MAX_WAIT_DURATION = 240
DEFAULT_ELASTICSEARCH_RETRY_ON_TIMEOUT = True
DEFAULT_ELASTICSEARCH_MAX_RETRIES = 5
DEFAULT_ELASTICSEARCH_RETRY_INTERVAL = 10
DEFAULT_ELASTICSEARCH_INITIAL_BACKOFF_DURATION = 1
DEFAULT_ELASTICSEARCH_BACKOFF_MULTIPLIER = 2

# --- Elasticsearch bulk-ingest defaults ----------------------------------------------
DEFAULT_QUEUE_MAX_SIZE = 1024
DEFAULT_QUEUE_MAX_MEM_SIZE = 25
DEFAULT_QUEUE_REFRESH_INTERVAL = 1
# Sized for the default `elasticsearch.request_timeout` (240s) and the default
# `elasticsearch.max_retries` / `elasticsearch.retry_interval` (5 attempts with linear
# backoff): 5 × 240 + (10 + 20 + 30 + 40) = 1300s. If any of those defaults change,
# this value must change too -- otherwise the mem-queue circuit breaker can fire
# before the retry layer has exhausted its budget.
DEFAULT_QUEUE_REFRESH_TIMEOUT = 1300
DEFAULT_CHUNK_SIZE = 500
DEFAULT_CHUNK_MAX_MEM_SIZE = 3
DEFAULT_MAX_TEXT_DOCUMENT_SIZE = 3
DEFAULT_DISPLAY_EVERY = 100
DEFAULT_MAX_CONCURRENCY = 5
DEFAULT_CONCURRENT_DOWNLOADS = 10

# --- File handling defaults ----------------------------------------------------------
DEFAULT_MAX_FILE_SIZE = 8388608  # 8MiB


def load_config(config_file):
    logger.info(f"Loading config from {config_file}")
    yaml_config = EnvYAML(config_file, flatten=False).export()
    nested_yaml_config = {}
    for key, value in yaml_config.items():
        _nest_configs(nested_yaml_config, key, value)
    configuration = dict(_merge_dicts(_default_config(), nested_yaml_config))

    return configuration


def add_defaults(config, default_config=None):
    if default_config is None:
        default_config = _default_config()
    configuration = dict(_merge_dicts(default_config, config))
    return configuration


def _default_config():
    return {
        "elasticsearch": {
            "host": DEFAULT_ELASTICSEARCH_HOST,
            "username": "elastic",
            "password": "changeme",
            "ssl": True,
            "verify_certs": True,
            "bulk": {
                "queue_max_size": DEFAULT_QUEUE_MAX_SIZE,
                "queue_max_mem_size": DEFAULT_QUEUE_MAX_MEM_SIZE,
                "queue_refresh_interval": DEFAULT_QUEUE_REFRESH_INTERVAL,
                "queue_refresh_timeout": DEFAULT_QUEUE_REFRESH_TIMEOUT,
                "display_every": DEFAULT_DISPLAY_EVERY,
                "chunk_size": DEFAULT_CHUNK_SIZE,
                "max_concurrency": DEFAULT_MAX_CONCURRENCY,
                "chunk_max_mem_size": DEFAULT_CHUNK_MAX_MEM_SIZE,
                "max_text_document_size": DEFAULT_MAX_TEXT_DOCUMENT_SIZE,
                "max_retries": DEFAULT_ELASTICSEARCH_MAX_RETRIES,
                "retry_interval": DEFAULT_ELASTICSEARCH_RETRY_INTERVAL,
                "concurrent_downloads": DEFAULT_CONCURRENT_DOWNLOADS,
                "enable_operations_logging": False,
                "error_monitor": {
                    "enabled": True,
                    "max_total_errors": 1000,
                    "max_consecutive_errors": 10,
                    "max_error_rate": 0.15,
                    "error_window_size": 100,
                    "error_queue_size": 10,
                },
            },
            "max_retries": DEFAULT_ELASTICSEARCH_MAX_RETRIES,
            "retry_interval": DEFAULT_ELASTICSEARCH_RETRY_INTERVAL,
            "retry_on_timeout": DEFAULT_ELASTICSEARCH_RETRY_ON_TIMEOUT,
            "request_timeout": DEFAULT_ELASTICSEARCH_REQUEST_TIMEOUT,
            "max_wait_duration": DEFAULT_ELASTICSEARCH_MAX_WAIT_DURATION,
            "initial_backoff_duration": DEFAULT_ELASTICSEARCH_INITIAL_BACKOFF_DURATION,
            "backoff_multiplier": DEFAULT_ELASTICSEARCH_BACKOFF_MULTIPLIER,
            "log_level": "info",
            "feature_use_connectors_api": True,
        },
        "service": {
            "worker_mode": False,
            "service_types": [],
            "idling": 30,
            "heartbeat": 300,
            "preflight_max_attempts": 10,
            "preflight_idle": 30,
            "max_errors": 20,
            "max_errors_span": 600,
            "max_concurrent_content_syncs": 1,
            "max_concurrent_access_control_syncs": 1,
            "max_concurrent_scheduling_tasks": 4,
            "max_file_download_size": DEFAULT_MAX_FILE_SIZE,
            "job_cleanup_interval": 300,
            "log_level": "INFO",
            "fips_mode": False,
        },
        "sources": {
            "azure_blob_storage": "connectors.sources.azure_blob_storage:AzureBlobStorageDataSource",
            "box": "connectors.sources.box:BoxDataSource",
            "confluence": "connectors.sources.atlassian.confluence:ConfluenceDataSource",
            "dir": "connectors.sources.directory:DirectoryDataSource",
            "dropbox": "connectors.sources.dropbox:DropboxDataSource",
            "github": "connectors.sources.github:GitHubDataSource",
            "gitlab": "connectors.sources.gitlab:GitLabDataSource",
            "gmail": "connectors.sources.gmail:GMailDataSource",
            "google_cloud_storage": "connectors.sources.google_cloud_storage:GoogleCloudStorageDataSource",
            "google_drive": "connectors.sources.google_drive:GoogleDriveDataSource",
            "graphql": "connectors.sources.graphql:GraphQLDataSource",
            "jira": "connectors.sources.atlassian.jira:JiraDataSource",
            "microsoft_teams": "connectors.sources.microsoft_teams:MicrosoftTeamsDataSource",
            "mongodb": "connectors.sources.mongo:MongoDataSource",
            "mssql": "connectors.sources.mssql:MSSQLDataSource",
            "mysql": "connectors.sources.mysql:MySqlDataSource",
            "network_drive": "connectors.sources.network_drive:NASDataSource",
            "notion": "connectors.sources.notion:NotionDataSource",
            "onedrive": "connectors.sources.onedrive:OneDriveDataSource",
            "oracle": "connectors.sources.oracle:OracleDataSource",
            "outlook": "connectors.sources.outlook:OutlookDataSource",
            "postgresql": "connectors.sources.postgresql:PostgreSQLDataSource",
            "redis": "connectors.sources.redis:RedisDataSource",
            "s3": "connectors.sources.s3:S3DataSource",
            "salesforce": "connectors.sources.salesforce:SalesforceDataSource",
            "sandfly": "connectors.sources.sandfly:SandflyDataSource",
            "servicenow": "connectors.sources.servicenow:ServiceNowDataSource",
            "sharepoint_online": "connectors.sources.sharepoint.sharepoint_online:SharepointOnlineDataSource",
            "sharepoint_server": "connectors.sources.sharepoint.sharepoint_server:SharepointServerDataSource",
            "slack": "connectors.sources.slack:SlackDataSource",
            "zoom": "connectors.sources.zoom:ZoomDataSource",
        },
    }


def _nest_configs(configuration, field, value):
    """
    Update configuration field value taking into account the nesting.

    Configuration is a hash of hashes, so we need to dive inside to do proper assignment.

    E.g. _nest_config({}, "elasticsearch.bulk.queuesize", 20) will result in the following config:
    {
        "elasticsearch": {
            "bulk": {
                "queuesize": 20
            }
        }
    }
    """
    subfields = field.split(".")
    last_key = subfields[-1]

    current_leaf = configuration
    for subfield in subfields[:-1]:
        if subfield not in current_leaf:
            current_leaf[subfield] = {}
        current_leaf = current_leaf[subfield]

    if isinstance(current_leaf.get(last_key), dict):
        current_leaf[last_key] = dict(_merge_dicts(current_leaf[last_key], value))
    else:
        current_leaf[last_key] = value


def _merge_dicts(hsh1, hsh2):
    for k in set(hsh1.keys()).union(hsh2.keys()):
        if k in hsh1 and k in hsh2:
            if isinstance(hsh1[k], dict) and isinstance(
                hsh2[k], dict
            ):  # only merge objects
                yield (k, dict(_merge_dicts(hsh1[k], hsh2[k])))
            else:
                yield (k, hsh2[k])
        elif k in hsh1:
            yield (k, hsh1[k])
        else:
            yield (k, hsh2[k])
