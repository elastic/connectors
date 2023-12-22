#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import json
import logging
import os
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser

from connectors.config import load_config
from connectors.es.client import ESManagementClient
from connectors.es.settings import DEFAULT_LANGUAGE
from connectors.logger import set_extra_logger
from connectors.source import get_source_klass
from connectors.utils import validate_index_name

CONNECTORS_INDEX = ".elastic-connectors-v1"
JOBS_INDEX = ".elastic-connectors-sync-jobs-v1"
DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config.yml")
DEFAULT_PIPELINE = {
    "version": 1,
    "description": "For testing",
    "processors": [
        {
            "remove": {
                "tag": "remove_meta_fields",
                "description": "Remove meta fields",
                "field": [
                    "_attachment",
                    "_attachment_indexed_chars",
                    "_extracted_attachment",
                    "_extract_binary_content",
                    "_reduce_whitespace",
                    "_run_ml_inference",
                ],
                "ignore_missing": True,
            }
        }
    ],
}

logger = logging.getLogger("kibana-fake")
set_extra_logger(logger, log_level=logging.DEBUG, prefix="KBN-FAKE")


async def prepare(service_type, index_name, config, connector_definition=None):
    klass = get_source_klass(config["sources"][service_type])
    es = ESManagementClient(config["elasticsearch"])

    await es.ensure_ingest_pipeline_exists(
        "ent-search-generic-ingestion",
        DEFAULT_PIPELINE["version"],
        DEFAULT_PIPELINE["description"],
        DEFAULT_PIPELINE["processors"],
    )

    try:
        # https:#github.com/elastic/enterprise-search-team/discussions/2153#discussioncomment-2999765
        doc = connector_definition or {
            # Used by the frontend to manage the api key
            # associated with the connector
            "api_key_id": "",
            # Configurations, e.g. API key
            "configuration": klass.get_default_configuration(),
            # Name of the index the documents will be written to.
            # Set by Kibana, *not* the connector.
            "index_name": index_name,
            # used to surface copy and icons in the front end
            "service_type": service_type,
            # Current status of the connector, and the value can be
            "status": "configured",
            "language": "en",
            # Last sync
            "last_access_control_sync_error": None,
            "last_access_control_sync_scheduled_at": None,
            "last_access_control_sync_status": None,
            "last_sync_status": None,
            "last_sync_error": None,
            "last_sync_scheduled_at": None,
            "last_synced": None,
            # Written by connector on each operation,
            # used by Kibana to hint to user about status of connector
            "last_seen": None,
            # Date the connector was created
            "created_at": None,
            # Date the connector was updated
            "updated_at": None,
            "filtering": [
                {
                    "domain": "DEFAULT",
                    "draft": {
                        "advanced_snippet": {
                            "updated_at": "2023-01-31T16:41:27.341Z",
                            "created_at": "2023-01-31T16:38:49.244Z",
                            "value": {},
                        },
                        "rules": [
                            {
                                "field": "_",
                                "updated_at": "2023-01-31T16:41:27.341Z",
                                "created_at": "2023-01-31T16:38:49.244Z",
                                "rule": "regex",
                                "id": "DEFAULT",
                                "value": ".*",
                                "order": 1,
                                "policy": "include",
                            }
                        ],
                        "validation": {"state": "valid", "errors": []},
                    },
                    "active": {
                        "advanced_snippet": {
                            "updated_at": "2023-01-31T16:41:27.341Z",
                            "created_at": "2023-01-31T16:38:49.244Z",
                            "value": {},
                        },
                        "rules": [
                            {
                                "field": "_",
                                "updated_at": "2023-01-31T16:41:27.341Z",
                                "created_at": "2023-01-31T16:38:49.244Z",
                                "rule": "regex",
                                "id": "DEFAULT",
                                "value": ".*",
                                "order": 1,
                                "policy": "include",
                            }
                        ],
                        "validation": {"state": "valid", "errors": []},
                    },
                }
            ],
            # Scheduling intervals
            "scheduling": {
                "full": {
                    "enabled": True,
                    "interval": "1 * * * * *",
                },
            },  # quartz syntax
            "pipeline": {
                "extract_binary_content": True,
                "name": "ent-search-generic-ingestion",
                "reduce_whitespace": True,
                "run_ml_inference": True,
            },
            "sync_cursor": None,
            "is_native": False,
        }

        logger.info(f"Prepare {CONNECTORS_INDEX} document")
        await es.upsert(
            index_name=CONNECTORS_INDEX,
            _id=config["connectors"][0]["connector_id"],
            doc=doc,
        )

        logger.info(f"Prepare {index_name}")
        await upsert_index(es, index_name)
        logger.info("Done")
    finally:
        await es.close()


async def upsert_index(es, index):
    """Override the index with new mappings and settings.

    If the index with such name exists, it's deleted and then created again
    with provided mappings and settings. Otherwise index is just created.

    After that, provided docs are inserted into the index.

    This method is supposed to be used only for testing - framework is not
    supposed to create/delete indices at all, Kibana is responsible for
    this logic.
    """

    if index.startswith("."):
        expand_wildcards = "hidden"
    else:
        expand_wildcards = "open"

    exists = await es.index_exists(index, expand_wildcards)

    if exists:
        logger.debug(f"{index} exists, deleting...")
        logger.debug("Deleting it first")
        await es.delete_indices([index], expand_wildcards)

    logger.debug(f"Creating index {index}")
    await es.create_content_index(index, DEFAULT_LANGUAGE)


def _parser():
    parser = ArgumentParser(
        prog="fake-kibana", formatter_class=ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--config-file", type=str, help="Configuration file", default=DEFAULT_CONFIG
    )
    parser.add_argument(
        "--service-type", type=str, help="Service type", default="mongodb"
    )
    parser.add_argument(
        "--connector-definition",
        type=str,
        help="Path to a json file with connector Elasticsearch entry to use for testing",
    )
    parser.add_argument(
        "--index-name",
        type=validate_index_name,
        help="Elasticsearch index",
        default="search-mongo",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Run the event loop in debug mode.",
    )

    return parser


def main(args=None):
    parser = _parser()
    args = parser.parse_args(args=args)

    connector_definition_file = args.connector_definition
    config_file = args.config_file

    if not os.path.exists(config_file):
        msg = f"config file at '{config_file}' does not exist"
        raise IOError(msg)

    config = load_config(config_file)
    connector_definition = None
    if (
        connector_definition_file
        and os.path.exists(connector_definition_file)
        and os.path.isfile(connector_definition_file)
    ):
        with open(connector_definition_file) as f:
            logger.info(f"Loaded connector definition from {connector_definition_file}")
            connector_definition = json.load(f)
    else:
        logger.info(
            "No connector definition file provided, using default connector definition"
        )

    try:
        asyncio.run(
            prepare(args.service_type, args.index_name, config, connector_definition)
        )
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Bye")

    return 0


if __name__ == "__main__":
    sys.exit(main())
