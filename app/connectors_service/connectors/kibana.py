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

from connectors_sdk.logger import set_extra_logger
from connectors.utils import get_source_klass

from connectors.config import load_config
from connectors.es import DEFAULT_LANGUAGE
from connectors.es.management_client import ESManagementClient
from connectors.protocol import ConnectorIndex
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
    connector_index = ConnectorIndex(config["elasticsearch"])

    await es.ensure_ingest_pipeline_exists(
        "search-default-ingestion",
        DEFAULT_PIPELINE["version"],
        DEFAULT_PIPELINE["description"],
        DEFAULT_PIPELINE["processors"],
    )

    try:
        if connector_definition is not None:
            connector_configuration = connector_definition["configuration"]
        else:
            connector_configuration = klass.get_default_configuration()

        connector_name = f"{service_type}-functional-test"

        logger.info(f"Creating '{connector_name}' connector")

        await connector_index.connector_put(
            connector_id=config["connectors"][0]["connector_id"],
            service_type=service_type,
            connector_name=connector_name,
            index_name=index_name,
        )

        logger.info(f"Updating scheduling for '{connector_name}' connector")

        await connector_index.connector_update_scheduling(
            connector_id=config["connectors"][0]["connector_id"],
            full={
                "enabled": True,
                "interval": "1 * * * * ?",  # every minute
            },
        )

        logger.info(f"Updating configuration for '{connector_name}' connector")

        await connector_index.connector_update_configuration(
            connector_id=config["connectors"][0]["connector_id"],
            schema=connector_configuration,
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

    exists = await es.index_exists(index)

    if exists:
        logger.debug(f"{index} exists, deleting...")
        logger.debug("Deleting it first")
        await es.delete_indices([index])

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
