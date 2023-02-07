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

import elasticsearch
from envyaml import EnvYAML

from connectors.byoei import ElasticServer
from connectors.es.settings import DEFAULT_LANGUAGE, Mappings, Settings
from connectors.logger import logger, set_logger
from connectors.source import get_source_klass
from connectors.utils import validate_index_name

CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"
DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config.yml")
DEFAULT_CONNECTOR_CONFIG = {
    "api_key_id": "",
    "status": "configured",
    "language": "en",
    "last_sync_status": "null",
    "last_sync_error": "",
    "last_synced": "",
    "last_seen": "",
    "created_at": "",
    "updated_at": "",
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
    "scheduling": {"enabled": True, "interval": "1 * * * * *"},
    "pipeline": {
        "extract_binary_content": True,
        "name": "ent-search-generic-ingestion",
        "reduce_whitespace": True,
        "run_ml_inference": True,
    },
    "sync_now": True,
    "is_native": True,
}
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


# XXX simulating Kibana click-arounds
async def prepare(service_type, index_name, config, connector_config):
    klass = get_source_klass(config["sources"][service_type])
    es = ElasticServer(config["elasticsearch"])

    # add a dummy pipeline
    try:
        pipeline = await es.client.ingest.get_pipeline(
            id="ent-search-generic-ingestion"
        )
    except elasticsearch.NotFoundError:
        await es.client.ingest.put_pipeline(
            id="ent-search-generic-ingestion", body=DEFAULT_PIPELINE
        )

    try:
        pipeline = await es.client.ingest.get_pipeline(
            id="ent-search-generic-ingestion"
        )
    except elasticsearch.NotFoundError:
        pipeline = {
            "description": "My optional pipeline description",
            "processors": [
                {
                    "set": {
                        "description": "My optional processor description",
                        "field": "my-keyword-field",
                        "value": "foo",
                    }
                }
            ],
        }

        await es.client.ingest.put_pipeline(
            id="ent-search-generic-ingestion", body=pipeline
        )

    try:
        dynamic_fields = {
            "configuration": klass.get_default_configuration(),
            "index_name": index_name,
            "service_type": service_type,
        }

        connector_doc = connector_config | dynamic_fields

        logger.info(f"Prepare {CONNECTORS_INDEX}")
        await upsert_index(es, CONNECTORS_INDEX, docs=[connector_doc])

        logger.info(f"Prepare {JOBS_INDEX}")
        await upsert_index(es, JOBS_INDEX, docs=[])

        logger.info(f"Prepare {index_name}")
        mappings = Mappings.default_text_fields_mappings(
            is_connectors_index=True,
        )
        settings = Settings(
            language_code=DEFAULT_LANGUAGE, analysis_icu=False
        ).to_hash()
        await upsert_index(es, index_name, mappings=mappings, settings=settings)
        logger.info("Done")
    finally:
        await es.close()


async def sync_now():
    logger.info("Sync now")


async def upsert_index(es, index, docs=None, mappings=None, settings=None):
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

    exists = await es.client.indices.exists(
        index=index, expand_wildcards=expand_wildcards
    )

    if exists:
        logger.debug(f"{index} exists, deleting...")
        logger.debug("Deleting it first")
        await es.client.indices.delete(index=index, expand_wildcards=expand_wildcards)

    logger.debug(f"Creating index {index}")
    await es.client.indices.create(index=index, mappings=mappings, settings=settings)

    if docs is None:
        return
    # XXX bulk
    doc_id = 1
    for doc in docs:
        await es.client.index(index=index, id=doc_id, document=doc)
        doc_id += 1


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
    parser.add_argument(
        "--connector-config",
        type=nullable,
        help="Path to a json file containing the state of the connector (represents a document in the .elastic-connectors index)",
    )

    return parser


def nullable(value):
    if not value:
        return None
    return value


def _load_connector_config(connector_config_file_path):
    if not connector_config_file_path or not os.path.exists(connector_config_file_path):
        connector_config_file = (
            ("at" + connector_config_file_path) if connector_config_file_path else ""
        )

        logger.warn(
            f"connector config file {connector_config_file} does not exist. Fallback to default connector config."
        )

        connector_config = DEFAULT_CONNECTOR_CONFIG
    else:
        connector_config_file = open(
            os.path.join(
                os.path.dirname(__file__),
                "sources",
                "tests",
                "fixtures",
                connector_config_file_path,
            )
        )
        connector_config = json.load(connector_config_file)
        logger.info(
            f"Successfully loaded connector config file from '{connector_config_file_path}'."
        )

    return connector_config


def main(args=None):
    parser = _parser()
    args = parser.parse_args(args=args)
    config_file = args.config_file
    connector_config_file = args.connector_config

    if not os.path.exists(config_file):
        raise IOError(f"config file at '{config_file}' does not exist")

    connector_config = _load_connector_config(connector_config_file)

    set_logger(args.debug and logging.DEBUG or logging.INFO)
    config = EnvYAML(config_file)
    try:
        asyncio.run(
            prepare(args.service_type, args.index_name, config, connector_config)
        )
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Bye")

    return 0


if __name__ == "__main__":
    sys.exit(main())
