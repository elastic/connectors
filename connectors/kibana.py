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

from connectors.config import load_config
from connectors.es.settings import DEFAULT_LANGUAGE, Mappings, Settings
from connectors.es.sink import SyncOrchestrator
from connectors.logger import logger, set_logger
from connectors.source import get_source_klass
from connectors.utils import validate_index_name

CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"
DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config.yml")
DEFAULT_FILTERING = [
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
]
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

# This should be updated when ftest starts to hate on it for any reason
# Later we won't need it at all
JOB_INDEX_MAPPINGS = {
    "dynamic": "false",
    "_meta": {"version": 1},
    "properties": {
        "cancelation_requested_at": {"type": "date"},
        "canceled_at": {"type": "date"},
        "completed_at": {"type": "date"},
        "connector": {
            "properties": {
                "configuration": {"type": "object"},
                "filtering": {
                    "properties": {
                        "advanced_snippet": {
                            "properties": {
                                "created_at": {"type": "date"},
                                "updated_at": {"type": "date"},
                                "value": {"type": "object"},
                            }
                        },
                        "domain": {"type": "keyword"},
                        "rules": {
                            "properties": {
                                "created_at": {"type": "date"},
                                "field": {"type": "keyword"},
                                "id": {"type": "keyword"},
                                "order": {"type": "short"},
                                "policy": {"type": "keyword"},
                                "rule": {"type": "keyword"},
                                "updated_at": {"type": "date"},
                                "value": {"type": "keyword"},
                            }
                        },
                        "warnings": {
                            "properties": {
                                "ids": {"type": "keyword"},
                                "messages": {"type": "text"},
                            }
                        },
                    }
                },
                "id": {"type": "keyword"},
                "index_name": {"type": "keyword"},
                "language": {"type": "keyword"},
                "pipeline": {
                    "properties": {
                        "extract_binary_content": {"type": "boolean"},
                        "name": {"type": "keyword"},
                        "reduce_whitespace": {"type": "boolean"},
                        "run_ml_inference": {"type": "boolean"},
                    }
                },
                "service_type": {"type": "keyword"},
            }
        },
        "created_at": {"type": "date"},
        "deleted_document_count": {"type": "integer"},
        "error": {"type": "keyword"},
        "indexed_document_count": {"type": "integer"},
        "indexed_document_volume": {"type": "integer"},
        "last_seen": {"type": "date"},
        "metadata": {"type": "object"},
        "started_at": {"type": "date"},
        "status": {"type": "keyword"},
        "total_document_count": {"type": "integer"},
        "trigger_method": {"type": "keyword"},
        "worker_hostname": {"type": "keyword"},
    },
}


async def prepare(service_type, index_name, config, connector_definition=None):
    klass = get_source_klass(config["sources"][service_type])
    es = SyncOrchestrator(config["elasticsearch"])

    # add a dummy pipeline
    try:
        pipeline = await es.client.ingest.get_pipeline(
            id="ent-search-generic-ingestion"
        )
    except elasticsearch.NotFoundError:
        await es.client.ingest.put_pipeline(
            id="ent-search-generic-ingestion",
            version=DEFAULT_PIPELINE["version"],
            description=DEFAULT_PIPELINE["description"],
            processors=DEFAULT_PIPELINE["processors"],
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
            id="ent-search-generic-ingestion",
            description=pipeline["description"],
            processors=pipeline["processors"],
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

        logger.info(f"Prepare {CONNECTORS_INDEX}")
        await upsert_index(
            es,
            CONNECTORS_INDEX,
            docs=[doc],
            doc_ids=[config["connectors"][0]["connector_id"]],
        )

        logger.info(f"Prepare {JOBS_INDEX}")
        await upsert_index(es, JOBS_INDEX, docs=[], mappings=JOB_INDEX_MAPPINGS)

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


async def upsert_index(
    es, index, docs=None, doc_ids=None, mappings=None, settings=None
):
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
    # TODO: bulk

    if doc_ids is None:
        for doc_id, doc in enumerate(docs, start=1):
            await es.client.index(index=index, id=doc_id, document=doc)
    else:
        for doc, doc_id in zip(docs, doc_ids, strict=True):
            await es.client.index(index=index, id=doc_id, document=doc)


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
        raise IOError(f"config file at '{config_file}' does not exist")

    set_logger(args.debug and logging.DEBUG or logging.INFO)
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
