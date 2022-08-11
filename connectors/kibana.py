#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import sys
import os
import asyncio
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser

from envyaml import EnvYAML

from connectors.byoei import ElasticServer
from connectors.logger import logger
from connectors.source import get_source_klass


CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"
DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config.yml")


# XXX simulating Kibana click-arounds
async def prepare(service_type, index_name, config):
    klass = get_source_klass(config["sources"][service_type])
    es = ElasticServer(config["elasticsearch"])

    try:
        # https:#github.com/elastic/enterprise-search-team/discussions/2153#discussioncomment-2999765
        doc = {
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
            # Last sync
            "last_sync_status": "null",
            "last_sync_error": "",
            "last_synced": "",
            # Written by connector on each operation,
            # used by Kibana to hint to user about status of connector
            "last_seen": "",
            # Date the connector was created
            "created_at": "",
            # Date the connector was updated
            "updated_at": "",
            # Scheduling intervals
            "scheduling": {"enabled": True, "interval": "1 * * * * *"},  # quartz syntax
            # A flag to run sync immediately
            "sync_now": True,
        }

        print(f"Prepare {CONNECTORS_INDEX}")
        await es.prepare_index(CONNECTORS_INDEX, [doc], delete_first=True)

        print(f"Prepare {JOBS_INDEX}")
        await es.prepare_index(JOBS_INDEX, [], delete_first=True)

        print(f"Delete {index_name}")
        if await es.client.indices.exists(index=index_name):
            await es.client.indices.delete(index=index_name)
        print("Done")
    finally:
        await es.close()


def _parser():
    parser = ArgumentParser(
        prog="connector-config", formatter_class=ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--config-file", type=str, help="Configuration file", default=DEFAULT_CONFIG
    )
    parser.add_argument(
        "--service-type", type=str, help="Service type", default="mongo"
    )
    parser.add_argument(
        "--index-name", type=str, help="Elasticsearch index", default="search-mongo"
    )
    return parser


def main(args=None):
    parser = _parser()
    args = parser.parse_args(args=args)
    config_file = args.config_file

    if not os.path.exists(config_file):
        raise IOError(f"{config_file} does not exist")

    config = EnvYAML(config_file)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(prepare(args.service_type, args.index_name, config))
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Bye")

    return 0


if __name__ == "__main__":
    sys.exit(main())
