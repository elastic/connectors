#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import asyncio

import yaml

from connectors.byoei import ElasticServer
from connectors.logger import logger
from connectors.sources.mongo import MongoDataSource


CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"

# XXX simulating Kibana click-arounds
async def prepare(config):
    es = ElasticServer(config["elasticsearch"])

    # https:#github.com/elastic/enterprise-search-team/discussions/2153#discussioncomment-2999765
    doc = {
        # Used by the frontend to manage the api key
        # associated with the connector
        "api_key_id": "",

        # Configurations, e.g. API key
        "configuration": MongoDataSource.get_default_configuration(),

        # Name of the index the documents will be written to.
        # Set by Kibana, *not* the connector.
        "index_name": "search-airbnb",

        # used to surface copy and icons in the front end
        "service_type": "mongo",

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
        "scheduling": {
            "enabled": True,
            "interval": "* * * * *"
        },  # crontab syntax

        # A flag to run sync immediately
        "sync_now": True,
    }

    print(f"Prepare {CONNECTORS_INDEX}")
    await es.prepare_index(CONNECTORS_INDEX, [doc], delete_first=True)

    print(f"Prepare {JOBS_INDEX}")
    await es.prepare_index(JOBS_INDEX, [], delete_first=True)

    print(f"Delete search-airbnb")
    if await es.client.indices.exists(index='search-airbnb'):
        await es.client.indices.delete(index='search-airbnb')
    print("Done")
    await es.close()


config_file = os.path.join(os.path.dirname(__file__), "..", "config.yml")

if not os.path.exists(config_file):
    raise IOError(f"{config_file} does not exist")

with open(config_file) as f:
    config = yaml.safe_load(f)


loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(prepare(config))
except (asyncio.CancelledError, KeyboardInterrupt):
    logger.info("Bye")
