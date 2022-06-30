#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Event loop

- polls for work by calling Elasticsearch on a regular basis
- instanciates connector plugins
- mirrors an Elasticsearch index with a collection of documents
"""
import os
import asyncio

import yaml

from connectors.elastic import ElasticServer
from connectors.logger import logger
from connectors.registry import get_connector_instance, get_connectors

IDLING = 10


async def poll(config):
    """Main event loop.
    """
    es = ElasticServer(config["elasticsearch"])

    while True:
        logger.debug("poll")
        async for definition in es.get_connectors_definitions():
            service_type = definition["service_type"]
            connector = get_connector_instance(definition, config)
            await connector.ping()

            es_index = definition["es_index"]
            await es.prepare_index(es_index)
            existing_ids = [doc_id async for doc_id in es.get_existing_ids(es_index)]
            result = await es.async_bulk(es_index, connector.get_docs())
            logger.info(result)

        await asyncio.sleep(IDLING)


def run(args):
    """Runner
    """
    if not os.path.exists(args.config_file):
        raise IOError(f"{args.config_file} does not exist")

    with open(args.config_file) as f:
        config = yaml.safe_load(f)

    if args.action == "list":
        logger.info("Registered connectors:")
        for connector in get_connectors(config):
            logger.info(f"- {connector.__doc__.strip()}")
        return 0

    logger.info(f"Connecting to {config['elasticsearch']['host']}")
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(poll(config))
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Bye")
    return 0
