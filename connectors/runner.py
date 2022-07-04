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
from connectors.byoc import BYOConnectors
from connectors.logger import logger
from connectors.registry import get_data_provider, get_data_providers

IDLING = 10


async def poll(config):
    """Main event loop."""
    es = ElasticServer(config["elasticsearch"])
    connectors = BYOConnectors(config["elasticsearch"])
    try:
        while True:
            logger.debug("poll")
            async for connector in connectors.get_list():

                service_type = connector.service_type
                logger.debug(f"Syncing '{service_type}'")
                next_sync = connector.next_sync()
                if next_sync == -1 or next_sync - IDLING > 0:
                    logger.debug(f"Next sync due in {next_sync} seconds")
                    continue

                await connector.sync_starts()

                data_provider = get_data_provider(connector, config)
                index_name = connector.index_name

                await data_provider.ping()
                await es.prepare_index(index_name)

                existing_ids = [
                    doc_id async for doc_id in es.get_existing_ids(index_name)
                ]
                result = await es.async_bulk(index_name, data_provider.get_docs())
                logger.info(result)
                await connector.sync_done()

            await asyncio.sleep(IDLING)
    finally:
        await es.close()


def run(args):
    """Runner"""
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
