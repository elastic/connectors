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
import signal

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
            logger.debug("Polling...")
            async for connector in connectors.get_list():

                service_type = connector.service_type
                logger.debug(f"Syncing '{service_type}'")
                next_sync = connector.next_sync()
                if next_sync == -1 or next_sync - IDLING > 0:
                    logger.debug(f"Next sync due in {next_sync} seconds")
                    continue

                await connector.sync_starts()
                try:
                    data_provider = get_data_provider(connector, config)
                    index_name = connector.index_name

                    await data_provider.ping()
                    await es.prepare_index(index_name)

                    result = await es.async_bulk(index_name, data_provider.get_docs())
                    logger.info(result)
                    await connector.sync_done()
                except Exception as e:
                    await connector.sync_failed(e)
                    raise

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

    loop = asyncio.get_event_loop()
    coro = asyncio.ensure_future(poll(config))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, coro.cancel)

    try:
        loop.run_until_complete(coro)
    except asyncio.CancelledError:
        logger.info("Bye")
    return 0
