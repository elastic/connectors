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
from connectors.registry import get_data_providers, get_data_provider


IDLING = 10


async def poll(config):
    """Main event loop."""
    es = ElasticServer(config["elasticsearch"])
    connectors = BYOConnectors(config["elasticsearch"])
    try:
        while True:
            logger.debug("Polling...")
            async for connector in connectors.get_list():
                data_provider = get_data_provider(connector, config)
                await connector.sync(data_provider, es, IDLING)

            await asyncio.sleep(IDLING)
    finally:
        await es.close()


async def get_list(config):
    logger.info("Registered connectors:")
    for provider in get_data_providers(config):
        logger.info(f"- {provider.__doc__.strip()}")


def run(args):
    """Runner"""
    if not os.path.exists(args.config_file):
        raise IOError(f"{args.config_file} does not exist")

    with open(args.config_file) as f:
        config = yaml.safe_load(f)

    loop = asyncio.get_event_loop()
    if args.action == "list":
        coro = asyncio.ensure_future(get_list(config))
    else:
        coro = asyncio.ensure_future(poll(config))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, coro.cancel)

    try:
        loop.run_until_complete(coro)
    except asyncio.CancelledError:
        logger.info("Bye")
    return 0
