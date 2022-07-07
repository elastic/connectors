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
import time

import yaml

from connectors.elastic import ElasticServer
from connectors.byoc import BYOConnectors
from connectors.logger import logger
from connectors.registry import get_data_providers, get_data_provider


IDLING = 10
ERRORS = [0, time.time()]


def raise_if_spurious(exception):
    errors, first = ERRORS
    errors += 1

    # if we piled up too many errors we raise and quit
    if errors > 20:
        raise exception

    # we re-init every ten minutes
    if time.time() - first > 600:
        first = time.time()
        errors = 0

    ERRORS[0] = errors
    ERRORS[1] = first


async def poll(config):
    """Main event loop."""
    loop = asyncio.get_event_loop()
    es = ElasticServer(config["elasticsearch"])
    connectors = BYOConnectors(config["elasticsearch"])
    try:
        while True:
            logger.debug("Polling...")
            async for connector in connectors.get_list():
                try:
                    data_provider = get_data_provider(connector, config)
                    loop.create_task(connector.heartbeat())
                    await connector.sync(data_provider, es, IDLING)
                except Exception as e:
                    logger.critical(e, exc_info=True)
                    raise_if_spurious(e)

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
