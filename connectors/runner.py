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

from connectors.byoei import ElasticServer
from connectors.byoc import BYOIndex
from connectors.logger import logger
from connectors.source import get_data_sources, get_data_source


IDLING = 10


class ConnectorService:
    def __init__(self, config_file):
        self.config_file = config_file
        self.errors = [0, time.time()]
        if not os.path.exists(config_file):
            raise IOError(f"{config_file} does not exist")
        with open(config_file) as f:
            self.config = yaml.safe_load(f)

    def raise_if_spurious(self, exception):
        errors, first = self.errors
        errors += 1

        # if we piled up too many errors we raise and quit
        if errors > 20:
            raise exception

        # we re-init every ten minutes
        if time.time() - first > 600:
            first = time.time()
            errors = 0

        self.errors[0] = errors
        self.errors[1] = first

    async def poll(self):
        """Main event loop."""
        loop = asyncio.get_event_loop()
        es = ElasticServer(self.config["elasticsearch"])
        connectors = BYOIndex(self.config["elasticsearch"])
        try:
            while True:
                logger.debug("Polling...")
                async for connector in connectors.get_list():
                    try:
                        data_source = get_data_source(connector, self.config)
                        loop.create_task(connector.heartbeat())
                        await connector.sync(data_source, es, IDLING)
                    except Exception as e:
                        logger.critical(e, exc_info=True)
                        self.raise_if_spurious(e)

                await asyncio.sleep(IDLING)
        finally:
            await es.close()

    async def get_list(self):
        logger.info("Registered connectors:")
        for source in get_data_sources(self.config):
            logger.info(f"- {source.__doc__.strip()}")


def run(args):
    """Runner"""
    service = ConnectorService(args.config_file)

    loop = asyncio.get_event_loop()
    if args.action == "list":
        coro = asyncio.ensure_future(service.get_list())
    else:
        coro = asyncio.ensure_future(service.poll())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, coro.cancel)

    try:
        loop.run_until_complete(coro)
    except asyncio.CancelledError:
        logger.info("Bye")

    return 0
