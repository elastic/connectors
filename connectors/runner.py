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

from envyaml import EnvYAML

from connectors.byoei import ElasticServer
from connectors.byoc import BYOIndex
from connectors.logger import logger
from connectors.source import get_data_sources, get_data_source


class ConnectorService:
    def __init__(self, config_file):
        self.config_file = config_file
        self.errors = [0, time.time()]
        if not os.path.exists(config_file):
            raise IOError(f"{config_file} does not exist")
        self.config = EnvYAML(config_file)
        self.service_config = self.config["service"]
        self.idling = self.service_config["idling"]
        self.hb = self.service_config["heartbeat"]
        self.running = False

    def raise_if_spurious(self, exception):
        errors, first = self.errors
        errors += 1

        # if we piled up too many errors we raise and quit
        if errors > self.service_config["max_errors"]:
            raise exception

        # we re-init every ten minutes
        if time.time() - first > self.service_config["max_errors_span"]:
            first = time.time()
            errors = 0

        self.errors[0] = errors
        self.errors[1] = first

    def stop(self):
        self.running = False

    async def poll(self, one_sync=False):
        """Main event loop."""
        loop = asyncio.get_event_loop()
        connectors = BYOIndex(self.config["elasticsearch"])
        es_host = self.config["elasticsearch"]["host"]

        if not (await connectors.ping()):
            logger.critical(f"{es_host} seem down. Bye!")
            await connectors.close()
            return -1

        es = ElasticServer(self.config["elasticsearch"])
        logger.info(f"Service started, listening to events from {es_host}")
        self.running = True
        try:
            while self.running:
                logger.debug(f"Polling every {self.idling} seconds")
                async for connector in connectors.get_list():
                    try:
                        data_source = get_data_source(connector, self.config)
                        loop.create_task(connector.heartbeat(self.hb))
                        await connector.sync(data_source, es, self.idling)
                        await asyncio.sleep(0)

                        if one_sync:
                            self.stop()
                            break
                    except Exception as e:
                        logger.critical(e, exc_info=True)
                        self.raise_if_spurious(e)
                if not one_sync:
                    await asyncio.sleep(self.idling)
        finally:
            await connectors.close()
            await es.close()
        return 0

    async def get_list(self):
        logger.info("Registered connectors:")
        for source in get_data_sources(self.config):
            logger.info(f"- {source.__doc__.strip()}")
        return 0


def run(args):
    """Runner"""
    service = ConnectorService(args.config_file)
    loop = asyncio.get_event_loop()

    if args.action == "list":
        coro = asyncio.ensure_future(service.get_list())
    else:
        coro = asyncio.ensure_future(service.poll(args.one_sync))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, coro.cancel)

    try:
        loop.run_until_complete(coro)
        logger.info("Bye")
        return coro.result()
    except asyncio.CancelledError:
        logger.info("Bye")
        return 0

    return -1
