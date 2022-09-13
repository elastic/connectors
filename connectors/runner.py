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
import functools

from envyaml import EnvYAML

from connectors.byoei import ElasticServer
from connectors.byoc import BYOIndex
from connectors.logger import logger
from connectors.source import (
    get_data_sources,
    get_data_source,
    ServiceTypeNotSupportedError,
    DataSourceError,
    purge_cache as purge_sources,
)
from connectors.utils import CancellableSleeps


class ConnectorService:
    def __init__(self, config_file):
        self.config_file = config_file
        self.errors = [0, time.time()]
        if not os.path.exists(config_file):
            raise IOError(f"{config_file} does not exist")
        self.config = EnvYAML(config_file)
        self.ent_search_config()
        self.service_config = self.config["service"]
        self.idling = self.service_config["idling"]
        self.hb = self.service_config["heartbeat"]
        self.running = False
        self._sleeper = None
        self._sleeps = CancellableSleeps()
        self.connectors = None
        self.keep_alive = self.service_config.get("keep_alive", False)

    def ent_search_config(self):
        if "ENT_SEARCH_CONFIG_PATH" not in os.environ:
            return
        logger.info("Found ENT_SEARCH_CONFIG_PATH, loading ent-search config")
        ent_search_config = EnvYAML(os.environ["ENT_SEARCH_CONFIG_PATH"])
        for field in (
            "elasticsearch.host",
            "elasticsearch.username",
            "elasticsearch.password",
        ):
            sub = field.split(".")[-1]
            if field not in ent_search_config:
                continue
            logger.debug(f"Overriding {field}")
            self.config["elasticsearch"][sub] = ent_search_config[field]

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
        self._sleeps.cancel()
        if self.connectors is not None:
            self.connectors.stop_waiting()

    async def poll(self, one_sync=False, sync_now=True):
        """Main event loop."""
        loop = asyncio.get_event_loop()
        self.connectors = BYOIndex(self.config["elasticsearch"])
        es_host = self.config["elasticsearch"]["host"]
        self.running = True
        native_service_types = self.config.get("native_service_types", [])
        logger.debug(f"Native support for {', '.join(native_service_types)}")

        # XXX we can support multiple connectors but Ruby can't so let's use a
        # single id
        # connectors_ids = self.config.get("connectors_ids", [])
        if "connector_id" in self.config:
            connectors_ids = [self.config.get("connector_id")]
        else:
            connectors_ids = []

        if not (await self.connectors.wait()):
            logger.critical(f"{es_host} seem down. Bye!")
            return -1

        es = ElasticServer(self.config["elasticsearch"])
        logger.info(f"Service started, listening to events from {es_host}")
        try:
            while self.running:
                logger.debug(f"Polling every {self.idling} seconds")
                try:
                    async for connector in self.connectors.get_list():
                        # we only look at connectors we natively support or the
                        # ones where we have the connector_id explicitely
                        if (
                            connector.service_type not in native_service_types
                            and connector.id not in connectors_ids
                        ):
                            logger.debug(
                                f"Connector {connector.id} of type {connector.service_type} not supported, ignoring"
                            )
                            continue

                        if connector.native:
                            logger.debug(f"Connector {connector.id} natively supported")
                        try:
                            data_source = get_data_source(connector, self.config)
                        except ServiceTypeNotSupportedError:
                            logger.debug(
                                f"Can't handle source of type {connector.service_type}"
                            )
                            continue
                        except DataSourceError as e:
                            logger.critical(e, exc_info=True)
                            self.raise_if_spurious(e)
                            continue

                        await connector.is_configured()
                        loop.create_task(connector.heartbeat(self.hb))
                        await connector.sync(data_source, es, self.idling, sync_now)
                        await asyncio.sleep(0)

                        if one_sync:
                            self.stop()
                            break
                except Exception as e:
                    logger.critical(e, exc_info=True)
                    self.raise_if_spurious(e)

                if not one_sync:
                    await self._sleeps.sleep(self.idling)
                else:
                    self.stop()

                if not self.keep_alive:
                    await purge_sources()
        finally:
            await self.connectors.close()
            await es.close()
        return 0

    async def get_list(self):
        logger.info("Registered connectors:")
        for source in get_data_sources(self.config):
            logger.info(f"- {source.__doc__.strip()}")
        return 0

    def shutdown(self, sig, coro):
        logger.info(f"Caught {sig.name}. Gracefull shutdown.")
        self.stop()


def run(args):
    """Runner"""
    service = ConnectorService(args.config_file)
    loop = asyncio.get_event_loop()

    if args.action == "list":
        coro = asyncio.ensure_future(service.get_list())
    else:
        coro = asyncio.ensure_future(service.poll(args.one_sync, args.sync_now))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, functools.partial(service.shutdown, sig, coro))

    try:
        loop.run_until_complete(coro)
        logger.info("Bye")
        return coro.result()
    except asyncio.CancelledError:
        logger.info("Bye")
        return 0

    return -1
