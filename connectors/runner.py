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
from connectors.byoc import (
    BYOIndex,
    Status,
    e2str,
    ServiceTypeNotConfiguredError,
    ConnectorUpdateError,
    ServiceTypeNotSupportedError,
    DataSourceError,
)
from connectors.logger import logger
from connectors.source import (
    get_data_sources,
)
from connectors.utils import CancellableSleeps, trace_mem


class ConnectorService:
    def __init__(self, config_file):
        self.config_file = config_file
        self.errors = [0, time.time()]
        if not os.path.exists(config_file):
            raise IOError(f"{config_file} does not exist")
        self.config = EnvYAML(config_file)
        self.ent_search_config()
        self.service_config = self.config["service"]
        self.trace_mem = self.service_config.get("trace_mem", False)
        self.idling = self.service_config["idling"]
        self.hb = self.service_config["heartbeat"]
        self.preflight_max_attempts = int(
            self.service_config.get("preflight_max_attempts", 10)
        )
        self.preflight_idle = int(self.service_config.get("preflight_idle", 30))
        self.running = False
        self._sleeper = None
        self._sleeps = CancellableSleeps()
        self.connectors = None
        self.trace_mem = self.service_config.get("trace_mem", False)

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

    async def _one_sync(self, connector, es, sync_now):
        with trace_mem(self.trace_mem):
            if connector.native:
                logger.debug(f"Connector {connector.id} natively supported")

            try:
                await connector.prepare(self.config)
            except ServiceTypeNotConfiguredError:
                logger.error(
                    f"Service type is not configured for connector {self.config['connector_id']}"
                )
                return
            except ConnectorUpdateError as e:
                logger.error(e)
                return
            except ServiceTypeNotSupportedError:
                logger.debug(f"Can't handle source of type {connector.service_type}")
                return
            except DataSourceError as e:
                await connector.error(e)
                logger.critical(e, exc_info=True)
                self.raise_if_spurious(e)
                return

            try:
                # the heartbeat is always triggered
                connector.start_heartbeat(self.hb)

                # we trigger a sync
                if connector.status in (Status.CREATED, Status.NEEDS_CONFIGURATION):
                    # we can't sync in that state
                    logger.info(f"Can't sync with status `{e2str(connector.status)}`")
                else:
                    await connector.sync(es, self.idling, sync_now)

                await asyncio.sleep(0)
            finally:
                await connector.close()

    async def poll(self, one_sync=False, sync_now=True):
        """Main event loop."""
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

        # pre-flight check
        attempts = 0
        while self.running:
            logger.info("Preflight checks...")
            try:
                # Checking the indices/pipeline in the loop to be less strict about the boot ordering
                await self.connectors.preflight()
                break
            except Exception as e:
                if attempts > self.preflight_max_attempts:
                    raise
                else:
                    logger.warn(
                        f"Attempt {attempts+1}/{self.preflight_max_attempts} failed. Retrying..."
                    )
                    logger.warn(str(e))
                    attempts += 1
                    await asyncio.sleep(self.preflight_idle)

        logger.info(f"Service started, listening to events from {es_host}")
        try:
            while self.running:
                try:
                    logger.debug(f"Polling every {self.idling} seconds")
                    async for connector in self.connectors.get_list():
                        if (
                            connector.service_type not in native_service_types
                            and connector.id not in connectors_ids
                        ):
                            logger.debug(
                                f"Connector {connector.id} of type {connector.service_type} not supported, ignoring"
                            )
                            continue

                        await self._one_sync(connector, es, sync_now)
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
        finally:
            await self.connectors.close()
            await es.close()

        return 0

    async def get_list(self):
        logger.info("Registered connectors:")
        for source in get_data_sources(self.config):
            logger.info(f"- {source.__doc__.strip()}")
        return 0

    def shutdown(self, sig):
        logger.info(f"Caught {sig.name}. Graceful shutdown.")
        self.stop()


def run(args):
    """Runner"""
    service = ConnectorService(args.config_file)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop is None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

    if args.action == "list":
        coro = service.get_list()
    else:
        coro = service.poll(args.one_sync, args.sync_now)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, functools.partial(service.shutdown, sig))

    try:
        return loop.run_until_complete(coro)
    except asyncio.CancelledError:
        return 0
    finally:
        logger.info("Bye")

    return -1
