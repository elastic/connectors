#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Event loop

- polls for work by calling Elasticsearch on a regular basis
- instantiates connector plugins
- mirrors an Elasticsearch index with a collection of documents
"""
import asyncio
import os
import time

from connectors.byoc import (
    ConnectorIndex,
    ConnectorUpdateError,
    DataSourceError,
    ServiceTypeNotConfiguredError,
    ServiceTypeNotSupportedError,
    Status,
    e2str,
)
from connectors.byoei import ElasticServer
from connectors.filtering.validation import (
    FilteringValidationState,
    InvalidFilteringError,
)
from connectors.logger import logger
from connectors.services.base import BaseService
from connectors.utils import CancellableSleeps


async def _validate_filtering(connector):
    validation_result = await connector.source_klass.validate_filtering(
        connector.filtering.get_active_filter()
    )
    if validation_result.state != FilteringValidationState.VALID:
        raise InvalidFilteringError(
            f"Filtering in state {validation_result.state}. Expected: {FilteringValidationState.VALID}."
        )
    if len(validation_result.errors):
        raise InvalidFilteringError(
            f"Filtering validation errors present: {validation_result.errors}."
        )


class SyncService(BaseService):
    def __init__(self, config, args):
        super().__init__(config)
        self.args = args
        self.errors = [0, time.time()]
        self.service_config = self.config["service"]
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

            logger.debug(f"Connector status is {connector.status}")

            # we trigger a sync
            if connector.status in (Status.CREATED, Status.NEEDS_CONFIGURATION):
                # we can't sync in that state
                logger.info(f"Can't sync with status `{e2str(connector.status)}`")
            else:
                await _validate_filtering(connector)
                await connector.sync(es, self.idling, sync_now)

            await asyncio.sleep(0)
        except InvalidFilteringError as e:
            logger.error(e)
            self.raise_if_spurious(e)
            return
        finally:
            await connector.close()

    async def run(self):
        if "PERF8" in os.environ:
            import perf8

            async with perf8.measure():
                return await self._run()
        else:
            return await self._run()

    async def _run(self):
        """Main event loop."""

        self.connectors = ConnectorIndex(self.config["elasticsearch"])
        self.running = True

        one_sync = self.args.one_sync
        sync_now = self.args.sync_now
        native_service_types = self.config.get("native_service_types", [])
        logger.debug(f"Native support for {', '.join(native_service_types)}")

        # XXX we can support multiple connectors but Ruby can't so let's use a
        # single id
        # connectors_ids = self.config.get("connectors_ids", [])
        if "connector_id" in self.config:
            connectors_ids = [self.config.get("connector_id")]
        else:
            connectors_ids = []

        logger.info(
            f"Service started, listening to events from {self.config['elasticsearch']['host']}"
        )

        es = ElasticServer(self.config["elasticsearch"])
        try:
            while self.running:
                try:
                    logger.debug(f"Polling every {self.idling} seconds")
                    query = self.connectors.build_docs_query(
                        native_service_types, connectors_ids
                    )

                    async for connector in self.connectors.get_all_docs(query=query):
                        await self._one_sync(connector, es, sync_now)
                    if one_sync:
                        break
                except Exception as e:
                    logger.critical(e, exc_info=True)
                    self.raise_if_spurious(e)
                finally:
                    if one_sync:
                        break
                await self._sleeps.sleep(self.idling)
        finally:
            self.stop()
            await self.connectors.close()
            await es.close()
        return 0
