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
    SYNC_DISABLED,
    BYOIndex,
    ConnectorUpdateError,
    DataSourceError,
    JobTriggerMethod,
    ServiceTypeNotConfiguredError,
    ServiceTypeNotSupportedError,
    Status,
    SyncJobIndex,
    e2str,
)
from connectors.logger import logger
from connectors.services.base import BaseService
from connectors.filtering.validation import (
    FilteringValidationState,
    InvalidFilteringError,
)


class JobSchedulerService(BaseService):
    def __init__(self, config):
        super().__init__(config)
        self.errors = [0, time.time()]
        self.service_config = self.config["service"]
        self.idling = self.service_config["idling"]
        self.hb = self.service_config["heartbeat"]
        self.connector_index = None
        self.sync_job_index = None

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
        if self.connector_index is not None:
            self.connector_index.stop_waiting()
        if self.sync_job_index is not None:
            self.sync_job_index.stop_waiting()

    async def _process_connector(self, connector):
        if connector.native:
            logger.debug(f"Connector {connector.id} natively supported")
        else:
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
                logger.debug(
                    f"Can't handle source of type {connector.service_type}"
                )
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
                return

            await _validate_filtering(connector)

            if not connector.sync_now:
                next_sync = connector.next_sync()
                if next_sync == SYNC_DISABLED or next_sync - self.idling > 0:
                    if next_sync == SYNC_DISABLED:
                        logger.debug(
                            f"Scheduling is disabled for {connector.service_type}"
                        )
                    else:
                        logger.debug(
                            f"Next sync for {connector.service_type} due in {int(next_sync)} seconds"
                        )
                    # if we don't sync, we still want to make sure we tell kibana we are connected
                    # if the status is different from connected
                    if connector.status != Status.CONNECTED:
                        connector.status = Status.CONNECTED
                        await connector.sync_doc()
                    return
            else:
                logger.info("Sync forced")

            await self._create_job(connector)

            await asyncio.sleep(0)
        except InvalidFilteringError as e:
            logger.error(e)
            self.raise_if_spurious(e)
            return
        finally:
            await connector.close()

    async def _create_job(self, connector):
        trigger_method = (
            JobTriggerMethod.ON_DEMAND
            if connector.sync_now
            else JobTriggerMethod.SCHEDULED
        )

        job_id = await self.sync_job_index.create(connector, trigger_method)

        connector.sync_now = connector.doc_source["sync_now"] = False

        await connector.sync_doc()

        logger.info(
            f"Created a job for connector {connector.service_type}, Job id: {job_id}"
        )

    async def run(self):
        if "PERF8" in os.environ:
            import perf8

            async with perf8.measure():
                return await self._run()
        else:
            return await self._run()

    async def _run(self):
        """Main event loop."""

        self.connector_index = BYOIndex(self.config["elasticsearch"])
        self.sync_job_index = SyncJobIndex(self.config["elasticsearch"])
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

        query = self.connector_index.build_docs_query(
            native_service_types, connectors_ids
        )

        try:
            while self.running:
                try:
                    logger.debug(f"Polling every {self.idling} seconds")
                    async for connector in self.connector_index.get_all_docs(
                        query=query
                    ):
                        await self._process_connector(connector)
                except Exception as e:
                    logger.critical(e, exc_info=True)
                    self.raise_if_spurious(e)
                await self._sleeps.sleep(self.idling)
        finally:
            self.stop()
            if self.connector_index is not None:
                await self.connector_index.close()
            if self.sync_job_index is not None:
                await self.sync_job_index.close()
        return 0
