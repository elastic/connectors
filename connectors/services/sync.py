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
    SyncJob,
    e2str,
)
from connectors.byoei import ElasticServer
from connectors.logger import logger
from connectors.services.base import BaseService
from connectors.utils import CancellableSleeps, trace_mem


class SyncService(BaseService):
    def __init__(self, args):
        super().__init__(args)
        self.service_config = self.config["service"]
        self.trace_mem = self.service_config.get("trace_mem", False)
        self.idling = self.service_config["idling"]
        self.preflight_max_attempts = int(
            self.service_config.get("preflight_max_attempts", 10)
        )
        self.preflight_idle = int(self.service_config.get("preflight_idle", 30))
        self.trace_mem = self.service_config.get("trace_mem", False)

        self.connectors = BYOIndex(self.config["elasticsearch"])
        self.jobs = SyncJobIndex(self.config["elasticsearch"])

        self.running = False
        self.errors = [0, time.time()]
        self._sleeps = CancellableSleeps()

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
        logger.debug("Shutting down producers")
        self.running = False
        self._sleeps.cancel()
        if self.connectors is not None:
            self.connectors.stop_waiting()

    async def run(self):
        if "PERF8" in os.environ:
            import perf8

            async with perf8.measure():
                return await self._run()
        else:
            return await self._run()

    async def _run(self):
        """Main event loop."""

        self.connectors = BYOIndex(self.config["elasticsearch"])
        self.running = True

        one_sync = self.args.one_sync

        es_host = self.config["elasticsearch"]["host"]
        self.running = True

        if not await self.connectors.wait():
            logger.critical(f"{es_host} seem down. Bye!")
            return -1

        await self._pre_flight_check()

        logger.info(f"Service started, listening to events from {es_host}")
        try:
            if one_sync:
                logger.debug(f"Running a single sync")
                await self._tick()
            else:
                while self.running:
                    try:
                        logger.debug(f"Polling every {self.idling} seconds")
                        await self._tick()
                        await self._sleeps.sleep(self.idling)
                    except Exception as e:
                        logger.critical(e, exc_info=True)
                        self.raise_if_spurious(e)
        finally:
            self.stop()
            await self.connectors.close()
        return 0

    async def _pre_flight_check(self):
        es_host = self.config["elasticsearch"]["host"]

        if not (await self.connectors.wait()):
            logger.critical(f"{es_host} seem down. Bye!")
            return -1

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

    async def _tick(self):
        native_service_types = self.config.get("native_service_types", [])
        if "connector_id" in self.config:
            connectors_ids = [self.config.get("connector_id")]
        else:
            connectors_ids = []

        logger.debug(f"Native support for {', '.join(native_service_types)}")

        query = self.connectors.build_docs_query(native_service_types, connectors_ids)

        async for connector in self.connectors.get_all_docs(query=query):
            await connector.heartbeat()

            if connector.status in (Status.CREATED, Status.NEEDS_CONFIGURATION):
                # we can't sync in that state
                logger.info(f"Can't sync with status `{e2str(connector.status)}`")
                continue

            if not connector.sync_now:
                next_sync = connector.next_sync()
                if next_sync == SYNC_DISABLED or next_sync - self.idling > 0:
                    if next_sync == SYNC_DISABLED:
                        logger.debug(
                            f"Scheduling is disabled for {connector.service_type}"
                        )
                        continue
                    else:
                        logger.debug(
                            f"Next sync for {connector.service_type} due in {int(next_sync)} seconds"
                        )
                        continue
                    # if we don't sync, we still want to make
                    # sure we tell kibana we are connected
                    # if the status is different from connected
                    if connector.status != Status.CONNECTED:
                        connector.status = Status.CONNECTED
                        await connector.sync_doc()

            else:
                logger.info("Sync forced")

            await self._create_job(connector)

    async def _create_job(self, connector):
        trigger_method = (
            JobTriggerMethod.ON_DEMAND
            if connector.sync_now
            else JobTriggerMethod.SCHEDULED
        )

        print(f"Trigger method is {trigger_method}")

        job_id = await self.jobs.create(connector.id, trigger_method)

        connector.sync_now = connector.doc_source["sync_now"] = False

        logger.info(
            f"Created a job for connector {connector.service_type}, Job id: {job_id}"
        )
