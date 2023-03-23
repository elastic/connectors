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
from connectors.byoc import (
    SYNC_DISABLED,
    ConnectorIndex,
    ConnectorUpdateError,
    DataSourceError,
    JobStatus,
    ServiceTypeNotConfiguredError,
    ServiceTypeNotSupportedError,
    Status,
    SyncJobIndex,
)
from connectors.byoei import ElasticServer
from connectors.logger import logger
from connectors.services.base import BaseService
from connectors.source import get_source_klass_dict
from connectors.sync_job_runner import SyncJobRunner
from connectors.utils import ConcurrentTasks

DEFAULT_MAX_CONCURRENT_SYNCS = 1


class JobSchedulingService(BaseService):
    name = "poll"

    def __init__(self, config):
        super().__init__(config)
        self.idling = self.service_config["idling"]
        self.heartbeat_interval = self.service_config["heartbeat"]
        self.concurrent_syncs = self.service_config.get(
            "max_concurrent_syncs", DEFAULT_MAX_CONCURRENT_SYNCS
        )
        self.bulk_options = self.es_config.get("bulk", {})
        self.source_klass_dict = get_source_klass_dict(config)
        self.connector_index = None
        self.sync_job_index = None
        self.syncs = None

    def stop(self):
        super().stop()
        if self.syncs is not None:
            self.syncs.cancel()

    async def _sync(self, connector, es):
        if self.running is False:
            logger.debug(
                f"Skipping run for {connector.id} because service is terminating"
            )
            return

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
            raise

        # the heartbeat is always triggered
        await connector.heartbeat(self.heartbeat_interval)

        logger.debug(f"Connector status is {connector.status}")

        # we trigger a sync
        if connector.status in (Status.CREATED, Status.NEEDS_CONFIGURATION):
            # we can't sync in that state
            logger.info(f"Can't sync with status `{connector.status.value}`")
            return

        if connector.service_type not in self.source_klass_dict:
            raise DataSourceError(
                f"Couldn't find data source class for {connector.service_type}"
            )

        source_klass = self.source_klass_dict[connector.service_type]
        if connector.features.sync_rules_enabled():
            await connector.validate_filtering(
                validator=source_klass(connector.configuration)
            )

        if not await self._should_sync(connector):
            return

        job_id = await self.sync_job_index.create(connector)
        if connector.sync_now:
            await connector.reset_sync_now_flag()
        sync_job = await self.sync_job_index.fetch_by_id(job_id)
        sync_job_runner = SyncJobRunner(
            source_klass=source_klass,
            sync_job=sync_job,
            connector=connector,
            elastic_server=es,
            bulk_options=self.bulk_options,
        )
        await self.syncs.put(sync_job_runner.execute)

    async def _run(self):
        """Main event loop."""
        self.connector_index = ConnectorIndex(self.es_config)
        self.sync_job_index = SyncJobIndex(self.es_config)

        native_service_types = self.config.get("native_service_types", [])
        logger.debug(f"Native support for {', '.join(native_service_types)}")

        # TODO: we can support multiple connectors but Ruby can't so let's use a
        # single id
        # connector_ids = self.config.get("connector_ids", [])
        if "connector_id" in self.config:
            connector_ids = [self.config.get("connector_id")]
        else:
            connector_ids = []

        logger.info(
            f"Service started, listening to events from {self.es_config['host']}"
        )

        es = ElasticServer(self.es_config)
        try:
            while self.running:
                # creating a pool of task for every round
                self.syncs = ConcurrentTasks(max_concurrency=self.concurrent_syncs)

                try:
                    logger.debug(f"Polling every {self.idling} seconds")
                    async for connector in self.connector_index.supported_connectors(
                        native_service_types=native_service_types,
                        connector_ids=connector_ids,
                    ):
                        await self._sync(connector, es)
                except Exception as e:
                    logger.critical(e, exc_info=True)
                    self.raise_if_spurious(e)
                finally:
                    await self.syncs.join()

                self.syncs = None
                # Immediately break instead of sleeping
                if not self.running:
                    break
                await self._sleeps.sleep(self.idling)
        finally:
            if self.connector_index is not None:
                self.connector_index.stop_waiting()
                await self.connector_index.close()
            if self.sync_job_index is not None:
                self.sync_job_index.stop_waiting()
                await self.sync_job_index.close()
            await es.close()
        return 0

    async def _should_sync(self, connector):
        try:
            next_sync = connector.next_sync()
            # First we check if sync is disabled, and it terminates all other conditions
            if next_sync == SYNC_DISABLED:
                logger.debug(f"Scheduling is disabled for connector {connector.id}")
                return False
            # Then we check if we need to restart SUSPENDED job
            if connector.last_sync_status == JobStatus.SUSPENDED:
                logger.debug("Restarting sync after suspension")
                return True
            # And only then we check if we need to run sync right now or not
            if next_sync - self.idling > 0:
                logger.debug(
                    f"Next sync for connector {connector.id} due in {int(next_sync)} seconds"
                )
                return False
            return True
        except Exception as e:
            logger.critical(e, exc_info=True)
            await connector.error(str(e))
            return False
