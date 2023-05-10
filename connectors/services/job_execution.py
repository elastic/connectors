#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.es.index import DocumentNotFoundError
from connectors.logger import logger
from connectors.protocol import ConnectorIndex, DataSourceError, JobStatus, SyncJobIndex
from connectors.services.base import BaseService
from connectors.source import get_source_klass
from connectors.sync_job_runner import SyncJobRunner
from connectors.utils import ConcurrentTasks

DEFAULT_MAX_CONCURRENT_SYNCS = 1


class JobExecutionService(BaseService):
    name = "execute"

    def __init__(self, config):
        super().__init__(config)
        self.idling = self.service_config["idling"]
        self.concurrent_syncs = self.service_config.get(
            "max_concurrent_syncs", DEFAULT_MAX_CONCURRENT_SYNCS
        )
        self.source_list = config["sources"]
        self.connector_index = None
        self.sync_job_index = None
        self.syncs = None

    def stop(self):
        super().stop()
        if self.syncs is not None:
            self.syncs.cancel()

    async def _sync(self, sync_job):
        if sync_job.service_type not in self.source_list:
            raise DataSourceError(
                f"Couldn't find data source class for {sync_job.service_type}"
            )
        source_klass = get_source_klass(self.source_list[sync_job.service_type])

        try:
            connector = await self.connector_index.fetch_by_id(sync_job.connector_id)
        except DocumentNotFoundError:
            logger.error(f"Couldn't find connector by id {sync_job.connector_id}")
            return

        if connector.last_sync_status == JobStatus.IN_PROGRESS:
            logger.debug(
                f"Connector {connector.id} is still syncing, skip the job {sync_job.id}..."
            )
            return

        sync_job_runner = SyncJobRunner(
            source_klass=source_klass,
            sync_job=sync_job,
            connector=connector,
            es_config=self.es_config,
        )
        await self.syncs.put(sync_job_runner.execute)

    async def _run(self):
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

        try:
            while self.running:
                # creating a pool of task for every round
                self.syncs = ConcurrentTasks(max_concurrency=self.concurrent_syncs)

                try:
                    logger.debug(f"Polling every {self.idling} seconds")
                    supported_connector_ids = [
                        connector.id
                        async for connector in self.connector_index.supported_connectors(
                            native_service_types=native_service_types,
                            connector_ids=connector_ids,
                        )
                    ]

                    if len(supported_connector_ids) == 0:
                        logger.info(
                            f"There's no supported connectors found with native service types [{', '.join(native_service_types)}] and connector ids [{', '.join(connector_ids)}]"
                        )
                    else:
                        async for sync_job in self.sync_job_index.pending_jobs(
                            connector_ids=supported_connector_ids
                        ):
                            await self._sync(sync_job)
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
        return 0
