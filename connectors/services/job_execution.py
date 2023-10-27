#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from connectors.es.client import License
from connectors.es.index import DocumentNotFoundError
from connectors.es.license import requires_platinum_license
from connectors.logger import logger
from connectors.protocol import (
    ConnectorIndex,
    DataSourceError,
    JobStatus,
    JobType,
    SyncJobIndex,
)
from connectors.services.base import BaseService
from connectors.source import get_source_klass
from connectors.sync_job_runner import SyncJobRunner
from connectors.utils import ConcurrentTasks

DEFAULT_MAX_CONCURRENT_CONTENT_SYNCS = 1
DEFAULT_MAX_CONCURRENT_ACCESS_CONTROL_SYNCS = 1


def load_max_concurrent_access_control_syncs(config):
    return config.get(
        "max_concurrent_access_control_syncs",
        DEFAULT_MAX_CONCURRENT_ACCESS_CONTROL_SYNCS,
    )


def load_max_concurrent_content_syncs(config):
    max_concurrent_content_syncs = config.get("max_concurrent_content_syncs")

    if max_concurrent_content_syncs is not None:
        return max_concurrent_content_syncs

    logger.warning(
        "'max_concurrent_syncs' is deprecated. Use 'max_concurrent_content_syncs' in 'config.yml'."
    )

    # keep for backwards compatibility
    return config.get("max_concurrent_syncs", DEFAULT_MAX_CONCURRENT_CONTENT_SYNCS)


class JobExecutionService(BaseService):
    name = "execute"

    def __init__(self, config):
        super().__init__(config)
        self.idling = self.service_config["idling"]
        self.max_concurrent_content_syncs = load_max_concurrent_content_syncs(
            self.service_config
        )
        self.max_concurrent_access_control_syncs = (
            load_max_concurrent_access_control_syncs(self.service_config)
        )
        self.source_list = config["sources"]
        self.connector_index = None
        self.sync_job_index = None
        self.content_syncs = None
        self.access_control_syncs = None

    def stop(self):
        super().stop()
        if self.content_syncs is not None:
            self.content_syncs.cancel()
        if self.access_control_syncs is not None:
            self.access_control_syncs.cancel()

    async def _content_sync(self, sync_job, connector, source_klass):
        if connector.last_sync_status == JobStatus.IN_PROGRESS:
            sync_job.log_debug("Connector is still syncing content, skip the job...")
            return

        sync_job_runner = SyncJobRunner(
            source_klass=source_klass,
            sync_job=sync_job,
            connector=connector,
            es_config=self._override_es_config(connector),
        )
        await self.content_syncs.put(sync_job_runner.execute)

    async def _access_control_sync(self, sync_job, connector, source_klass):
        if connector.last_access_control_sync_status == JobStatus.IN_PROGRESS:
            sync_job.log_debug(
                "Connector is still syncing access control, skip the job..."
            )
            return

        sync_job_runner = SyncJobRunner(
            source_klass=source_klass,
            sync_job=sync_job,
            connector=connector,
            es_config=self._override_es_config(connector),
        )
        await self.access_control_syncs.put(sync_job_runner.execute)

    async def _sync(self, sync_job):
        if sync_job.service_type not in self.source_list:
            raise DataSourceError(
                f"Couldn't find data source class for {sync_job.service_type}"
            )
        source_klass = get_source_klass(self.source_list[sync_job.service_type])
        connector_id = sync_job.connector_id

        try:
            connector = await self.connector_index.fetch_by_id(connector_id)
        except DocumentNotFoundError:
            sync_job.log_error("Couldn't find connector")
            return

        if requires_platinum_license(sync_job, connector, source_klass):
            (
                is_platinum_license_enabled,
                license_enabled,
            ) = await self.connector_index.has_active_license_enabled(License.PLATINUM)

            if not is_platinum_license_enabled:
                sync_job.log_error(
                    f"Minimum required Elasticsearch license: '{License.PLATINUM.value}'. Actual license: '{license_enabled.value}'."
                )
                return

        job_type = sync_job.job_type

        if job_type in [JobType.FULL, JobType.INCREMENTAL]:
            await self._content_sync(sync_job, connector, source_klass)

        elif (
            job_type == JobType.ACCESS_CONTROL
            and connector.features.document_level_security_enabled()
        ):
            await self._access_control_sync(sync_job, connector, source_klass)

        else:
            sync_job.log_error(
                f"Unsupported job type '{job_type}'. Skipping sync job execution..."
            )

    async def _run(self):
        self.connector_index = ConnectorIndex(self.es_config)
        self.sync_job_index = SyncJobIndex(self.es_config)

        native_service_types = self.config.get("native_service_types", []) or []
        if len(native_service_types) > 0:
            logger.debug(
                f"Native support for job execution for {', '.join(native_service_types)}"
            )
        else:
            logger.debug("No native service types configured for job execution")

        connector_ids = list(self.connectors.keys())

        logger.info(
            f"Job Execution Service started, listening to events from {self.es_config['host']}"
        )

        try:
            while self.running:
                # creating pools of tasks for every round
                self.content_syncs = ConcurrentTasks(
                    max_concurrency=self.max_concurrent_content_syncs
                )
                self.access_control_syncs = ConcurrentTasks(
                    max_concurrency=self.max_concurrent_access_control_syncs
                )

                try:
                    logger.debug(
                        f"Polling every {self.idling} seconds for Job Execution"
                    )
                    supported_connector_ids = [
                        connector.id
                        async for connector in self.connector_index.supported_connectors(
                            native_service_types=native_service_types,
                            connector_ids=connector_ids,
                        )
                    ]

                    if len(supported_connector_ids) == 0:
                        logger.debug(
                            f"There's no supported connectors found with native service types [{', '.join(native_service_types)}] or connector ids [{', '.join(connector_ids)}]"
                        )
                    else:
                        async for sync_job in self.sync_job_index.pending_jobs(
                            connector_ids=supported_connector_ids,
                            job_types=[
                                JobType.FULL.value,
                                JobType.INCREMENTAL.value,
                                JobType.ACCESS_CONTROL.value,
                            ],
                        ):
                            await self._sync(sync_job)
                except Exception as e:
                    logger.critical(e, exc_info=True)
                    self.raise_if_spurious(e)
                finally:
                    await self.content_syncs.join()
                    await self.access_control_syncs.join()

                self.content_syncs = None
                self.access_control_syncs = None

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
