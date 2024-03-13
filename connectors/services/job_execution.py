#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from functools import cached_property

from connectors.es.client import License
from connectors.es.index import DocumentNotFoundError
from connectors.es.license import requires_platinum_license
from connectors.logger import logger
from connectors.protocol import (
    ConnectorIndex,
    DataSourceError,
    SyncJobIndex,
)
from connectors.services.base import BaseService
from connectors.source import get_source_klass
from connectors.sync_job_runner import SyncJobRunner
from connectors.utils import ConcurrentTasks


class JobExecutionService(BaseService):
    name = "execute"

    def __init__(self, config):
        super().__init__(config)
        self.idling = self.service_config["idling"]
        self.source_list = config["sources"]
        self.sync_job_pool = ConcurrentTasks(max_concurrency=self.max_concurrency)

    def stop(self):
        super().stop()
        self.sync_job_pool.cancel()

    @cached_property
    def display_name(self):
        raise NotImplementedError()

    @cached_property
    def max_concurrency_config(self):
        raise NotImplementedError()

    @cached_property
    def job_types(self):
        raise NotImplementedError()

    @cached_property
    def max_concurrency(self):
        raise NotImplementedError()

    def should_execute(self, connector, sync_job):
        raise NotImplementedError()

    async def _sync(self, sync_job):
        if sync_job.service_type not in self.source_list:
            msg = f"Couldn't find data source class for {sync_job.service_type}"
            raise DataSourceError(msg)
        source_klass = get_source_klass(self.source_list[sync_job.service_type])
        connector_id = sync_job.connector_id

        sync_job.log_debug(f"Detected pending {sync_job.job_type} sync.")

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

        if not self.should_execute(connector, sync_job):
            return

        sync_job_runner = SyncJobRunner(
            source_klass=source_klass,
            sync_job=sync_job,
            connector=connector,
            es_config=self._override_es_config(connector),
            service_config=self.service_config,
        )

        sync_job.log_debug(f"Attempting to start {sync_job.job_type} sync.")

        if not self.sync_job_pool.try_put(sync_job_runner.execute):
            sync_job.log_debug(
                f"{self.display_name.capitalize()} service is already running {self.max_concurrency} sync jobs and can't run more at this poinit. Increase '{self.max_concurrency_config}' in config if you want the service to run more sync jobs."  # pyright: ignore
            )

    async def _run(self):
        self.connector_index = ConnectorIndex(self.es_config)
        self.sync_job_index = SyncJobIndex(self.es_config)

        native_service_types = self.config.get("native_service_types", []) or []
        if len(native_service_types) > 0:
            logger.debug(
                f"Native support for {self.display_name} for {', '.join(native_service_types)}"
            )
        else:
            logger.debug(f"No native service types configured for {self.display_name}")

        connector_ids = list(self.connectors.keys())

        logger.info(
            f"{self.display_name.capitalize()} service started, listening to events from {self.es_config['host']}"
        )

        try:
            while self.running:
                try:
                    logger.debug(
                        f"Polling every {self.idling} seconds for {self.display_name}"
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
                            job_types=self.job_types,
                        ):
                            await self._sync(sync_job)
                except Exception as e:
                    logger.critical(e, exc_info=True)
                    self.raise_if_spurious(e)

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
