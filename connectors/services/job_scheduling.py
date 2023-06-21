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
from datetime import datetime

from connectors.es.client import License, with_concurrency_control
from connectors.es.index import DocumentNotFoundError
from connectors.logger import logger
from connectors.protocol import (
    ConnectorIndex,
    DataSourceError,
    JobTriggerMethod,
    JobType,
    ServiceTypeNotConfiguredError,
    ServiceTypeNotSupportedError,
    Status,
    SyncJobIndex,
)
from connectors.services.base import BaseService
from connectors.source import get_source_klass


class JobSchedulingService(BaseService):
    name = "schedule"

    def __init__(self, config):
        super().__init__(config)
        self.idling = self.service_config["idling"]
        self.heartbeat_interval = self.service_config["heartbeat"]
        self.source_list = config["sources"]
        self.connector_index = None
        self.sync_job_index = None

    async def _schedule(self, connector):
        if self.running is False:
            logger.debug(
                f"Skipping run for {connector.id} because service is terminating"
            )
            return

        if connector.native:
            logger.debug(f"Connector {connector.id} natively supported")

        try:
            await connector.prepare(self.config)
        except DocumentNotFoundError:
            logger.error(f"Couldn't find connector by id {connector.id}")
            return
        except ServiceTypeNotConfiguredError:
            logger.error(
                f"Service type is not configured for connector {self.config['connector_id']}"
            )
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
        if connector.status == Status.CREATED:
            logger.info(
                f'Connector for {connector.service_type}(id: "{connector.id}") has just been created and cannot sync. Wait for Kibana to initialise connector correctly before proceeding.'
            )
            return

        if connector.status == Status.NEEDS_CONFIGURATION:
            logger.info(
                f'Connector for {connector.service_type}(id: "{connector.id}") is not configured yet. Finish connector configuration in Kibana to make it possible to run a sync.'
            )
            return

        if connector.service_type not in self.source_list:
            raise DataSourceError(
                f"Couldn't find data source class for {connector.service_type}"
            )

        source_klass = get_source_klass(self.source_list[connector.service_type])
        if connector.features.sync_rules_enabled():
            await connector.validate_filtering(
                validator=source_klass(connector.configuration)
            )

        if connector.features.document_level_security_enabled():
            (
                is_platinum_license_enabled,
                license_enabled,
            ) = await self.connector_index.has_active_license_enabled(
                License.PLATINUM
            )  # pyright: ignore

            if is_platinum_license_enabled:
                await self._scheduled_sync(connector, JobType.ACCESS_CONTROL)
            else:
                logger.error(
                    f"Minimum required Elasticsearch license: '{License.PLATINUM.value}'. Actual license: '{license_enabled.value}'. Skipping access control sync scheduling..."
                )

        if (
            connector.features.incremental_sync_enabled()
            and source_klass.support_incremental_sync
        ):
            await self._scheduled_sync(connector, JobType.INCREMENTAL)

        await self._scheduled_sync(connector, JobType.FULL)

    async def _run(self):
        """Main event loop."""
        self.connector_index = ConnectorIndex(self.es_config)
        self.sync_job_index = SyncJobIndex(self.es_config)

        native_service_types = self.config.get("native_service_types")
        if native_service_types is None:
            native_service_types = []
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
                try:
                    logger.debug(f"Polling every {self.idling} seconds")
                    async for connector in self.connector_index.supported_connectors(
                        native_service_types=native_service_types,
                        connector_ids=connector_ids,
                    ):
                        await self._schedule(connector)
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

    async def _scheduled_sync(self, connector, job_type):
        @with_concurrency_control()
        async def _should_schedule_scheduled_sync(job_type):
            try:
                await connector.reload()
            except DocumentNotFoundError:
                logger.error(f"Couldn't reload connector {connector.id}")
                return False

            job_type_value = job_type.value
            now = datetime.utcnow()
            last_sync_scheduled_at = connector.last_sync_scheduled_at_by_job_type(
                job_type
            )

            if last_sync_scheduled_at is not None and last_sync_scheduled_at > now:
                logger.debug(
                    f"A scheduled '{job_type_value}' sync is created by another connector instance, skipping..."
                )
                return False

            try:
                next_sync = connector.next_sync(job_type)
            except Exception as e:
                logger.critical(e, exc_info=True)
                await connector.error(str(e))
                return False

            if next_sync is None:
                logger.debug(
                    f"'{job_type_value}' sync scheduling is disabled for connector {connector.id}"
                )
                return False

            next_sync_due = (next_sync - now).total_seconds()
            if next_sync_due - self.idling > 0:
                logger.debug(
                    f"Next '{job_type_value}' sync for connector {connector.id} due in {int(next_sync_due)} seconds"
                )
                return False

            await connector.update_last_sync_scheduled_at_by_job_type(
                job_type, next_sync
            )

            return True

        if await _should_schedule_scheduled_sync(job_type):
            logger.info(
                f"Creating a scheduled '{job_type.value}' sync for connector {connector.id}..."
            )
            await self.sync_job_index.create(
                connector=connector,
                trigger_method=JobTriggerMethod.SCHEDULED,
                job_type=job_type,
            )
