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

import functools
from datetime import datetime, timezone
from dataclasses import dataclass

from connectors.logger import logger

from connectors.es.client import License, with_concurrency_control
from connectors.es.index import DocumentNotFoundError
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
from connectors.source import BaseDataSource, get_source_klass
from connectors.utils import ConcurrentTasks


@dataclass
class CachedDataSource:
    """Holds a cached DataSource instance with creation time."""
    data_source: BaseDataSource
    created_at: datetime
    service_type: str


class DataSourceCache:
    """Global cache for DataSource objects based on configuration hash."""

    def __init__(self, expiry_seconds: int = 300):
        """
        Initialize DataSource cache.

        Args:
            expiry_seconds: How long to cache DataSource objects (default: 300 seconds)
        """
        self.expiry_seconds = expiry_seconds
        self._cache: dict[str, CachedDataSource] = {}

    def get_or_create(self, service_type, configuration, source_list):
        """
        Get cached DataSource or create new one.

        Args:
            service_type: The connector service type
            configuration: DataSourceConfiguration object
            source_list: Available source classes

        Returns:
            DataSource instance (cached or newly created)
        """
        # Generate cache key from service type and config hash
        config_hash = str(configuration.hash_digest())
        cache_key = f"{service_type}:{config_hash}"

        logger.debug(f"DataSourceCache: Looking up cache key: {cache_key}")

        # Clean up expired entries
        self._cleanup_expired_entries()

        cached_entry = self._cache.get(cache_key)

        if cached_entry and (
            datetime.now(timezone.utc) - cached_entry.created_at
        ).total_seconds() < self.expiry_seconds:
            age_seconds = (datetime.now(timezone.utc) - cached_entry.created_at).total_seconds()
            logger.info(f"DataSourceCache: Cache HIT for {service_type} (age: {age_seconds:.1f}s)")
            return cached_entry.data_source

        # Create new DataSource instance
        logger.info(f"DataSourceCache: Cache MISS for {service_type}, creating new instance")
        source_klass = get_source_klass(source_list[service_type])
        data_source = source_klass(configuration)

        # Cache the new instance
        self._cache[cache_key] = CachedDataSource(
            data_source=data_source,
            created_at=datetime.now(timezone.utc),
            service_type=service_type
        )
        logger.debug(f"DataSourceCache: Cached new {service_type} instance. Cache size: {len(self._cache)}")

        return data_source

    def _cleanup_expired_entries(self):
        """Remove expired cache entries (sync version - doesn't close DataSources)."""
        current_time = datetime.now(timezone.utc)
        expired_keys = [
            key for key, cached_entry in self._cache.items()
            if (current_time - cached_entry.created_at).total_seconds() >= self.expiry_seconds
        ]

        for key in expired_keys:
            # Just remove from cache - async cleanup will properly close them
            del self._cache[key]

    async def cleanup_expired_entries_async(self):
        """Async cleanup that properly closes expired DataSource objects."""
        current_time = datetime.now(timezone.utc)
        expired_keys = [
            key for key, cached_entry in self._cache.items()
            if (current_time - cached_entry.created_at).total_seconds() >= self.expiry_seconds
        ]

        if expired_keys:
            logger.info(f"DataSourceCache: Cleaning up {len(expired_keys)} expired entries")

        for key in expired_keys:
            cached_entry = self._cache[key]
            try:
                logger.debug(f"DataSourceCache: Closing expired {cached_entry.service_type} DataSource")
                await cached_entry.data_source.close()
            except Exception as e:
                logger.error(f"DataSourceCache: Error closing {cached_entry.service_type} DataSource: {e}")
            del self._cache[key]

        if expired_keys:
            logger.debug(f"DataSourceCache: Cache size after cleanup: {len(self._cache)}")

    async def close_all(self):
        """Close all cached DataSource objects."""
        if self._cache:
            logger.info(f"DataSourceCache: Closing all {len(self._cache)} cached DataSource objects")

        for key, cached_entry in self._cache.items():
            try:
                logger.debug(f"DataSourceCache: Closing {cached_entry.service_type} DataSource on shutdown")
                await cached_entry.data_source.close()
            except Exception as e:
                logger.error(f"DataSourceCache: Error closing {cached_entry.service_type} DataSource on shutdown: {e}")

        self._cache.clear()
        logger.debug("DataSourceCache: All cached DataSources closed")


# Global DataSource cache instance
_data_source_cache = DataSourceCache(expiry_seconds=75)


class JobSchedulingService(BaseService):
    name = "schedule"

    def __init__(self, config):
        super().__init__(config, "job_scheduling_service")
        self.idling = self.service_config["idling"]
        self.heartbeat_interval = self.service_config["heartbeat"]
        self.source_list = config["sources"]
        self.first_run = True
        self.last_wake_up_time = datetime.now(timezone.utc)
        self.max_concurrency = self.service_config.get(
            "max_concurrent_scheduling_tasks"
        )
        self.schedule_tasks_pool = ConcurrentTasks(max_concurrency=self.max_concurrency)

    def stop(self):
        super().stop()
        self.schedule_tasks_pool.cancel()

    async def _schedule(self, connector):
        # To do some first-time stuff
        just_started = self.first_run
        self.first_run = False

        if self.running is False:
            connector.log_debug("Skipping run because service is terminating")
            return

        if connector.native:
            connector.log_debug("Natively supported")

        try:
            await connector.prepare(
                self.connectors.get(connector.id, {}), self.config["sources"]
            )
        except DocumentNotFoundError:
            connector.log_error("Couldn't find connector")
            return
        except ServiceTypeNotConfiguredError:
            connector.log_error("Service type is not configured")
            return
        except ServiceTypeNotSupportedError:
            connector.log_debug(f"Can't handle source of type {connector.service_type}")
            return
        except DataSourceError as e:
            await connector.error(e)
            connector.log_error(e, exc_info=True)
            raise

        # the heartbeat is always triggered
        await connector.heartbeat(self.heartbeat_interval, force=just_started)

        connector.log_debug(f"Status is {connector.status}")

        # we trigger a sync
        if connector.status == Status.CREATED:
            connector.log_info(
                "Connector has just been created and cannot sync. Wait for Kibana to initialise connector correctly before proceeding."
            )
            return

        if connector.status == Status.NEEDS_CONFIGURATION:
            connector.log_info(
                "Connector is not configured yet. Finish connector configuration in Kibana to make it possible to run a sync."
            )
            return

        if connector.service_type not in self.source_list:
            msg = f"Couldn't find data source class for {connector.service_type}"
            raise DataSourceError(msg)

        # Use cached DataSource to avoid repeated expensive operations
        connector.log_debug(f"JobScheduling: Getting DataSource for {connector.service_type}")
        data_source = _data_source_cache.get_or_create(
            service_type=connector.service_type,
            configuration=connector.configuration,
            source_list=self.source_list
        )
        data_source.set_logger(connector.logger)
        connector.log_debug(f"JobScheduling: Retrieved DataSource for {connector.service_type}")

        try:
            connector.log_debug("Validating configuration")
            data_source.validate_config_fields()
            await data_source.validate_config()

            connector.log_debug("Pinging the backend")
            await data_source.ping()

            if connector.features.sync_rules_enabled():
                await connector.validate_filtering(validator=data_source)

            connector.log_debug(
                "Connector is configured correctly and can reach the data source"
            )
            await connector.connected()
        except Exception as e:
            connector.log_error(e, exc_info=True)
            await connector.error(e)
            return
        # Note: DataSource is cached, so we don't close it here
        # It will be closed automatically when cache entries expire

        if connector.features.document_level_security_enabled():
            (
                is_platinum_license_enabled,
                license_enabled,
            ) = await self.connector_index.has_active_license_enabled(License.PLATINUM)  # pyright: ignore

            if is_platinum_license_enabled:
                await self._try_schedule_sync(connector, JobType.ACCESS_CONTROL)
            else:
                connector.log_error(
                    f"Minimum required Elasticsearch license: '{License.PLATINUM.value}'. Actual license: '{license_enabled.value}'. Skipping access control sync scheduling..."
                )

        if (
            connector.features.incremental_sync_enabled()
            and data_source.incremental_sync_enabled
        ):
            await self._try_schedule_sync(connector, JobType.INCREMENTAL)

        await self._try_schedule_sync(connector, JobType.FULL)

    async def _run(self):
        """Main event loop."""
        self.connector_index = ConnectorIndex(self.es_config)
        self.sync_job_index = SyncJobIndex(self.es_config)

        native_service_types = self.config.get("native_service_types", []) or []
        if len(native_service_types) > 0:
            self.logger.debug(
                f"Native support for job scheduling for {', '.join(native_service_types)}"
            )
        else:
            self.logger.debug("No native service types configured for job scheduling")
        connector_ids = list(self.connectors.keys())

        self.logger.info(
            f"Job Scheduling Service started, listening to events from {self.es_config['host']}"
        )

        try:
            while self.running:
                try:
                    self.logger.debug(
                        f"Polling every {self.idling} seconds for Job Scheduling"
                    )

                    # Periodically clean up expired cached DataSources
                    self.logger.debug("JobScheduling: Running periodic DataSource cache cleanup")
                    await _data_source_cache.cleanup_expired_entries_async()

                    async for connector in self.connector_index.supported_connectors(
                        native_service_types=native_service_types,
                        connector_ids=connector_ids,
                    ):
                        if not self.schedule_tasks_pool.try_put(
                            functools.partial(self._schedule, connector)
                        ):
                            connector.log_debug(
                                f"Job Scheduling service is already running {self.max_concurrency} concurrent scheduling jobs and can't run more at this point. Increase 'max_concurrent_scheduling_tasks' in config if you want the service to run more concurrent scheduling jobs."  # pyright: ignore
                            )

                except Exception as e:
                    self.logger.error(e, exc_info=True)
                    self.raise_if_spurious(e)

                # Immediately break instead of sleeping
                await self.schedule_tasks_pool.join()
                if not self.running:
                    break
                self.last_wake_up_time = datetime.now(timezone.utc)
                await self._sleeps.sleep(self.idling)
        finally:
            # Clean up all cached DataSources on shutdown
            self.logger.info("JobScheduling: Shutting down - closing all cached DataSources")
            await _data_source_cache.close_all()

            if self.connector_index is not None:
                self.connector_index.stop_waiting()
                await self.connector_index.close()
            if self.sync_job_index is not None:
                self.sync_job_index.stop_waiting()
                await self.sync_job_index.close()
        return 0

    async def _try_schedule_sync(self, connector, job_type):
        this_wake_up_time = datetime.now(timezone.utc)
        last_wake_up_time = self.last_wake_up_time

        self.logger.debug(
            f"Scheduler woke up at {this_wake_up_time}. Previously woke up at {last_wake_up_time}."
        )

        @with_concurrency_control()
        async def _should_schedule(job_type):
            try:
                await connector.reload()
            except DocumentNotFoundError:
                connector.log_error("Couldn't reload connector")
                return False

            job_type_value = job_type.value

            last_sync_scheduled_at = connector.last_sync_scheduled_at_by_job_type(
                job_type
            )

            self.logger.debug(f"Last sync was scheduled at {last_sync_scheduled_at}")

            if (
                last_sync_scheduled_at is not None
                and last_sync_scheduled_at > last_wake_up_time
            ):
                connector.log_debug(
                    f"A scheduled '{job_type_value}' sync is created by another connector instance, skipping..."
                )
                return False

            try:
                next_sync = connector.next_sync(job_type, last_wake_up_time)
                connector.log_debug(f"Next '{job_type_value}' sync is at {next_sync}")
            except Exception as e:
                connector.log_error(e, exc_info=True)
                await connector.error(str(e))
                return False

            if next_sync is None:
                connector.log_debug(f"'{job_type_value}' sync scheduling is disabled")
                return False

            if this_wake_up_time < next_sync:
                next_sync_due = (next_sync - datetime.now(timezone.utc)).total_seconds()
                connector.log_debug(
                    f"Next '{job_type_value}' sync due in {int(next_sync_due)} seconds"
                )
                return False

            await connector.update_last_sync_scheduled_at_by_job_type(
                job_type, next_sync
            )

            return True

        if await _should_schedule(job_type):
            connector.log_info(f"Creating a scheduled '{job_type.value}' sync...")
            await self.sync_job_index.create(
                connector=connector,
                trigger_method=JobTriggerMethod.SCHEDULED,
                job_type=job_type,
            )
