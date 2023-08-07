#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import time

import elasticsearch

from connectors.es import Mappings
from connectors.es.client import License, with_concurrency_control
from connectors.es.index import DocumentNotFoundError
from connectors.es.license import requires_platinum_license
from connectors.es.sink import OP_INDEX, SyncOrchestrator, UnsupportedJobType
from connectors.logger import logger
from connectors.protocol import JobStatus, JobType
from connectors.utils import truncate_id

UTF_8 = "utf-8"

JOB_REPORTING_INTERVAL = 10
JOB_CHECK_INTERVAL = 1
ES_ID_SIZE_LIMIT = 512


class SyncJobRunningError(Exception):
    pass


class InsufficientESLicenseError(Exception):
    def __init__(self, required_license, actual_license):
        super().__init__(
            f"Minimum required Elasticsearch license: '{required_license.value}'. Actual license: '{actual_license.value}'."
        )


class SyncJobStartError(Exception):
    pass


class ConnectorNotFoundError(Exception):
    def __init__(self, connector_id):
        super().__init__(f"Connector is not found for connector ID {connector_id}.")


class ConnectorJobNotFoundError(Exception):
    def __init__(self, job_id):
        super().__init__(f"Connector job is not found for job ID {job_id}.")


class ConnectorJobCanceledError(Exception):
    pass


class ConnectorJobNotRunningError(Exception):
    def __init__(self, job_id, status):
        super().__init__(
            f"Connector job (ID: {job_id}) is not running but in status of {status}."
        )


class SyncJobRunner:
    """The class to run a sync job.

    It takes a sync job, and tries to claim it, and executes it. It also makes sure the sync job is updated
    appropriately when it errors out, is canceled, completes successfully or the service shuts down.

    Arguments:
        - `source_klass`: The source class of the connector
        - `sync_job`: The sync job to run
        - `connector`: The connector of the sync job
        - `es_config`: The elasticsearch configuration to build connection to Elasticsearch server

    """

    def __init__(
        self,
        source_klass,
        sync_job,
        connector,
        es_config,
    ):
        self.source_klass = source_klass
        self.data_provider = None
        self.sync_job = sync_job
        self.connector = connector
        self.es_config = es_config
        self.elastic_server = None
        self.job_reporting_task = None
        self.bulk_options = self.es_config.get("bulk", {})
        self._start_time = None
        self.running = False

    async def execute(self):
        if self.running:
            raise SyncJobRunningError(
                f"Sync job {self.sync_job.id} is already running."
            )

        self.running = True

        await self.sync_starts()
        sync_cursor = (
            self.connector.sync_cursor
            if self.sync_job.job_type == JobType.INCREMENTAL
            else None
        )
        await self.sync_job.claim(sync_cursor=sync_cursor)
        self._start_time = time.time()

        try:
            self.data_provider = self.source_klass(
                configuration=self.sync_job.configuration
            )
            self.data_provider.set_logger(self.sync_job.logger)
            if not await self.data_provider.changed():
                self.sync_job.log_debug("No change in remote source, skipping...")
                await self._sync_done(sync_status=JobStatus.COMPLETED)
                return

            self.data_provider.set_features(self.connector.features)

            self.sync_job.log_debug("Validating configuration")
            self.data_provider.validate_config_fields()
            await self.data_provider.validate_config()

            self.sync_job.log_debug("Pinging the backend")
            await self.data_provider.ping()

            job_type = self.sync_job.job_type

            # allows the data provider to change the bulk options
            bulk_options = self.bulk_options.copy()
            self.data_provider.tweak_bulk_options(bulk_options)

            self.elastic_server = SyncOrchestrator(self.es_config, self.sync_job.logger)

            if job_type in [JobType.INCREMENTAL, JobType.FULL]:
                await self._execute_content_sync_job(job_type, bulk_options)
            elif job_type == JobType.ACCESS_CONTROL:
                await self._execute_access_control_sync_job(job_type, bulk_options)
            else:
                raise UnsupportedJobType

            self.job_reporting_task = asyncio.create_task(
                self.update_ingestion_stats(JOB_REPORTING_INTERVAL)
            )

            while not self.elastic_server.done():
                await self.check_job()
                await asyncio.sleep(JOB_CHECK_INTERVAL)
            fetch_error = self.elastic_server.fetch_error()
            sync_status = (
                JobStatus.COMPLETED if fetch_error is None else JobStatus.ERROR
            )
            await self._sync_done(sync_status=sync_status, sync_error=fetch_error)
        except asyncio.CancelledError:
            await self._sync_done(sync_status=JobStatus.SUSPENDED)
        except ConnectorJobCanceledError:
            await self._sync_done(sync_status=JobStatus.CANCELED)
        except Exception as e:
            self.sync_job.log_error(e, exc_info=True)
            await self._sync_done(sync_status=JobStatus.ERROR, sync_error=e)
        finally:
            self.running = False
            if self.elastic_server is not None:
                await self.elastic_server.close()
            if self.data_provider is not None:
                await self.data_provider.close()

    async def _execute_access_control_sync_job(self, job_type, bulk_options):
        if requires_platinum_license(self.sync_job, self.connector, self.source_klass):
            (
                is_platinum_license_enabled,
                license_enabled,
            ) = await self.elastic_server.has_active_license_enabled(License.PLATINUM)

            if not is_platinum_license_enabled:
                raise InsufficientESLicenseError(
                    required_license=License.PLATINUM, actual_license=license_enabled
                )

        await self.elastic_server.async_bulk(
            self.sync_job.index_name,
            self.generator(),
            self.sync_job.pipeline,
            job_type,
            options=bulk_options,
        )

    async def _execute_content_sync_job(self, job_type, bulk_options):
        sync_rules_enabled = self.connector.features.sync_rules_enabled()
        if sync_rules_enabled:
            await self.sync_job.validate_filtering(validator=self.data_provider)
        mappings = Mappings.default_text_fields_mappings(
            is_connectors_index=True,
        )
        logger.debug("Preparing the content index")
        await self.elastic_server.prepare_content_index(
            self.sync_job.index_name, mappings=mappings
        )

        content_extraction_enabled = (
            self.sync_job.configuration.get("use_text_extraction_service")
            or self.sync_job.pipeline["extract_binary_content"]
        )

        await self.elastic_server.async_bulk(
            self.sync_job.index_name,
            self.prepare_docs(),
            self.sync_job.pipeline,
            job_type,
            filter_=self.sync_job.filtering,
            sync_rules_enabled=sync_rules_enabled,
            content_extraction_enabled=content_extraction_enabled,
            options=bulk_options,
        )

    async def _sync_done(self, sync_status, sync_error=None):
        if self.elastic_server is not None and not self.elastic_server.done():
            await self.elastic_server.cancel()
        if self.job_reporting_task is not None and not self.job_reporting_task.done():
            self.job_reporting_task.cancel()
            try:
                await self.job_reporting_task
            except asyncio.CancelledError:
                self.sync_job.log_info("Job reporting task is stopped.")

        result = (
            {} if self.elastic_server is None else self.elastic_server.ingestion_stats()
        )
        ingestion_stats = {
            "indexed_document_count": result.get("indexed_document_count", 0),
            "indexed_document_volume": result.get("indexed_document_volume", 0),
            "deleted_document_count": result.get("deleted_document_count", 0),
        }

        if await self.reload_sync_job():
            if await self.reload_connector():
                ingestion_stats[
                    "total_document_count"
                ] = await self.connector.document_count()

            if sync_status == JobStatus.ERROR:
                await self.sync_job.fail(sync_error, ingestion_stats=ingestion_stats)
            elif sync_status == JobStatus.SUSPENDED:
                await self.sync_job.suspend(ingestion_stats=ingestion_stats)
            elif sync_status == JobStatus.CANCELED:
                await self.sync_job.cancel(ingestion_stats=ingestion_stats)
            else:
                await self.sync_job.done(ingestion_stats=ingestion_stats)

        if await self.reload_connector():
            sync_cursor = (
                self.data_provider.sync_cursor()
                if self.sync_job.is_content_sync()
                else None
            )
            await self.connector.sync_done(
                self.sync_job if await self.reload_sync_job() else None,
                cursor=sync_cursor,
            )

        self.sync_job.log_info(
            f"Sync done: {ingestion_stats.get('indexed_document_count')} indexed, "
            f"{ingestion_stats.get('deleted_document_count')} deleted. "
            f"({int(time.time() - self._start_time)} seconds)"  # pyright: ignore
        )

    @with_concurrency_control()
    async def sync_starts(self):
        if not await self.reload_connector():
            raise SyncJobStartError(f"Couldn't reload connector {self.connector.id}")

        job_type = self.sync_job.job_type

        if job_type in [JobType.FULL, JobType.INCREMENTAL]:
            if self.connector.last_sync_status == JobStatus.IN_PROGRESS:
                logger.debug(
                    f"A content sync job is started for connector {self.connector.id} by another connector instance, skipping..."
                )
                raise SyncJobStartError(
                    f"A content sync job is started for connector {self.connector.id} by another connector instance"
                )
        elif job_type == JobType.ACCESS_CONTROL:
            if self.connector.last_access_control_sync_status == JobStatus.IN_PROGRESS:
                logger.debug(
                    f"An access control sync job is started for connector {self.connector.id} by another connector instance, skipping..."
                )
                raise SyncJobStartError(
                    f"An access control sync job is started for connector {self.connector.id} by another connector instance"
                )
        else:
            logger.error(f"Unknown job type: '{job_type}'. Skipping running sync job")
            raise SyncJobStartError(
                f"Unknown job type: '{job_type}'. Skipping running sync job"
            )

        try:
            await self.connector.sync_starts(job_type)
        except elasticsearch.ConflictError:
            raise
        except Exception as e:
            raise SyncJobStartError from e

    async def prepare_docs(self):
        self.sync_job.log_debug(f"Using pipeline {self.sync_job.pipeline}")

        async for doc, lazy_download, operation in self.generator():
            doc_id = str(doc.get("_id", ""))
            doc_id_size = len(doc_id.encode(UTF_8))

            if doc_id_size > ES_ID_SIZE_LIMIT:
                self.sync_job.log_debug(
                    f"Id '{truncate_id(doc_id)}' is too long: {doc_id_size} of maximum {ES_ID_SIZE_LIMIT} bytes, hashing"
                )

                hashed_id = self.source_klass.hash_id(doc_id)
                hashed_id_size = len(hashed_id.encode(UTF_8))

                if hashed_id_size > ES_ID_SIZE_LIMIT:
                    self.sync_job.log_error(
                        f"Hashed document id '{hashed_id}' with a size of '{hashed_id_size}' bytes is above the size limit of '{ES_ID_SIZE_LIMIT}' bytes."
                        f"Check the `hash_id` implementation of {self.source_klass.name}."
                    )
                    continue

                doc["_id"] = hashed_id

            # adapt doc for pipeline settings
            doc["_extract_binary_content"] = self.sync_job.pipeline[
                "extract_binary_content"
            ]
            doc["_reduce_whitespace"] = self.sync_job.pipeline["reduce_whitespace"]
            doc["_run_ml_inference"] = self.sync_job.pipeline["run_ml_inference"]
            yield doc, lazy_download, operation

    async def generator(self):
        match self.sync_job.job_type:
            case JobType.FULL:
                async for doc, lazy_download in self.data_provider.get_docs(
                    filtering=self.sync_job.filtering
                ):
                    yield doc, lazy_download, OP_INDEX
            case JobType.INCREMENTAL:
                async for doc, lazy_download, operation in self.data_provider.get_docs_incrementally(
                    sync_cursor=self.connector.sync_cursor,
                    filtering=self.sync_job.filtering,
                ):
                    yield doc, lazy_download, operation
            case JobType.ACCESS_CONTROL:
                async for doc in self.data_provider.get_access_control():
                    yield doc, None, None
            case _:
                raise UnsupportedJobType

    async def update_ingestion_stats(self, interval):
        while True:
            await asyncio.sleep(interval)

            if not await self.reload_sync_job():
                break

            result = self.elastic_server.ingestion_stats()
            ingestion_stats = {
                "indexed_document_count": result.get("indexed_document_count", 0),
                "indexed_document_volume": result.get("indexed_document_volume", 0),
                "deleted_document_count": result.get("deleted_document_count", 0),
            }
            await self.sync_job.update_metadata(ingestion_stats=ingestion_stats)

    async def check_job(self):
        if not await self.reload_connector():
            raise ConnectorNotFoundError(self.connector.id)

        if not await self.reload_sync_job():
            raise ConnectorJobNotFoundError(self.sync_job.id)

        if self.sync_job.status == JobStatus.CANCELING:
            raise ConnectorJobCanceledError

        if self.sync_job.status != JobStatus.IN_PROGRESS:
            raise ConnectorJobNotRunningError(self.sync_job.id, self.sync_job.status)

    async def reload_sync_job(self):
        try:
            await self.sync_job.reload()
            return True
        except DocumentNotFoundError:
            self.sync_job.log_error("Couldn't reload sync job")
            return False

    async def reload_connector(self):
        try:
            await self.connector.reload()
            return True
        except DocumentNotFoundError:
            self.connector.log_error("Couldn't reload connector")
            return False
