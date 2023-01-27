#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import time

from connectors.byoc import JobStatus
from connectors.es import Mappings
from connectors.filtering.validation import (
    FilteringValidationState,
    InvalidFilteringError,
)
from connectors.logger import logger


class SyncJobRunningError(Exception):
    pass


class SyncJobRunner:
    """The class to run a sync job."""

    def __init__(
        self,
        source_klass,
        sync_job,
        connector,
        elastic_server,
        bulk_options,
    ):
        self.source_klass = source_klass
        self.sync_job = sync_job
        self.job_id = self.sync_job.id
        self.connector = connector
        self.connector_id = self.connector.id
        self.elastic_server = elastic_server
        self.bulk_options = bulk_options
        self._start_time = None
        self.running = False

    async def execute(self):
        if self.running:
            raise SyncJobRunningError(f"Sync job #{self.job_id} is already running.")

        self.running = True
        if not await self._claim_job():
            logger.error(f"Unable to claim job #{self.job_id}")
            return

        try:
            self._start_time = time.time()
            sync_status = None
            sync_error = None

            data_provider = self.source_klass(
                self.sync_job.configuration, self.sync_job.filtering
            )
            if not await data_provider.changed():
                logger.debug(
                    f"No change in {self.sync_job.service_type} data provider, skipping..."
                )
                return

            logger.debug(f"Syncing '{self.sync_job.service_type}'")
            logger.debug(f"Pinging the {self.source_klass} backend")
            await data_provider.ping()
            await asyncio.sleep(0)

            mappings = Mappings.default_text_fields_mappings(
                is_connectors_index=True,
            )

            logger.debug("Preparing the content index")
            await self.elastic_server.prepare_content_index(
                self.sync_job.index_name, mappings=mappings
            )
            await asyncio.sleep(0)

            # allows the data provider to change the bulk options
            bulk_options = self.bulk_options.copy()
            data_provider.tweak_bulk_options(bulk_options)

            sync_rules_enabled = self.connector.features.sync_rules_enabled()

            if sync_rules_enabled:
                await self._validate_filtering()

            result = await self.elastic_server.async_bulk(
                self.sync_job.index_name,
                self.prepare_docs(data_provider),
                self.sync_job.pipeline,
                filtering=self.sync_job.filtering,
                sync_rules_enabled=sync_rules_enabled,
                options=bulk_options,
            )
            sync_status = JobStatus.COMPLETED
            sync_error = None
        except Exception as e:
            sync_status = JobStatus.ERROR
            sync_error = e
            logger.critical(e, exc_info=True)
        finally:
            doc_updated = result.get("doc_updated", 0)
            doc_created = result.get("doc_created", 0)
            doc_deleted = result.get("doc_deleted", 0)
            sync_error = result.get("fetch_error", sync_error)
            indexed_count = doc_updated + doc_created

            if sync_error is not None or sync_status is None:
                sync_status = JobStatus.ERROR
            if sync_status == JobStatus.ERROR and sync_error is None:
                sync_error = "Sync thread didn't finish execution. Check connector logs for more details."

            ingestion_stats = {
                "indexed_document_count": indexed_count,
                "indexed_document_volume": 0,
                "deleted_document_count": 0,
                "total_document_count": await self.connector.document_count(),
            }
            if sync_status == JobStatus.ERROR:
                await self.sync_job.fail(sync_error, ingestion_stats=ingestion_stats)
            else:
                await self.sync_job.done(ingestion_stats=ingestion_stats)

            self.sync_job = await self.sync_job.reload()
            await self.connector.sync_done(self.sync_job)

            logger.info(
                f"Sync done: {indexed_count} indexed, {doc_deleted} "
                f" deleted. ({int(time.time() - self._start_time)} seconds)"
            )

    async def _claim_job(self):
        try:
            await self.sync_job.claim()
            await self.connector.sync_starts()
            return True
        except Exception as e:
            logger.critical(e, exc_info=True)
            return False

    async def prepare_docs(self, data_provider):
        logger.debug(f"Using pipeline {self.sync_job.pipeline}")

        async for doc, lazy_download in data_provider.get_docs():
            # adapt doc for pipeline settings
            doc[
                "_extract_binary_content"
            ] = self.sync_job.pipeline.extract_binary_content
            doc["_reduce_whitespace"] = self.sync_job.pipeline.reduce_whitespace
            doc["_run_ml_inference"] = self.sync_job.pipeline.run_ml_inference
            yield doc, lazy_download

    async def _validate_filtering(self):
        validation_result = await self.source_klass.validate_filtering(
            self.sync_job.filtering
        )
        if validation_result.state != FilteringValidationState.VALID:
            raise InvalidFilteringError(
                f"Filtering in state {validation_result.state}. Expected: {FilteringValidationState.VALID}."
            )
        if len(validation_result.errors):
            raise InvalidFilteringError(
                f"Filtering validation errors present: {validation_result.errors}."
            )
