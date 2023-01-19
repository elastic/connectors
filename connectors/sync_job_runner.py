#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import time

from connectors.byoc import DataSourceError, PipelineSettings
from connectors.es import Mappings
from connectors.logger import logger
from connectors.source import DataSourceConfiguration, get_source_klass

from connectors.filtering.validation import (
    FilteringValidationState,
    InvalidFilteringError,
)


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
        if not await self.sync_job.claim():
            logger.error(f"Unable to claim job #{self.job_id}")
            return

        try:
            self._start_time = time.time()
            await self._validate_filtering()

            index_name = self.sync_job.connector["index_name"]
            service_type = self.sync_job.connector["service_type"]
            pipeline = PipelineSettings(self.sync_job.connector.get("pipeline", {}))

            configuration = DataSourceConfiguration(self.sync_job.connector["configuration"])
            data_provider = self.source_klass(configuration)

            if not await data_provider.changed():
                logger.debug(f"No change in {service_type} data provider, skipping...")
                return

            logger.debug(f"Syncing '{service_type}'")
            logger.debug(f"Pinging the {self.source_klass} backend")
            await data_provider.ping()
            await asyncio.sleep(0)

            mappings = Mappings.default_text_fields_mappings(
                is_connectors_index=True,
            )

            logger.debug("Preparing the content index")
            await self.elastic_server.prepare_content_index(
                self.index_name, mappings=mappings
            )
            await asyncio.sleep(0)

            result = await self.elastic_server.async_bulk(
                index_name,
                self.prepare_docs(pipeline, data_provider),
                pipeline,
                options=self.bulk_options,
            )
            for i in range(50):
                await asyncio.sleep(0.1)
            await self._sync_done(result)

        except Exception as e:
            await self._sync_done({}, exception=e)
            raise

    async def prepare_docs(self, pipeline, data_provider):
        logger.debug(f"Using pipeline {pipeline}")

        async for doc, lazy_download in data_provider.get_docs():
            # adapt doc for pipeline settings
            doc["_extract_binary_content"] = pipeline.extract_binary_content
            doc["_reduce_whitespace"] = pipeline.reduce_whitespace
            doc["_run_ml_inference"] = pipeline.run_ml_inference
            yield doc, lazy_download

    async def _sync_done(self, result, exception=None):
        doc_updated = result.get("doc_updated", 0)
        doc_created = result.get("doc_created", 0)
        doc_deleted = result.get("doc_deleted", 0)
        exception = result.get("fetch_error", exception)

        indexed_count = doc_updated + doc_created

        await self.sync_job.done(indexed_count, doc_deleted, exception)
        await self.connector.job_done(self.sync_job)

        logger.info(
            f"Sync done: {indexed_count} indexed, {doc_deleted} "
            f" deleted. ({int(time.time() - self._start_time)} seconds)"
        )

    async def _validate_filtering(self):
        validation_result = await self.source_klass.validate_filtering(
            self.sync_job.filtering.get_active_filter()
        )
        if validation_result.state != FilteringValidationState.VALID:
            raise InvalidFilteringError(
                f"Filtering in state {validation_result.state}. Expected: {FilteringValidationState.VALID}."
            )
        if len(validation_result.errors):
            raise InvalidFilteringError(
                f"Filtering validation errors present: {validation_result.errors}."
            )
