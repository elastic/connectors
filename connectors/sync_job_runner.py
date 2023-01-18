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


class SyncJobRunner:
    """The class to run a sync job."""

    def __init__(
        self,
        sources,
        sync_job,
        connector,
        elastic_server,
        bulk_options,
    ):
        self.sources = sources
        self.sync_job = sync_job
        self.connector = connector
        self.elastic_server = elastic_server
        self.bulk_options = bulk_options
        self._start_time = None

    async def execute(self):
        await self.sync_job.claim()

        self._start_time = time.time()
        index_name = self.connector["index_name"]
        service_type = self.connector["service_type"]
        pipeline = PipelineSettings(self.connector.get("pipeline", {}))

        fqn = self.sources[service_type]
        configuration = DataSourceConfiguration(self.connector["configuration"])
        try:
            source_klass = get_source_klass(fqn)
        except Exception as e:
            logger.critical(e, exc_info=True)
            raise DataSourceError(f"Could not instantiate {fqn} for {service_type}")

        data_provider = source_klass(configuration)

        if not await data_provider.changed():
            logger.debug(f"No change in {service_type} data provider, skipping...")
            return

        logger.debug(f"Syncing '{service_type}'")
        try:
            logger.debug(f"Pinging the {source_klass} backend")
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
