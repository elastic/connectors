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
import asyncio
import os
import time

from connectors.byoc import BYOIndex, DataSourceError, PipelineSettings, SyncJobIndex
from connectors.byoei import ElasticServer
from connectors.es import defaults_for
from connectors.logger import logger
from connectors.services.base import BaseService
from connectors.source import DataSourceConfiguration, get_source_klass


class SyncJobRunner:
    def __init__(
        self,
        sources,
        sync_job,
        elastic_server,
        connector_index,
        sync_job_index,
        bulk_options,
    ):
        self.sources = sources
        self.sync_job = sync_job
        self.elastic_server = elastic_server
        self.bulk_options = bulk_options
        self.connector_index = connector_index
        self.sync_job_index = sync_job_index

    async def execute(self):
        await self.sync_job.claim()

        start_time = time.time()
        index_name = self.sync_job.connector["index_name"]
        service_type = self.sync_job.connector["service_type"]
        language_code = self.sync_job.connector["language"]
        pipeline = PipelineSettings(self.sync_job.connector.get("pipeline", {}))

        fqn = self.sources[service_type]
        configuration = DataSourceConfiguration(
            self.sync_job.connector["configuration"]
        )
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

            mappings, settings = defaults_for(
                is_connectors_index=True, language_code=language_code
            )
            logger.debug("Preparing the index")
            await self.elastic_server.prepare_index(
                index_name, mappings=mappings, settings=settings
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
            await self._sync_done(result, start_time)

        except Exception as e:
            await self._sync_done({}, start_time, exception=e)
            raise
        finally:
            self._start_time = None

    async def prepare_docs(self, pipeline, data_provider):
        logger.debug(f"Using pipeline {pipeline}")

        async for doc, lazy_download in data_provider.get_docs():
            # adapt doc for pipeline settings
            doc["_extract_binary_content"] = pipeline.extract_binary_content
            doc["_reduce_whitespace"] = pipeline.reduce_whitespace
            doc["_run_ml_inference"] = pipeline.run_ml_inference
            yield doc, lazy_download

    async def _sync_done(self, result, start_time, exception=None):
        doc_updated = result.get("doc_updated", 0)
        doc_created = result.get("doc_created", 0)
        doc_deleted = result.get("doc_deleted", 0)
        exception = result.get("fetch_error", exception)

        indexed_count = doc_updated + doc_created

        await self.sync_job.done(indexed_count, doc_deleted, exception)

        # Loading connector as late as possible so that there's less likely
        # a version conflict.
        connector = await self.connector_index.fetch_by_id(self.sync_job.connector_id)
        await connector.job_done(self.sync_job)

        logger.info(
            f"Sync done: {indexed_count} indexed, {doc_deleted} "
            f" deleted. ({int(time.time() - start_time)} seconds)"
        )


class JobExecutorService(BaseService):
    def __init__(self, config):
        super().__init__(config)
        self.errors = [0, time.time()]
        self.service_config = self.config["service"]
        self.idling = self.service_config["idling"]
        self.connector_index = None
        self.sync_job_index = None

    def raise_if_spurious(self, exception):
        errors, first = self.errors
        errors += 1

        # if we piled up too many errors we raise and quit
        if errors > self.service_config["max_errors"]:
            raise exception

        # we re-init every ten minutes
        if time.time() - first > self.service_config["max_errors_span"]:
            first = time.time()
            errors = 0

        self.errors[0] = errors
        self.errors[1] = first

    def stop(self):
        self.running = False
        self._sleeps.cancel()
        if self.connector_index is not None:
            self.connector_index.stop_waiting()
        if self.sync_job_index is not None:
            self.sync_job_index.stop_waiting()

    async def _process_sync_job(self, sync_job):
        try:
            sync_job_runner = SyncJobRunner(
                sources=self.config["sources"],
                sync_job=sync_job,
                elastic_server=self.elastic_server,
                connector_index=self.connector_index,
                sync_job_index=self.sync_job_index,
                bulk_options=self.bulk_options,
            )
            await sync_job_runner.execute()

            await asyncio.sleep(0)
        finally:
            await sync_job.close()

    async def run(self):
        if "PERF8" in os.environ:
            import perf8

            async with perf8.measure():
                return await self._run()
        else:
            return await self._run()

    async def _run(self):
        """Main event loop."""

        self.connector_index = BYOIndex(self.config["elasticsearch"])
        self.sync_job_index = SyncJobIndex(self.config["elasticsearch"])
        self.running = True

        native_service_types = self.config.get("native_service_types", [])
        logger.debug(f"Native support for {', '.join(native_service_types)}")

        # XXX we can support multiple connectors but Ruby can't so let's use a
        # single id
        # connectors_ids = self.config.get("connectors_ids", [])
        if "connector_id" in self.config:
            connectors_ids = [self.config.get("connector_id")]
        else:
            connectors_ids = []

        query = self.sync_job_index.build_docs_query(
            native_service_types, connectors_ids
        )

        es = ElasticServer(self.config["elasticsearch"])
        try:
            while self.running:
                try:
                    logger.debug(f"Polling every {self.idling} seconds")

                    async for sync_job in self.sync_job_index.get_all_docs(query=query):
                        await self._process_sync_job(sync_job)
                except Exception as e:
                    logger.critical(e, exc_info=True)
                    self.raise_if_spurious(e)
                await self._sleeps.sleep(self.idling)
        finally:
            self.stop()
            if self.connector_index is not None:
                await self.connector_index.close()
            if self.sync_job_index is not None:
                await self.sync_job_index.close()
            await es.close()
        return 0
