#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Event loop

- polls for work by calling Elasticsearch on a regular basis
- instanciates connector plugins
- mirrors an Elasticsearch index with a collection of documents
"""
import asyncio
import os
import time

from connectors.es import defaults_for, DEFAULT_LANGUAGE
from connectors.byoc import DataSourceError, SyncJobIndex, PipelineSettings, BYOIndex
from connectors.byoei import ElasticServer
from connectors.logger import logger
from connectors.services.base import BaseService
from connectors.source import DataSourceConfiguration, get_source_klass
from connectors.utils import CancellableSleeps, trace_mem


class JobService(BaseService):
    def __init__(self, args):
        super().__init__(args)
        self.running = False

        self._sleeps = CancellableSleeps()

        elastic_config = self.config["elasticsearch"]

        self.elastic_server = ElasticServer(elastic_config)
        self.jobs = SyncJobIndex(elastic_config)
        self.connectors = BYOIndex(elastic_config)

        self.language_code = self.config.get("language_code", DEFAULT_LANGUAGE) # XXX: get language code from the connector instead
        self.bulk_options = elastic_config.get("bulk", {})

    async def run(self):
        one_sync = self.args.one_sync
        self.running = True

        try:
            while self.running:
                if "connector_id" in self.config:
                    connectors_ids = [self.config.get("connector_id")]
                else:
                    connectors_ids = []

                query = self.jobs.build_docs_query(connectors_ids)

                synced_anything = False
                async for job in self.jobs.get_all_docs(query=query):
                    await self.execute(job)
                    synced_anything = True

                if one_sync and synced_anything:
                    logger.info("Ran a round of syncs and exiting.")
                    break


                await self._sleeps.sleep(10)
        finally:
            self.stop()
        return 0

    async def execute(self, job):
        start_time = time.time()
        index_name = job.connector["index_name"]
        service_type = job.connector["service_type"]
        pipeline = PipelineSettings(job.connector.get("pipeline", {}))

        connector = await self.connectors.fetch_by_id(job.connector_id)

        fqn = self.config["sources"][service_type]
        configuration = DataSourceConfiguration(job.connector["configuration"])
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
                is_connectors_index=True,
                language_code=self.language_code
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
            await self._sync_done(job, connector, result, start_time)

        except Exception as e:
            await self._sync_done(job, connector, {}, start_time, exception=e)
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

    async def _sync_done(self, job, connector, result, start_time, exception=None):
        doc_updated = result.get("doc_updated", 0)
        doc_created = result.get("doc_created", 0)
        doc_deleted = result.get("doc_deleted", 0)
        exception = result.get("fetch_error", exception)

        indexed_count = doc_updated + doc_created

        await job.done(indexed_count, doc_deleted, exception)

        await connector.job_done(job)

        logger.info(
            f"Sync done: {indexed_count} indexed, {doc_deleted} "
            f" deleted. ({int(time.time() - start_time)} seconds)"
        )

    def stop(self):
        logger.debug("Shutting down consumers")
        self.running = False
        self._sleeps.cancel()
