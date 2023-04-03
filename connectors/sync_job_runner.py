#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import time

from connectors.byoc import JobStatus
from connectors.byoei import ElasticServer
from connectors.es import Mappings
from connectors.es.index import DocumentNotFoundError
from connectors.logger import logger

JOB_REPORTING_INTERVAL = 10
JOB_CHECK_INTERVAL = 1
ES_ID_SIZE_LIMIT = 512


class SyncJobRunningError(Exception):
    pass


class JobClaimError(Exception):
    pass


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
        self.job_id = self.sync_job.id
        self.connector = connector
        self.connector_id = self.connector.id
        self.es_config = es_config
        self.elastic_server = None
        self.job_reporting_task = None
        self.bulk_options = self.es_config.get("bulk", {})
        self._start_time = None
        self.running = False

    async def execute(self):
        if self.running:
            raise SyncJobRunningError(f"Sync job {self.job_id} is already running.")

        self.running = True
        if not await self._claim_job():
            logger.error(
                f"Unable to claim job {self.job_id} for connector {self.connector_id}"
            )
            raise JobClaimError

        try:
            self.data_provider = self.source_klass(self.sync_job.configuration)
            if not await self.data_provider.changed():
                logger.debug(
                    f"No change in {self.sync_job.service_type} data provider, skipping..."
                )
                await self._sync_done(sync_status=JobStatus.COMPLETED)
                return

            logger.debug(f"Validating configuration for {self.data_provider}")
            await self.data_provider.validate_config()

            logger.debug(
                f"Syncing '{self.sync_job.service_type}' for connector '{self.connector_id}'"
            )
            logger.debug(f"Pinging the {self.source_klass} backend")
            await self.data_provider.ping()

            sync_rules_enabled = self.connector.features.sync_rules_enabled()
            if sync_rules_enabled:
                await self.sync_job.validate_filtering(validator=self.data_provider)

            mappings = Mappings.default_text_fields_mappings(
                is_connectors_index=True,
            )

            self.elastic_server = ElasticServer(self.es_config)

            logger.debug("Preparing the content index")
            await self.elastic_server.prepare_content_index(
                self.sync_job.index_name, mappings=mappings
            )

            # allows the data provider to change the bulk options
            bulk_options = self.bulk_options.copy()
            self.data_provider.tweak_bulk_options(bulk_options)

            await self.elastic_server.async_bulk(
                self.sync_job.index_name,
                self.prepare_docs(),
                self.sync_job.pipeline,
                filter_=self.sync_job.filtering,
                sync_rules_enabled=sync_rules_enabled,
                content_extraction_enabled=self.sync_job.pipeline[
                    "extract_binary_content"
                ],
                options=bulk_options,
            )

            self.job_reporting_task = asyncio.create_task(
                self.update_ingestion_stats(JOB_REPORTING_INTERVAL)
            )
            while not self.elastic_server.done():
                await asyncio.sleep(JOB_CHECK_INTERVAL)
            fetch_error = self.elastic_server.fetch_error()
            sync_status = (
                JobStatus.COMPLETED if fetch_error is None else JobStatus.ERROR
            )
            await self._sync_done(sync_status=sync_status, sync_error=fetch_error)
        except asyncio.CancelledError:
            await self._sync_done(sync_status=JobStatus.SUSPENDED)
        except Exception as e:
            await self._sync_done(sync_status=JobStatus.ERROR, sync_error=e)
        finally:
            self.running = False
            if self.elastic_server is not None:
                await self.elastic_server.close()
            if self.data_provider is not None:
                await self.data_provider.close()

    async def _sync_done(self, sync_status, sync_error=None):
        if self.elastic_server is not None and not self.elastic_server.done():
            await self.elastic_server.cancel()
        if self.job_reporting_task is not None and not self.job_reporting_task.done():
            self.job_reporting_task.cancel()
            try:
                await self.job_reporting_task
            except asyncio.CancelledError:
                logger.info("Job reporting task is stopped.")

        result = (
            {} if self.elastic_server is None else self.elastic_server.ingestion_stats()
        )
        ingestion_stats = {
            "indexed_document_count": result.get("indexed_document_count", 0),
            "indexed_document_volume": result.get("indexed_document_volume", 0),
            "deleted_document_count": result.get("deleted_document_count", 0),
            "total_document_count": await self.connector.document_count(),
        }

        if sync_status == JobStatus.ERROR:
            await self.sync_job.fail(sync_error, ingestion_stats=ingestion_stats)
        elif sync_status == JobStatus.SUSPENDED:
            await self.sync_job.suspend(ingestion_stats=ingestion_stats)
        elif sync_status == JobStatus.CANCELED:
            await self.sync_job.cancel(ingestion_stats=ingestion_stats)
        else:
            await self.sync_job.done(ingestion_stats=ingestion_stats)

        try:
            await self.sync_job.reload()
        except DocumentNotFoundError as e:
            logger.error(f"Failed to reload sync job {self.job_id}. Error: {e}")
            self.sync_job = None
        await self.connector.sync_done(self.sync_job)
        logger.info(
            f"[{self.job_id}] Sync done: {ingestion_stats.get('indexed_document_count')} indexed, "
            f"{ingestion_stats.get('deleted_document_count')} deleted. "
            f"({int(time.time() - self._start_time)} seconds)"  # pyright: ignore
        )

    async def _claim_job(self):
        try:
            await self.sync_job.claim()
            await self.connector.sync_starts()
            self._start_time = time.time()
            return True
        except Exception as e:
            logger.critical(e, exc_info=True)
            return False

    async def prepare_docs(self):
        logger.debug(f"Using pipeline {self.sync_job.pipeline}")

        async for doc, lazy_download in self.data_provider.get_docs(
            filtering=self.sync_job.filtering
        ):
            doc_id = str(doc.get("_id", ""))
            doc_id_size = len(doc_id.encode("utf-8"))

            if doc_id_size > ES_ID_SIZE_LIMIT:
                logger.error(
                    f"Document with id '{doc_id}' with a size of '{doc_id_size}' bytes could not be ingested. "
                    f"Elasticsearch has an upper limit of '{ES_ID_SIZE_LIMIT}' bytes for the '_id' field."
                )
                continue

            # adapt doc for pipeline settings
            doc["_extract_binary_content"] = self.sync_job.pipeline[
                "extract_binary_content"
            ]
            doc["_reduce_whitespace"] = self.sync_job.pipeline["reduce_whitespace"]
            doc["_run_ml_inference"] = self.sync_job.pipeline["run_ml_inference"]
            yield doc, lazy_download

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

    async def reload_sync_job(self):
        if self.sync_job is None:
            return False
        try:
            await self.sync_job.reload()
        except DocumentNotFoundError:
            logger.error(f"Couldn't find sync job by id {self.job_id}")
            self.sync_job = None
        return self.sync_job is not None
