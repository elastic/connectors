#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import time

from connectors.byoc import JobStatus
from connectors.es import Mappings
from connectors.logger import logger


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
        - `elastic_server`: The sync orchestrator used to fetch data from 3rd-party source and ingest into Elasticsearch
        - `bulk_options`: The bulk options used for the ingestion

    """

    def __init__(
        self,
        source_klass,
        sync_job,
        connector,
        elastic_server,
        bulk_options,
    ):
        self.source_klass = source_klass
        self.data_provider = None
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
                await self._sync_done(sync_status=JobStatus.COMPLETED, result={})
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

            logger.debug("Preparing the content index")
            await self.elastic_server.prepare_content_index(
                self.sync_job.index_name, mappings=mappings
            )

            # allows the data provider to change the bulk options
            bulk_options = self.bulk_options.copy()
            self.data_provider.tweak_bulk_options(bulk_options)

            result = await self.elastic_server.async_bulk(
                self.sync_job.index_name,
                self.prepare_docs(),
                self.sync_job.pipeline,
                filter_=self.sync_job.filtering,
                sync_rules_enabled=sync_rules_enabled,
                options=bulk_options,
            )
            sync_error = result.get("fetch_error")
            sync_status = JobStatus.COMPLETED if sync_error is None else JobStatus.ERROR
            await self._sync_done(
                sync_status=sync_status, result=result, sync_error=sync_error
            )
        except asyncio.CancelledError:
            await self._sync_done(sync_status=JobStatus.SUSPENDED, result={})
        except Exception as e:
            await self._sync_done(sync_status=JobStatus.ERROR, result={}, sync_error=e)
        finally:
            if self.data_provider is not None:
                await self.data_provider.close()

    async def _sync_done(self, sync_status, result=None, sync_error=None):
        if result is None:
            result = {}
        doc_updated = result.get("doc_updated", 0)
        doc_created = result.get("doc_created", 0)
        doc_deleted = result.get("doc_deleted", 0)
        indexed_count = doc_updated + doc_created

        ingestion_stats = {
            "indexed_document_count": indexed_count,
            "indexed_document_volume": 0,
            "deleted_document_count": doc_deleted,
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

        self.sync_job = await self.sync_job.reload()
        await self.connector.sync_done(self.sync_job)
        logger.info(
            f"[{self.job_id}] Sync done: {indexed_count} indexed, {doc_deleted} "
            f" deleted. ({int(time.time() - self._start_time)} seconds)"  # pyright: ignore
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
            # adapt doc for pipeline settings
            doc["_extract_binary_content"] = self.sync_job.pipeline[
                "extract_binary_content"
            ]
            doc["_reduce_whitespace"] = self.sync_job.pipeline["reduce_whitespace"]
            doc["_run_ml_inference"] = self.sync_job.pipeline["run_ml_inference"]
            yield doc, lazy_download
