#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Implementation of BYOC protocol.
"""
import asyncio
from enum import Enum
import time
from datetime import datetime, timezone

from connectors.utils import (
    iso_utc,
    next_run,
    ESClient,
    DEFAULT_QUEUE_SIZE,
    DEFAULT_DISPLAY_EVERY,
    DEFAULT_QUEUE_MEM_SIZE,
    DEFAULT_CHUNK_MEM_SIZE,
)
from connectors.logger import logger
from connectors.source import DataSourceConfiguration
from elasticsearch.exceptions import ApiError
from connectors.index import defaults_for


CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"
PIPELINE = "ent-search-generic-ingestion"
SYNC_DISABLED = -1


def e2str(entry):
    return entry.name.lower()


class Status(Enum):
    CREATED = 1
    NEEDS_CONFIGURATION = 2
    CONFIGURED = 3
    CONNECTED = 4
    ERROR = 5


class JobStatus(Enum):
    PENDING = 1
    IN_PROGRESS = 2
    CANCELING = 3
    CANCELED = 4
    SUSPENDED = 5
    COMPLETED = 6
    ERROR = 7


CUSTOM_READ_ONLY_FIELDS = (
    "is_native",
    "api_key_id",
    "pipeline",
    "scheduling",
)

NATIVE_READ_ONLY_FIELDS = CUSTOM_READ_ONLY_FIELDS + (
    "service_type",
    "configuration",
)


class BYOIndex(ESClient):
    def __init__(self, elastic_config):
        super().__init__(elastic_config)
        logger.debug(f"BYOIndex connecting to {elastic_config['host']}")
        self.bulk_queue_max_size = elastic_config.get(
            "bulk_queue_max_size", DEFAULT_QUEUE_SIZE
        )
        self.bulk_display_every = elastic_config.get(
            "bulk_display_every", DEFAULT_DISPLAY_EVERY
        )
        self.bulk_queue_max_mem_size = elastic_config.get(
            "bulk_queue_max_mem_size", DEFAULT_QUEUE_MEM_SIZE
        )
        self.bulk_chunk_max_mem_size = elastic_config.get(
            "bulk_chunk_max_mem_size", DEFAULT_CHUNK_MEM_SIZE
        )

    async def save(self, connector):
        # we never update the configuration
        document = dict(connector.doc_source)

        # read only we never update
        for key in (
            NATIVE_READ_ONLY_FIELDS if connector.native else CUSTOM_READ_ONLY_FIELDS
        ):
            if key in document:
                del document[key]

        return await self.client.update(
            index=CONNECTORS_INDEX,
            id=connector.id,
            doc=document,
        )

    async def preflight(self):
        await self.check_exists(
            indices=[CONNECTORS_INDEX, JOBS_INDEX], pipelines=[PIPELINE]
        )

    async def get_list(self):
        await self.client.indices.refresh(index=CONNECTORS_INDEX)

        try:
            resp = await self.client.search(
                index=CONNECTORS_INDEX,
                query={"match_all": {}},
                size=20,
                expand_wildcards="hidden",
            )
        except ApiError as e:
            logger.critical(f"The server returned {e.status_code}")
            logger.critical(e.body, exc_info=True)
            return

        logger.debug(f"Found {len(resp['hits']['hits'])} connectors")
        for hit in resp["hits"]["hits"]:
            yield BYOConnector(
                self,
                hit["_id"],
                hit["_source"],
                bulk_queue_max_size=self.bulk_queue_max_size,
                bulk_display_every=self.bulk_display_every,
                bulk_queue_max_mem_size=self.bulk_queue_max_mem_size,
                bulk_chunk_max_mem_size=self.bulk_chunk_max_mem_size,
            )


class SyncJob:
    def __init__(self, connector_id, elastic_client):
        self.connector_id = connector_id
        self.client = elastic_client
        self.created_at = datetime.now(timezone.utc)
        self.completed_at = None
        self.job_id = None
        self.status = None

    @property
    def duration(self):
        if self.completed_at is None:
            return -1
        msec = (self.completed_at - self.created_at).microseconds
        return round(msec / 9, 2)

    async def start(self):
        self.status = JobStatus.IN_PROGRESS
        job_def = {
            "connector": {
                "id": self.connector_id,
            },
            "status": e2str(self.status),
            "error": None,
            "deleted_document_count": 0,
            "indexed_document_count": 0,
            "created_at": iso_utc(self.created_at),
            "completed_at": None,
        }
        resp = await self.client.index(index=JOBS_INDEX, document=job_def)
        self.job_id = resp["_id"]
        return self.job_id

    async def done(self, indexed_count=0, deleted_count=0, exception=None):
        self.completed_at = datetime.now(timezone.utc)

        job_def = {
            "deleted_document_count": deleted_count,
            "indexed_document_count": indexed_count,
            "completed_at": iso_utc(self.completed_at),
        }

        if exception is None:
            self.status = JobStatus.COMPLETED
            job_def["error"] = None
        else:
            self.status = JobStatus.ERROR
            job_def["error"] = str(exception)

        job_def["status"] = e2str(self.status)

        return await self.client.update(index=JOBS_INDEX, id=self.job_id, doc=job_def)


class PipelineSettings:
    def __init__(self, pipeline):
        self.name = pipeline.get("name", "ent-search-generic-ingestion")
        self.extract_binary_content = pipeline.get("extract_binary_content", True)
        self.reduce_whitespace = pipeline.get("reduce_whitespace", True)
        self.run_ml_inference = pipeline.get("run_ml_inference", True)

    def __repr__(self):
        return (
            f"Pipeline {self.name} <binary: {self.extract_binary_content}, "
            f"whitespace {self.reduce_whitespace}, ml inference {self.run_ml_inference}>"
        )


class BYOConnector:
    def __init__(
        self,
        index,
        connector_id,
        doc_source,
        bulk_queue_max_size=DEFAULT_QUEUE_SIZE,
        bulk_display_every=DEFAULT_DISPLAY_EVERY,
        bulk_queue_max_mem_size=DEFAULT_QUEUE_MEM_SIZE,
        bulk_chunk_max_mem_size=DEFAULT_CHUNK_MEM_SIZE,
    ):
        self.doc_source = doc_source
        self.id = connector_id
        self.index = index
        self._update_config(doc_source)
        self._dirty = False
        self.client = index.client
        self.doc_source["last_seen"] = iso_utc()
        self._heartbeat_started = self._syncing = False
        self._closed = False
        self._start_time = None
        self._hb = None
        self.bulk_queue_max_size = bulk_queue_max_size
        self.bulk_display_every = bulk_display_every
        self.bulk_queue_max_mem_size = bulk_queue_max_mem_size
        self.bulk_chunk_max_mem_size = bulk_chunk_max_mem_size

    def _update_config(self, doc_source):
        self.status = doc_source["status"]
        self.sync_now = doc_source.get("sync_now", False)
        self.native = doc_source.get("is_native", False)
        self._service_type = doc_source["service_type"]
        self.index_name = doc_source["index_name"]
        self._configuration = DataSourceConfiguration(doc_source["configuration"])
        self.scheduling = doc_source["scheduling"]
        self.pipeline = PipelineSettings(doc_source.get("pipeline", {}))
        self._dirty = True

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if isinstance(value, str):
            value = Status[value.upper()]
        if not isinstance(value, Status):
            raise TypeError(value)

        self._status = value
        self.doc_source["status"] = e2str(self._status)

    @property
    def service_type(self):
        return self._service_type

    @service_type.setter
    def service_type(self, value):
        self._service_type = self.doc_source["service_type"] = value
        self._update_config(self.doc_source)

    @property
    def configuration(self):
        return self._configuration

    @configuration.setter
    def configuration(self, value):
        self.doc_source["configuration"] = value
        status = (
            Status.CONFIGURED
            if all(
                isinstance(value, dict) and value.get("value") is not None
                for value in value.values()
            )
            else Status.NEEDS_CONFIGURATION
        )
        self.doc_source["status"] = e2str(status)
        self._update_config(self.doc_source)

    async def close(self):
        self._closed = True
        if self._heartbeat_started:
            self._hb.cancel()
            self._heartbeat_started = False

    async def sync_doc(self, force=True):
        if not self._dirty and not force:
            return
        self.doc_source["last_seen"] = iso_utc()
        await self.index.save(self)
        self._dirty = False

    def start_heartbeat(self, delay):
        if self._heartbeat_started:
            return
        self._heartbeat_started = True

        async def _heartbeat():
            while not self._closed:
                logger.info(f"*** Connector {self.id} HEARTBEAT")
                if not self._syncing:
                    self.doc_source["last_seen"] = iso_utc()
                    await self.sync_doc()
                await asyncio.sleep(delay)

        self._hb = asyncio.create_task(_heartbeat())

    def next_sync(self):
        """Returns in seconds when the next sync should happen.

        If the function returns SYNC_DISABLED, no sync is scheduled.
        """
        if self.sync_now:
            logger.debug("sync_now is true, syncing!")
            return 0
        if not self.scheduling["enabled"]:
            logger.debug("scheduler is disabled")
            return SYNC_DISABLED
        return next_run(self.scheduling["interval"])

    async def _sync_starts(self):
        job = SyncJob(self.id, self.client)
        job_id = await job.start()

        self.sync_now = self.doc_source["sync_now"] = False
        self.doc_source["last_sync_status"] = e2str(job.status)
        self.status = Status.CONNECTED
        await self.sync_doc()

        self._start_time = time.time()
        logger.info(f"Sync starts, Job id: {job_id}")
        return job

    async def error(self, error):
        self.doc_source["error"] = str(error)
        await self.sync_doc()

    async def _sync_done(self, job, result, exception=None):
        doc_updated = result.get("doc_updated", 0)
        doc_created = result.get("doc_created", 0)
        doc_deleted = result.get("doc_deleted", 0)
        exception = result.get("fetch_error", exception)

        indexed_count = doc_updated + doc_created

        await job.done(indexed_count, doc_deleted, exception)

        self.doc_source["last_sync_status"] = e2str(job.status)
        if exception is None:
            self.doc_source["last_sync_error"] = None
            self.doc_source["error"] = None
        else:
            self.doc_source["last_sync_error"] = str(exception)
            self.doc_source["error"] = str(exception)
            self.status = Status.ERROR

        self.doc_source["last_synced"] = iso_utc()
        await self.sync_doc()
        logger.info(
            f"Sync done: {indexed_count} indexed, {doc_deleted} "
            f" deleted. ({int(time.time() - self._start_time)} seconds)"
        )

    async def prepare_docs(self, data_provider):
        logger.debug(f"Using pipeline {self.pipeline}")

        async for doc, lazy_download in data_provider.get_docs():
            # adapt doc for pipeline settings
            doc["_extract_binary_content"] = self.pipeline.extract_binary_content
            doc["_reduce_whitespace"] = self.pipeline.reduce_whitespace
            doc["_run_ml_inference"] = self.pipeline.run_ml_inference
            yield doc, lazy_download

    async def sync(self, data_provider, elastic_server, idling, sync_now=False):
        # If anything bad happens before we create a sync job
        # (like bad scheduling config, etc)
        #
        # we will raise the error in the logs here and let Kibana knows
        # by toggling the status and setting the error and status field
        try:
            service_type = self.service_type
            if not sync_now:
                next_sync = self.next_sync()
                if next_sync == SYNC_DISABLED or next_sync - idling > 0:
                    if next_sync == SYNC_DISABLED:
                        logger.debug(f"Scheduling is disabled for {service_type}")
                    else:
                        logger.debug(
                            f"Next sync for {service_type} due in {int(next_sync)} seconds"
                        )
                    # if we don't sync, we still want to make sure we tell kibana we are connected
                    # if the status is different from comnected
                    if self.status != Status.CONNECTED:
                        self.status = Status.CONNECTED
                        await self.sync_doc()
                    return
            else:
                logger.info("Sync forced")

            if not await data_provider.changed():
                logger.debug(f"No change in {service_type} data provider, skipping...")
                return
        except Exception as exc:
            self.doc_source["error"] = str(exc)
            self.status = Status.ERROR
            await self.sync_doc()
            raise

        logger.debug(f"Syncing '{service_type}'")
        self._syncing = True
        job = await self._sync_starts()
        try:
            logger.debug(f"Pinging the {data_provider} backend")
            await data_provider.ping()
            await asyncio.sleep(0)

            # TODO: where do we get language_code and analysis_icu?
            mappings, settings = defaults_for(is_connectors_index=True)
            logger.debug("Preparing the index")
            await elastic_server.prepare_index(
                self.index_name, mappings=mappings, settings=settings
            )
            await asyncio.sleep(0)

            result = await elastic_server.async_bulk(
                self.index_name,
                self.prepare_docs(data_provider),
                data_provider.connector.pipeline,
                queue_size=self.bulk_queue_max_size,
                display_every=self.bulk_display_every,
                queue_mem_size=self.bulk_queue_max_mem_size,
                chunk_mem_size=self.bulk_chunk_max_mem_size,
            )
            await self._sync_done(job, result)

        except Exception as e:
            await self._sync_done(job, {}, exception=e)
            raise
        finally:
            self._syncing = False
            self._start_time = None
