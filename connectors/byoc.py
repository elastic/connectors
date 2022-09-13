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

from connectors.utils import iso_utc, next_run, ESClient
from connectors.logger import logger
from connectors.source import DataSourceConfiguration
from elasticsearch.exceptions import ApiError
from connectors.index import defaults_for


CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"


def e2str(entry):
    return entry.name.lower()


class Status(Enum):
    CREATED = 1
    NEEDS_CONFIGURATION = 2
    CONFIGURED = 3
    CONNECTED = 4
    ERROR = 5


class JobStatus(Enum):
    NULL = 1
    IN_PROGRESS = 2
    COMPLETED = 3
    ERROR = 4


_CONNECTORS_CACHE = {}


async def purge_cache():
    for connector in _CONNECTORS_CACHE.values():
        try:
            await connector.close()
        except Exception as e:
            logger.critical(e, exc_info=True)
        await asyncio.sleep(0)
    _CONNECTORS_CACHE.clear()


class BYOIndex(ESClient):
    def __init__(self, elastic_config):
        super().__init__(elastic_config)
        logger.debug(f"BYOIndex connecting to {elastic_config['host']}")
        self.bulk_queue_max_size = elastic_config.get("bulk_queue_max_size", 1024)

    async def close(self):
        await purge_cache()
        await super().close()

    async def save(self, connector):
        # we never update the configuration
        document = dict(connector.doc_source)

        # read only we never update
        for key in "api_key_id", "pipeline", "scheduling", "configuration":
            if key in document:
                del document[key]

        return await self.client.update(
            index=CONNECTORS_INDEX,
            id=connector.id,
            doc=document,
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
            connector_id = hit["_id"]
            if connector_id not in _CONNECTORS_CACHE:
                logger.debug(
                    f"=> id: {connector_id}, service_type: {hit['_source']['service_type']}"
                )
                _CONNECTORS_CACHE[connector_id] = BYOConnector(
                    self,
                    connector_id,
                    hit["_source"],
                    bulk_queue_max_size=self.bulk_queue_max_size,
                )
            else:
                _CONNECTORS_CACHE[connector_id].update_config(hit["_source"])

            yield _CONNECTORS_CACHE[connector_id]


class SyncJob:
    def __init__(self, connector_id, elastic_client):
        self.connector_id = connector_id
        self.client = elastic_client
        self.created_at = iso_utc()
        self.job_id = None
        self.status = None

    async def start(self):
        self.status = JobStatus.IN_PROGRESS
        job_def = {
            "connector_id": self.connector_id,
            "status": e2str(self.status),
            "error": "",
            "deleted_document_count": 0,
            "indexed_document_count": 0,
            "created_at": self.created_at,
            "updated_at": self.created_at,
        }
        resp = await self.client.index(index=JOBS_INDEX, document=job_def)
        self.job_id = resp["_id"]
        return self.job_id

    async def done(self, indexed_count=0, deleted_count=0, exception=None):
        job_def = {
            "deleted_document_count": indexed_count,
            "indexed_document_count": deleted_count,
            "updated_at": iso_utc(),
        }

        if exception is None:
            self.status = JobStatus.COMPLETED
        else:
            self.status = JobStatus.ERROR
            job_def["error"] = str(exception)

        job_def["status"] = e2str(self.status)

        return await self.client.index(
            index=JOBS_INDEX, id=self.job_id, document=job_def
        )


class BYOConnector:
    def __init__(self, index, connector_id, doc_source, bulk_queue_max_size=1024):
        self.doc_source = doc_source
        self.id = connector_id
        self.index = index
        self.update_config(doc_source)
        self.client = index.client
        self.doc_source["status"] = e2str(Status.CONNECTED)
        self.doc_source["last_seen"] = iso_utc()
        self._heartbeat_started = self._syncing = False
        self._closed = False
        self._start_time = None
        self._hb = None
        self.bulk_queue_max_size = bulk_queue_max_size

    def update_config(self, doc_source):
        self.native = doc_source.get("is_native", False)
        self.service_type = doc_source["service_type"]
        self.index_name = doc_source["index_name"]
        self.configuration = DataSourceConfiguration(doc_source["configuration"])
        self.scheduling = doc_source["scheduling"]

    async def close(self):
        self._closed = True
        if self._heartbeat_started:
            self._hb.cancel()
            self._heartbeat_started = False

    async def is_configured(self):
        if self.doc_source["status"] == e2str(Status.CONFIGURED):
            return
        self.doc_source["status"] = e2str(Status.CONFIGURED)
        await self._write()

    async def _write(self):
        self.doc_source["last_seen"] = iso_utc()
        await self.index.save(self)

    async def heartbeat(self, delay):
        if self._heartbeat_started:
            return
        self._heartbeat_started = True

        async def _heartbeat():
            while not self._closed:
                logger.debug(f"*** BEAT every {delay} seconds")
                if not self._syncing:
                    self.doc_source["last_seen"] = iso_utc()
                    await self._write()
                await asyncio.sleep(delay)

        self._hb = asyncio.create_task(_heartbeat())

    def next_sync(self):
        """Returns in seconds when the next sync should happen.

        If the function returns -1, no sync is scheduled.
        """
        if self.doc_source["sync_now"]:
            return 0
        if not self.scheduling["enabled"]:
            return -1
        return next_run(self.scheduling["interval"])

    async def _sync_starts(self):
        job = SyncJob(self.id, self.client)
        job_id = await job.start()

        self.doc_source["sync_now"] = False
        self.doc_source["last_sync_status"] = e2str(job.status)
        await self._write()

        self._start_time = time.time()
        logger.info(f"Sync starts, Job id: {job_id}")
        return job

    async def _sync_done(self, job, result, exception=None):
        doc_updated = result.get("doc_updated", 0)
        doc_created = result.get("doc_created", 0)
        doc_deleted = result.get("doc_deleted", 0)
        exception = result.get("fetch_error", exception)

        indexed_count = doc_updated + doc_created

        await job.done(indexed_count, doc_deleted, exception)

        self.doc_source["last_sync_status"] = e2str(job.status)
        if exception is None:
            self.doc_source["last_sync_error"] = ""
        else:
            self.doc_source["last_sync_error"] = str(exception)
        self.doc_source["last_synced"] = iso_utc()
        await self._write()
        logger.info(
            f"Sync done: {indexed_count} indexed, {doc_deleted} "
            f" deleted. ({int(time.time() - self._start_time)} seconds)"
        )

    async def sync(self, data_provider, elastic_server, idling, sync_now=False):
        service_type = self.service_type
        if not sync_now:
            next_sync = self.next_sync()
            if next_sync == -1 or next_sync - idling > 0:
                logger.debug(
                    f"Next sync for {service_type} due in {int(next_sync)} seconds"
                )
                return
        else:
            logger.info("Sync forced")

        if not await data_provider.changed():
            logger.debug(f"No change in {service_type} data provider, skipping...")
            return

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
                data_provider.get_docs(),
                queue_size=self.bulk_queue_max_size,
            )
            await self._sync_done(job, result)

        except Exception as e:
            await self._sync_done(job, {}, exception=e)
            raise
        finally:
            self._syncing = False
            self._start_time = None
