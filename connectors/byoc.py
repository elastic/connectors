#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Implementation of BYOC protocol.
"""
import asyncio
from datetime import datetime
from enum import Enum

from elasticsearch import AsyncElasticsearch
from crontab import CronTab

from connectors.logger import logger

CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"
HEARTBEAT_DELAY = 300


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


def iso_utc(when=None):
    if when is None:
        when = datetime.utcnow()
    return when.isoformat()


_CONNECTORS_CACHE = {}


class BYOConnectors:
    def __init__(self, config):
        logger.debug(f"BYOConnectors connecting to {config['host']}")
        self.host = config["host"]
        self.auth = config["user"], config["password"]
        self.client = AsyncElasticsearch(hosts=[self.host], basic_auth=self.auth)

    async def close(self):
        await self.client.close()

    async def save(self, connector):
        return await self.client.index(
            index=CONNECTORS_INDEX, id=connector.doc_id, body=dict(connector.definition)
        )

    async def get_list(self):
        resp = await self.client.search(
            index=CONNECTORS_INDEX,
            body={"query": {"match_all": {}}},
            size=20,
            expand_wildcards="hidden",
        )
        for hit in resp["hits"]["hits"]:
            doc_id = hit["_id"]
            if doc_id not in _CONNECTORS_CACHE:
                _CONNECTORS_CACHE[doc_id] = BYOConnector(self, doc_id, hit["_source"])
            else:
                # XXX Need to check and update
                pass

            yield _CONNECTORS_CACHE[doc_id]


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
        resp = await self.client.index(index=JOBS_INDEX, body=job_def)
        self.job_id = resp["_id"]
        return self.job_id

    async def done(self, indexed_count, deleted_count):
        self.status = JobStatus.COMPLETED
        job_def = {
            "status": e2str(self.status),
            "deleted_document_count": indexed_count,
            "indexed_document_count": deleted_count,
            "updated_at": iso_utc(),
        }
        await self.client.index(index=JOBS_INDEX, id=self.job_id, body=job_def)

    async def failed(self, exception):
        self.status = JobStatus.ERROR
        job_def = {
            "status": e2str(self.status),
            "error": str(exception),
            "deleted_document_count": 0,
            "indexed_document_count": 0,
            "updated_at": iso_utc(),
        }
        await self.client.index(index=JOBS_INDEX, id=self.job_id, body=job_def)


class BYOConnector:
    def __init__(self, connectors, doc_id, definition):
        self.definition = definition
        self.doc_id = doc_id
        self.service_type = definition["service_type"]
        self.index_name = definition["index_name"]
        self.configuration = {}
        for key, value in definition["configuration"].items():
            self.configuration[key] = value["value"]
        self.scheduling = definition["scheduling"]
        self.connectors = connectors
        self.client = connectors.client
        self.definition["status"] = e2str(Status.CONNECTED)
        self.definition["last_seen"] = iso_utc()
        self._heartbeat_started = self._syncing = False

    async def _write(self):
        self.definition["last_seen"] = iso_utc()
        await self.connectors.save(self)

    async def heartbeat(self):
        if self._heartbeat_started:
            return
        self._heartbeat_started = True
        while True:
            logger.debug("*** BEAT")
            if not self._syncing:
                self.definition["last_seen"] = iso_utc()
                await self._write()
            await asyncio.sleep(HEARTBEAT_DELAY)

    def next_sync(self):
        """Returns in seconds when the next sync should happen.

        If the function returns -1, no sync is scheduled.
        """
        if self.definition["sync_now"]:
            return 0
        if not self.scheduling["enabled"]:
            return -1
        return CronTab(self.scheduling["interval"]).next(default_utc=True)

    async def _sync_starts(self):
        job = SyncJob(self.doc_id, self.client)
        job_id = await job.start()

        self.definition["sync_now"] = False
        self.definition["last_sync_status"] = e2str(job.status)
        await self._write()

        logger.info(f"Sync starts, Job id: {job_id}")
        return job

    async def _sync_done(self, job, indexed_count, deleted_count):
        await job.done(indexed_count, deleted_count)

        self.definition["sync_status"] = e2str(job.status)
        self.definition["last_sync"] = iso_utc()
        await self._write()

        logger.info(f"Sync done: {indexed_count} indexed, {deleted_count} deleted.")

    async def _sync_failed(self, job, exception):
        await job.failed(exception)

        self.definition["last_sync_error"] = str(exception)
        self.definition["last_sync_status"] = e2str(job.status)
        self.definition["last_sync"] = iso_utc()
        await self._write()

    async def sync(self, data_provider, elastic_server, idling):
        service_type = self.service_type
        logger.debug(f"Syncing '{service_type}'")
        next_sync = self.next_sync()
        if next_sync == -1 or next_sync - idling > 0:
            logger.debug(f"Next sync due in {int(next_sync)} seconds")
            return

        self._syncing = True
        job = await self._sync_starts()
        try:
            await data_provider.ping()
            await elastic_server.prepare_index(self.index_name)
            result = await elastic_server.async_bulk(
                self.index_name, data_provider.get_docs()
            )
            await self._sync_done(job, result.get("update", 0), result.get("delete", 0))
        except Exception as e:
            await self._sync_failed(job, e)
            raise
        finally:
            self._syncing = False
