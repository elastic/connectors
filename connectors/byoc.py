#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Implementation of BYOC protocol.
"""
from datetime import datetime
from enum import Enum

from elasticsearch import AsyncElasticsearch
from crontab import CronTab

from connectors.logger import logger

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


def utc_now():
    return datetime.utcnow().isoformat()


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
            yield BYOConnector(self, hit["_id"], hit["_source"])


class SyncJob:
    def __init__(self, connector_id, elastic_client):
        self.connector_id = connector_id
        self.client = elastic_client
        self.created_at = utc_now()
        self.job_id = None

    async def start(self):
        job_def = {
            "connector_id": self.connector_id,
            "status": e2str(JobStatus.IN_PROGRESS),
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
        job_def = {
            "status": e2str(JobStatus.COMPLETED),
            "deleted_document_count": indexed_count,
            "indexed_document_count": deleted_count,
            "updated_at": utc_now(),
        }
        await self.client.index(index=JOBS_INDEX, id=self.job_id, body=job_def)

    async def failed(self, exception):
        job_def = {
            "status": e2str(JobStatus.ERROR),
            "error": str(exception),
            "deleted_document_count": 0,
            "indexed_document_count": 0,
            "updated_at": utc_now(),
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

    async def _write(self):
        await self.connectors.save(self)

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
        self.definition["sync_status"] = "2"
        self.definition["last_seen"] = utc_now()
        await self._write()

        logger.info(f"Sync starts, Job id: {job_id}")
        return job

    async def _sync_done(self, job, indexed_count, deleted_count):
        await job.done(indexed_count, deleted_count)

        self.definition["sync_status"] = "1"
        self.definition["last_seen"] = self.definition["last_sync"] = utc_now()
        await self._write()

        logger.info(f"Sync done: {indexed_count} indexed, {deleted_count} deleted.")

    async def _sync_failed(self, job, exception):
        await job.failed(exception)

        self.definition["sync_error"] = str(exception)
        self.definition["sync_status"] = "3"
        self.definition["last_seen"] = self.definition["last_sync"] = utc_now()
        await self._write()

    async def sync(self, data_provider, elastic_server, idling):
        service_type = self.service_type
        logger.debug(f"Syncing '{service_type}'")
        next_sync = self.next_sync()
        if next_sync == -1 or next_sync - idling > 0:
            logger.debug(f"Next sync due in {int(next_sync)} seconds")
            return

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
