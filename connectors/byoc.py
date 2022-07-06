#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Implementation of BYOC protocol.
"""
from datetime import datetime

from elasticsearch import AsyncElasticsearch
from crontab import CronTab

from connectors.logger import logger

CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"


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
    def __init__(connector_id, elastic_client):
        self.connector_id = connector_id
        self.client = elastic_client
        self.created_at = utc_now()
        self.job_id = None

    async def start(self):
        job_def = {
            "connector_id": self.connector_id,
            "status": "2",
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
            "status": "1",
            "deleted_document_count": indexed_count,
            "indexed_document_count": deleted_count,
            "updated_at": utc_now(),
        }
        await self.client.index(index=JOBS_INDEX, id=self.job_id, body=job_def)

    async def failed(self, exception):
        job_def = {
            "status": "3",
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

    async def sync_starts(self):
        # create a sync job
        job = SyncJob(self.doc_id, self.client)
        job_id = await job.start()

        # save connector state
        self.definition["sync_now"] = False
        self.definition["sync_status"] = "2"
        self.definition["last_seen"] = now
        await self._write()

        logger.info(f"Sync starts, Job id: {job_id}")
        return job

    async def sync_done(self, job, indexed_count, deleted_count):
        # update the sync job
        await job.done(indexed_count, deleted_count)

        now = utc_now()
        self.definition["sync_status"] = "1"
        self.definition["last_seen"] = self.definition["last_sync"] = now
        await self._write()
        logger.info(f"Sync done: {indexed_count} indexed, {deleted_count} deleted.")

    async def sync_failed(self, job, exception):
        # update the sync job
        await job.failed(exception)

        self.definition["sync_error"] = str(exception)
        self.definition["sync_status"] = "3"
        self.definition["last_seen"] = self.definition["last_sync"] = now
        await self._write()

    async def sync(self, data_provider, elastic_server, idling):
        service_type = self.service_type
        logger.debug(f"Syncing '{service_type}'")
        next_sync = self.next_sync()
        if next_sync == -1 or next_sync - idling > 0:
            logger.debug(f"Next sync due in {int(next_sync)} seconds")
            return

        job = await self.sync_starts()
        try:
            await data_provider.ping()
            await elastic_server.prepare_index(self.index_name)
            result = await elastic_server.async_bulk(
                self.index_name, data_provider.get_docs()
            )
            await self.sync_done(job, result.get("update", 0), result.get("delete", 0))
        except Exception as e:
            await self.sync_failed(job, e)
            raise
