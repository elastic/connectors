#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Connector definition
"""
from datetime import datetime

from elasticsearch import AsyncElasticsearch
from crontab import CronTab

from connectors.logger import logger

CONNECTORS_INDEX = ".elastic-connectors"


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

    def utc_now(self):
        return datetime.utcnow().isoformat()

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
        self.definition["sync_now"] = False
        self.definition["sync_status"] = "2"
        self.definition["last_seen"] = self.utc_now()
        await self._write()

    async def sync_done(self):
        self.definition["sync_status"] = "1"
        self.definition["last_seen"] = self.definition["last_sync"] = self.utc_now()
        await self._write()

    async def sync_failed(self, exception):
        self.definition["sync_error"] = str(exception)
        self.definition["sync_status"] = "3"
        self.definition["last_seen"] = self.definition["last_sync"] = self.utc_now()
        await self._write()

    async def sync(self, data_provider, elastic_server, idling):
        service_type = self.service_type
        logger.debug(f"Syncing '{service_type}'")
        next_sync = self.next_sync()
        if next_sync == -1 or next_sync - idling > 0:
            logger.debug(f"Next sync due in {next_sync} seconds")
            return

        await self.sync_starts()
        try:
            await data_provider.ping()
            await elastic_server.prepare_index(self.index_name)
            result = await elastic_server.async_bulk(
                index_name, data_provider.get_docs()
            )
            logger.info(result)
            await self.sync_done()
        except Exception as e:
            await self.sync_failed(e)
            raise
