#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import json
import os
from datetime import datetime

import pytest
from aioresponses import CallbackResult
from elasticsearch import AsyncElasticsearch
from envyaml import EnvYAML

from connectors.byoc import (
    BYOConnector,
    BYOIndex,
    JobStatus,
    Status,
    SyncJob,
    e2str,
    iso_utc,
)
from connectors.byoei import ElasticServer
from connectors.source import BaseDataSource

CONFIG = os.path.join(os.path.dirname(__file__), "config.yml")


def test_e2str():
    # The BYOC protocol uses lower case
    assert e2str(Status.NEEDS_CONFIGURATION) == "needs_configuration"


def test_utc():
    # All dates are in ISO 8601 UTC so we can serialize them
    now = datetime.utcnow()
    then = json.loads(json.dumps({"date": iso_utc(when=now)}))["date"]
    assert now.isoformat() == then


@pytest.mark.asyncio
async def test_sync_job(mock_responses):
    client = AsyncElasticsearch(hosts=["http://nowhere.com:9200"])

    job = SyncJob("connector-id", client)

    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )

    sent_docs = []

    def callback(url, **kwargs):
        sent_docs.append(json.loads(kwargs["data"]))
        return CallbackResult(
            body=json.dumps({"_id": "1"}), status=200, headers=headers
        )

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors-sync-jobs/_doc",
        callback=callback,
        headers=headers,
    )

    mock_responses.put(
        "http://nowhere.com:9200/.elastic-connectors-sync-jobs/_doc/1",
        callback=callback,
        headers=headers,
    )
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors-sync-jobs/_update/1",
        callback=callback,
        repeat=True,
    )

    assert job.duration == -1
    await job.start()
    assert job.status == JobStatus.IN_PROGRESS
    assert job.job_id is not None
    await asyncio.sleep(0.2)
    await job.done(12, 34)
    assert job.status == JobStatus.COMPLETED
    await client.close()
    assert job.duration >= 0.2

    # verify what was sent
    assert len(sent_docs) == 2
    doc, update = sent_docs
    assert doc["status"] == "in_progress"
    assert update["doc"]["status"] == "completed"
    assert update["doc"]["indexed_document_count"] == 12
    assert update["doc"]["deleted_document_count"] == 34


mongo = {
    "api_key_id": "",
    "configuration": {
        "host": {"value": "mongodb://127.0.0.1:27021", "label": "MongoDB Host"},
        "database": {"value": "sample_airbnb", "label": "MongoDB Database"},
        "collection": {
            "value": "listingsAndReviews",
            "label": "MongoDB Collection",
        },
    },
    "index_name": "search-airbnb",
    "service_type": "mongodb",
    "status": "configured",
    "last_sync_status": "null",
    "last_sync_error": "",
    "last_synced": "",
    "last_seen": "",
    "created_at": "",
    "updated_at": "",
    "scheduling": {"enabled": True, "interval": "0 * * * *"},
    "sync_now": True,
}


@pytest.mark.asyncio
async def test_heartbeat(mock_responses, patch_logger):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={
            "hits": {"hits": [{"_id": "1", "_source": mongo}], "total": {"value": 1}}
        },
        headers=headers,
    )

    for i in range(10):
        mock_responses.put(
            "http://nowhere.com:9200/.elastic-connectors/_doc/1",
            payload={"_id": "1"},
            headers=headers,
        )

    connectors = BYOIndex(config)
    conns = []

    query = connectors.build_docs_query([["mongodb"]])
    async for connector in connectors.get_all_docs(query=query):
        connector.start_heartbeat(0.2)
        connector.start_heartbeat(1.0)  # NO-OP
        conns.append(connector)

    await asyncio.sleep(0.4)
    await conns[0].close()
    await connectors.close()


@pytest.mark.asyncio
async def test_connectors_get_list(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={
            "hits": {"hits": [{"_id": "1", "_source": mongo}], "total": {"value": 1}}
        },
        headers=headers,
    )

    connectors = BYOIndex(config)
    conns = []
    query = connectors.build_docs_query([["mongodb"]])
    async for connector in connectors.get_all_docs(query=query):
        conns.append(connector)

    assert len(conns) == 1
    await connectors.close()


class StubIndex:
    def __init__(self):
        self.client = None

    async def save(self, connector):
        pass


doc = {"_id": 1}


class Data:
    def __init__(self, connector):
        self.connector = connector

    async def ping(self):
        pass

    async def changed(self):
        return True

    async def get_docs(self, *args, **kw):
        for d in [doc, doc]:
            yield {"_id": 1}, None

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_sync_mongo(mock_responses, patch_logger):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={
            "hits": {"hits": [{"_id": "1", "_source": mongo}], "total": {"value": 1}}
        },
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/.elastic-connectors/_doc/1",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_update/1",
        headers=headers,
        repeat=True,
    )
    mock_responses.put(
        "http://nowhere.com:9200/.elastic-connectors/_doc/1",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors-sync-jobs/_doc",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors-sync-jobs/_update/1",
        headers=headers,
        repeat=True,
    )
    mock_responses.put(
        "http://nowhere.com:9200/.elastic-connectors-sync-jobs/_doc/1",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.head(
        "http://nowhere.com:9200/search-airbnb?expand_wildcards=open",
        headers=headers,
        repeat=True,
    )
    mock_responses.get(
        "http://nowhere.com:9200/search-airbnb/_mapping?expand_wildcards=open",
        payload={"search-airbnb": {"mappings": {}}},
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-airbnb/_mapping?expand_wildcards=open",
        headers=headers,
    )
    mock_responses.get(
        "http://nowhere.com:9200/search-airbnb",
        payload={"hits": {"hits": [{"_id": "1", "_source": mongo}]}},
        headers=headers,
    )
    mock_responses.get(
        "http://nowhere.com:9200/search-airbnb/_search?scroll=5m",
        payload={"hits": {"hits": [{"_id": "1", "_source": mongo}]}},
        headers=headers,
    )
    mock_responses.post(
        "http://nowhere.com:9200/search-airbnb/_search?scroll=5m",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-airbnb/_search?scroll=5m",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/_bulk?pipeline=ent-search-generic-ingestion",
        payload={"items": []},
        headers=headers,
    )

    es = ElasticServer(config)
    connectors = BYOIndex(config)
    service_config = {"sources": {"mongodb": "connectors.tests.test_byoc:Data"}}

    try:
        query = connectors.build_docs_query([["mongodb"]])
        async for connector in connectors.get_all_docs(query=query):
            await connector.prepare(service_config)
            await connector.sync(es, 0)
            await connector.close()
    finally:
        await connectors.close()
        await es.close()


@pytest.mark.asyncio
async def test_properties(mock_responses):

    connector_src = {
        "service_type": "test",
        "index_name": "search-some-index",
        "configuration": {},
        "scheduling": {},
        "status": "created",
    }

    connector = BYOConnector(StubIndex(), "test", connector_src, {})

    assert connector.status == Status.CREATED
    assert connector.service_type == "test"
    connector.service_type = "test2"
    assert connector.service_type == "test2"
    assert connector._dirty

    await connector.sync_doc()
    assert not connector._dirty

    # setting some config with a value that is None
    connector.configuration = {"cool": {"value": "foo"}, "cool2": {"value": None}}

    assert connector.status == Status.NEEDS_CONFIGURATION

    # setting some config
    connector.configuration = {"cool": {"value": "foo"}, "cool2": {"value": "baz"}}

    assert connector.status == Status.CONFIGURED

    with pytest.raises(TypeError):
        connector.status = 1234


@pytest.mark.asyncio
async def test_connectors_properties(mock_responses, set_env):
    """Verifies that the BYOConnector class has access to analysis_icu and language_code form config.

    Args:
        mock_responses (aioresponses.core.aioresponses): Fixture to mock the requests made.
        set_env (None): Fixture to environment variable for config yml.
    """
    config = EnvYAML(CONFIG)
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={
            "hits": {"hits": [{"_id": "1", "_source": mongo}], "total": {"value": 1}}
        },
        headers=headers,
    )

    connectors = BYOIndex(config["elasticsearch"])

    query = connectors.build_docs_query([["mongodb"]])
    async for connector in connectors.get_all_docs(query=query):
        assert connector.analysis_icu == config["elasticsearch"]["analysis_icu"]
        assert connector.language_code == config["elasticsearch"]["language_code"]

    await connectors.close()


class Banana(BaseDataSource):
    """Banana"""

    @classmethod
    def get_default_configuration(cls):
        return {"one": {"value": None}}


@pytest.mark.asyncio
async def test_prepare(mock_responses):
    class Client:
        pass

    class Index:
        client = Client()

        async def save(self, conn):
            pass

    # generic empty doc created by the user through the Kibana UI
    # when it's created that way, the service type is None,
    # so it's up to the connector to set it back to its value
    doc = {
        "status": "created",
        "service_type": None,
        "index_name": "test",
        "configuration": {},
        "scheduling": {"enabled": False},
    }
    connector = BYOConnector(Index(), "1", doc, {})

    config = {
        "connector_id": "1",
        "service_type": "mongodb",
        "sources": {"mongodb": "connectors.tests.test_byoc:Banana"},
    }

    await connector.prepare(config)
    assert connector.source_klass.__doc__ == "Banana"
    assert connector.status == Status.NEEDS_CONFIGURATION
