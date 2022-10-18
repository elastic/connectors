#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import json
from datetime import datetime

from aioresponses import CallbackResult
from elasticsearch import AsyncElasticsearch
import pytest

from connectors.byoei import ElasticServer
from connectors.byoc import (
    e2str,
    Status,
    iso_utc,
    SyncJob,
    JobStatus,
    BYOIndex,
    BYOConnector,
)


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
        payload={"hits": {"hits": [{"_id": "1", "_source": mongo}]}},
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

    async for connector in connectors.get_list():
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
        payload={"hits": {"hits": [{"_id": "1", "_source": mongo}]}},
        headers=headers,
    )

    connectors = BYOIndex(config)
    conns = []

    async for connector in connectors.get_list():
        conns.append(connector)

    assert len(conns) == 1
    await connectors.close()


@pytest.mark.asyncio
async def test_sync_mongo(mock_responses, patch_logger):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={"hits": {"hits": [{"_id": "1", "_source": mongo}]}},
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

    doc = {"_id": 1}

    class StubIndex:
        def __init__(self):
            self.client = None

    class Data:
        def __init__(self):
            self.connector = self.make_connector()

        async def ping(self):
            pass

        async def changed(self):
            return True

        async def get_docs(self, *args, **kw):
            for d in [doc, doc]:
                yield {"_id": 1}, None

        def make_connector(self):
            index_name = "search-some-index"
            connector_src = {
                "service_type": "test",
                "index_name": index_name,
                "configuration": {},
                "scheduling": {},
                "status": "created",
            }
            return BYOConnector(StubIndex(), "test", connector_src)

    es = ElasticServer(config)
    connectors = BYOIndex(config)
    try:
        async for connector in connectors.get_list():
            await connector.sync(Data(), es, 0)
            await connector.close()
    finally:
        await connectors.close()
        await es.close()
