#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import datetime
import pytest
from connectors.byoei import ElasticServer


@pytest.mark.asyncio
async def test_prepare_index(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.head(
        "http://nowhere.com:9200/search-new-index?expand_wildcards=hidden",
        headers=headers,
        status=404,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-new-index",
        payload={"_id": "1"},
        headers=headers,
    )

    es = ElasticServer(config)
    await es.prepare_index("search-new-index", delete_first=True)

    mock_responses.head(
        "http://nowhere.com:9200/search-new-index?expand_wildcards=hidden",
        headers=headers,
    )
    mock_responses.delete(
        "http://nowhere.com:9200/search-new-index?expand_wildcards=hidden",
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-new-index",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-new-index/_doc/1",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-new-index/_doc/2",
        payload={"_id": "2"},
        headers=headers,
    )
    await es.prepare_index(
        "search-new-index", delete_first=True, docs=[{"_id": "2"}, {"_id": "3"}]
    )
    await es.close()


@pytest.mark.asyncio
async def test_get_existing_ids(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.get(
        "http://nowhere.com:9200/search-some-index",
        payload={"_id": "1"},
        headers=headers,
    )
    source_1 = {"id": "1", "timestamp": datetime.datetime.now().isoformat()}
    source_2 = {"id": "2", "timestamp": datetime.datetime.now().isoformat()}

    mock_responses.post(
        "http://nowhere.com:9200/search-some-index/_search?scroll=5m",
        payload={
            "_scroll_id": "1234",
            "_shards": {},
            "hits": {
                "hits": [
                    {"_id": "1", "_source": source_1},
                    {"_id": "2", "_source": source_2},
                ]
            },
        },
        headers=headers,
    )
    mock_responses.delete(
        "http://nowhere.com:9200/_search/scroll",
        headers=headers,
    )
    mock_responses.post(
        "http://nowhere.com:9200/_search/scroll",
        payload={
            "hits": {
                "hits": [
                    {"_id": "1", "_source": source_1},
                    {"_id": "2", "_source": source_2},
                ]
            }
        },
        headers=headers,
    )

    es = ElasticServer(config)
    ids = []
    async for doc in es.get_existing_ids("search-some-index"):
        ids.append(doc["id"])

    assert ids == ["1", "2"]
    await es.close()


@pytest.mark.asyncio
async def test_async_bulk(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.get(
        "http://nowhere.com:9200/search-some-index",
        payload={"_id": "1"},
        headers=headers,
    )
    source_1 = {"id": "1", "timestamp": datetime.datetime.now().isoformat()}
    source_2 = {"id": "2", "timestamp": datetime.datetime.now().isoformat()}

    mock_responses.post(
        "http://nowhere.com:9200/search-some-index/_search?scroll=5m",
        payload={
            "_scroll_id": "1234",
            "_shards": {},
            "hits": {
                "hits": [
                    {"_id": "1", "_source": source_1},
                    {"_id": "2", "_source": source_2},
                ]
            },
        },
        headers=headers,
    )
    mock_responses.delete(
        "http://nowhere.com:9200/_search/scroll",
        headers=headers,
    )
    mock_responses.post(
        "http://nowhere.com:9200/_search/scroll",
        payload={
            "hits": {
                "hits": [
                    {"_id": "1", "_source": source_1},
                    {"_id": "2", "_source": source_2},
                ]
            }
        },
        headers=headers,
    )

    mock_responses.put(
        "http://nowhere.com:9200/_bulk",
        payload={
            "took": 7,
            "errors": False,
            "items": [
                {
                    "index": {
                        "_index": "test",
                        "_id": "1",
                        "_version": 1,
                        "result": "created",
                        "forced_refresh": False,
                        "status": 200,
                    }
                },
                {"delete": {"_index": "test", "_id": "3", "status": 200}},
                {"create": {"_index": "test", "_id": "3", "status": 200}},
            ],
        },
        headers=headers,
    )

    es = ElasticServer(config)

    async def get_docs():
        yield {"_id": "1"}
        yield {"_id": "3"}

    res = await es.async_bulk("search-some-index", get_docs())

    # we should delete `2`, update `1` and create `3`
    assert res == {"create": 1, "delete": 1, "index": 1}
    await es.close()
