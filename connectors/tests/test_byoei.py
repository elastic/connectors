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

    # prepare-index, with mappings
    mappings = {"properties": {"name": {"type": "keyword"}}}
    mock_responses.head(
        "http://nowhere.com:9200/search-new-index?expand_wildcards=hidden",
        headers=headers,
    )
    mock_responses.get(
        "http://nowhere.com:9200/search-new-index/_mapping?expand_wildcards=hidden",
        headers=headers,
        payload={"search-new-index": {"mappings": {}}},
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-new-index/_mapping?expand_wildcards=hidden",
        headers=headers,
    )
    await es.prepare_index("search-new-index", mappings=mappings)

    await es.close()


def set_responses(mock_responses, ts=None):
    if ts is None:
        ts = datetime.datetime.now().isoformat()
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.get(
        "http://nowhere.com:9200/search-some-index",
        payload={"_id": "1"},
        headers=headers,
    )
    source_1 = {"id": "1", "timestamp": ts}
    source_2 = {"id": "2", "timestamp": ts}

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


@pytest.mark.asyncio
async def test_get_existing_ids(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    set_responses(mock_responses)

    es = ElasticServer(config)
    ids = []
    async for doc in es.get_existing_ids("search-some-index"):
        ids.append(doc["id"])

    assert ids == ["1", "2"]
    await es.close()


@pytest.mark.asyncio
async def test_async_bulk(mock_responses, patch_logger):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    set_responses(mock_responses)

    es = ElasticServer(config)

    async def get_docs():
        async def _dl_none(doit=True, timestamp=None):
            return None

        async def _dl(doit=True, timestamp=None):
            if not doit:
                return
            return {"TEXT": "DATA", "timestamp": timestamp, "_id": "1"}

        yield {"_id": "1", "timestamp": datetime.datetime.now().isoformat()}, _dl
        yield {"_id": "3"}, _dl_none

    res = await es.async_bulk("search-some-index", get_docs())

    assert res == {
        "bulk_operations": {"create": 1, "delete": 1, "update": 2},
        "doc_created": 1,
        "doc_deleted": 1,
        "attachment_extracted": 1,
        "doc_updated": 1,
        "fetch_error": None,
    }

    # two syncs
    set_responses(mock_responses)
    res = await es.async_bulk("search-some-index", get_docs())

    assert res == {
        "bulk_operations": {"create": 1, "delete": 1, "update": 2},
        "doc_created": 1,
        "doc_deleted": 1,
        "attachment_extracted": 1,
        "doc_updated": 1,
        "fetch_error": None,
    }

    await es.close()


@pytest.mark.asyncio
async def test_async_bulk_same_ts(mock_responses, patch_logger):

    ts = datetime.datetime.now().isoformat()
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    set_responses(mock_responses, ts)
    es = ElasticServer(config)

    async def get_docs():
        async def _dl(doit=True, timestamp=None):
            if not doit:
                return  # Canceled
            return {"TEXT": "DATA", "timestamp": timestamp, "_id": "1"}

        yield {"_id": "1", "timestamp": ts}, _dl
        yield {"_id": "3", "timestamp": ts}, None

    res = await es.async_bulk("search-some-index", get_docs())

    assert res == {
        "bulk_operations": {"create": 1, "delete": 1},
        "doc_created": 1,
        "doc_deleted": 1,
        "attachment_extracted": 0,
        "doc_updated": 0,
        "fetch_error": None,
    }

    set_responses(mock_responses, ts)
    res = await es.async_bulk("search-some-index", get_docs())

    assert res == {
        "bulk_operations": {"create": 1, "delete": 1},
        "doc_created": 1,
        "doc_deleted": 1,
        "attachment_extracted": 0,
        "doc_updated": 0,
        "fetch_error": None,
    }

    await es.close()
