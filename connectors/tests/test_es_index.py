#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import Mock, AsyncMock

import pytest

from connectors.es.index import ESIndex

from elasticsearch import ApiError


headers = {"X-Elastic-Product": "Elasticsearch"}
config = {
    "username": "elastic",
    "password": "changeme",
    "host": "http://nowhere.com:9200",
}


@pytest.mark.asyncio
async def test_es_index_create_object_error(mock_responses, patch_logger):
    index = ESIndex("index", config)
    mock_responses.post(
        "http://nowhere.com:9200/index/_refresh", headers=headers, status=200
    )

    mock_responses.post(
        "http://nowhere.com:9200/index/_search?expand_wildcards=hidden",
        headers=headers,
        status=200,
        payload={"hits": {"total": {"value": 1}, "hits": [{"id": 1}]}},
    )
    with pytest.raises(NotImplementedError) as _:
        async for doc_ in index.get_all_docs():
            pass


class FakeDocument:
    pass


class FakeIndex(ESIndex):
    def _create_object(self, doc):
        return FakeDocument()


@pytest.mark.asyncio
async def test_fetch_by_id(mock_responses):
    index_name = "fake_index"
    doc_id = "1"
    index = FakeIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_refresh", headers=headers, status=200
    )

    mock_responses.get(
        f"http://nowhere.com:9200/{index_name}/_doc/{doc_id}",
        headers=headers,
        status=200,
        payload={"_index": index_name, "_id": doc_id, "_source": {}}
    )

    result = await index.fetch_by_id(doc_id)
    assert isinstance(result, FakeDocument)

    await index.close()


@pytest.mark.asyncio
async def test_fetch_by_id_not_found(mock_responses):
    index_name = "fake_index"
    doc_id = "1"
    index = FakeIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_refresh", headers=headers, status=200
    )

    mock_responses.get(
        f"http://nowhere.com:9200/{index_name}/_doc/{doc_id}",
        headers=headers,
        status=404,
    )

    result = await index.fetch_by_id(doc_id)
    assert result is None

    await index.close()


@pytest.mark.asyncio
async def test_fetch_by_id_api_error(mock_responses):
    index_name = "fake_index"
    doc_id = "1"
    index = FakeIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_refresh", headers=headers, status=200
    )

    mock_responses.get(
        f"http://nowhere.com:9200/{index_name}/_doc/{doc_id}",
        headers=headers,
        status=500,
    )

    with pytest.raises(ApiError):
        await index.fetch_by_id(doc_id)

    await index.close()


@pytest.mark.asyncio
async def test_index(mock_responses):
    index_name = "fake_index"
    doc_id = "1"
    index = ESIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_doc",
        headers=headers,
        status=200,
        payload={"_id": doc_id},
    )

    indexed_id = await index.index({})
    assert indexed_id == doc_id

    await index.close()


@pytest.mark.asyncio
async def test_update(mock_responses):
    index_name = "fake_index"
    doc_id = "1"
    index = ESIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_update/{doc_id}",
        headers=headers,
        status=200,
    )

    try:
        await index.update(doc_id, {})
    except Exception as e:
        assert False, f"'update' raised an exception {e}"

    await index.close()
