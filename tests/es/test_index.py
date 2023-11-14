#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import AsyncMock, Mock

import pytest
from elasticsearch import ApiError, ConflictError

from connectors.es.index import DocumentNotFoundError, ESIndex

headers = {"X-Elastic-Product": "Elasticsearch"}
config = {
    "username": "elastic",
    "password": "changeme",
    "host": "http://nowhere.com:9200",
}
index_name = "fake_index"


@pytest.mark.asyncio
async def test_es_index_create_object_error(mock_responses):
    index = ESIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_refresh", headers=headers, status=200
    )

    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_search?expand_wildcards=hidden",
        headers=headers,
        status=200,
        payload={"hits": {"total": {"value": 1}, "hits": [{"_id": 1}]}},
    )
    with pytest.raises(NotImplementedError) as _:
        async for _ in index.get_all_docs():
            pass

    await index.close()


class FakeDocument:
    pass


class FakeIndex(ESIndex):
    def _create_object(self, doc):
        return FakeDocument()


@pytest.mark.asyncio
async def test_fetch_by_id(mock_responses):
    doc_id = "1"
    index = FakeIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_refresh", headers=headers, status=200
    )

    mock_responses.get(
        f"http://nowhere.com:9200/{index_name}/_doc/{doc_id}",
        headers=headers,
        status=200,
        payload={"_index": index_name, "_id": doc_id, "_source": {}},
    )

    result = await index.fetch_by_id(doc_id)
    assert isinstance(result, FakeDocument)

    await index.close()


@pytest.mark.asyncio
async def test_fetch_response_by_id(mock_responses):
    doc_id = "1"
    index = ESIndex(index_name, config)
    doc_source = {
        "_index": index_name,
        "_id": doc_id,
        "_seq_no": 1,
        "_primary_term": 1,
        "_source": {},
    }
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_refresh", headers=headers, status=200
    )

    mock_responses.get(
        f"http://nowhere.com:9200/{index_name}/_doc/{doc_id}",
        headers=headers,
        status=200,
        payload=doc_source,
    )

    result = await index.fetch_response_by_id(doc_id)
    assert result == doc_source

    await index.close()


@pytest.mark.asyncio
async def test_fetch_response_by_id_not_found(mock_responses):
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

    with pytest.raises(DocumentNotFoundError):
        await index.fetch_response_by_id(doc_id)

    await index.close()


@pytest.mark.asyncio
async def test_fetch_response_by_id_api_error(mock_responses):
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
        await index.fetch_response_by_id(doc_id)

    await index.close()


@pytest.mark.asyncio
async def test_index(mock_responses):
    doc_id = "1"
    index = ESIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_doc",
        headers=headers,
        status=200,
        payload={"_id": doc_id},
    )

    resp = await index.index({})
    assert resp["_id"] == doc_id

    await index.close()


@pytest.mark.asyncio
async def test_update(mock_responses):
    doc_id = "1"
    index = ESIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_update/{doc_id}",
        headers=headers,
        status=200,
    )

    # the test will fail if any error is raised
    await index.update(doc_id, {})

    await index.close()


@pytest.mark.asyncio
async def test_update_with_concurrency_control(mock_responses):
    doc_id = "1"
    index = ESIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_update/{doc_id}?if_seq_no=1&if_primary_term=1",
        headers=headers,
        status=409,
    )

    with pytest.raises(ConflictError):
        await index.update(doc_id, {}, if_seq_no=1, if_primary_term=1)

    await index.close()


@pytest.mark.asyncio
async def test_update_by_script():
    doc_id = "1"
    script = {"source": ""}
    index = ESIndex(index_name, config)
    index.client = Mock()
    index.client.update = AsyncMock()

    await index.update_by_script(doc_id, script)
    index.client.update.assert_awaited_once_with(
        index=index_name, id=doc_id, script=script
    )


@pytest.mark.asyncio
async def test_get_all_docs_with_error(mock_responses):
    index = FakeIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_refresh", headers=headers, status=200
    )

    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_search?expand_wildcards=hidden",
        headers=headers,
        status=500,
    )

    docs = [doc async for doc in index.get_all_docs()]
    assert len(docs) == 0

    await index.close()


@pytest.mark.asyncio
async def test_get_all_docs(mock_responses):
    index = FakeIndex(index_name, config)
    total = 3
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_refresh", headers=headers, status=200
    )

    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_search?expand_wildcards=hidden",
        headers=headers,
        status=200,
        payload={
            "hits": {"total": {"value": total}, "hits": [{"_id": "1"}, {"_id": "2"}]}
        },
    )
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_search?expand_wildcards=hidden",
        headers=headers,
        status=200,
        payload={"hits": {"total": {"value": total}, "hits": [{"_id": "3"}]}},
    )

    doc_count = 0
    async for doc in index.get_all_docs(page_size=2):
        assert isinstance(doc, FakeDocument)
        doc_count += 1
    assert doc_count == total

    await index.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mappings, expected_meta",
    [
        ({".elastic-connectors-v1": {"mappings": {}}}, None),
        (
            {".elastic-connectors-v1": {"mappings": {"_meta": {"foo": "bar"}}}},
            {"foo": "bar"},
        ),
        (
            {
                ".elastic-connectors-v1": {"mappings": {"_meta": {"foo": "bar"}}},
                ".elastic-connectors-v2": {"mappings": {"_meta": {"baz": "qux"}}},
            },
            {"baz": "qux"},
        ),
    ],
)
async def test__meta(mappings, expected_meta, mock_responses):
    mock_responses.get(
        "http://nowhere.com:9200/fake_index/_mapping",
        payload=mappings,
        headers=headers,
    )

    index = ESIndex(index_name, config)

    meta = await index.meta()
    assert meta == expected_meta
    await index.close()


@pytest.mark.asyncio
async def test_update_meta(mock_responses):
    meta = {"foo": "bar"}
    mock_responses.put(
        "http://nowhere.com:9200/fake_index/_mapping?write_index_only=true",
        payload={"_meta": meta},
        headers=headers,
    )
    index = ESIndex(index_name, config)
    await index.update_meta(meta)
    await index.close()
