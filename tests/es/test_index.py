#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import AsyncMock, Mock

import pytest
from elasticsearch import ApiError, ConflictError

from connectors.es.index import DocumentNotFoundError, ESApi, ESIndex

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
async def test_fetch_response_by_id_api_error(mock_responses, patch_sleep):
    doc_id = "1"
    index = FakeIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_refresh", headers=headers, status=200
    )

    mock_responses.get(
        f"http://nowhere.com:9200/{index_name}/_doc/{doc_id}",
        headers=headers,
        status=500,
        repeat=True,
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
async def test_get_all_docs_with_error(mock_responses, patch_logger, patch_sleep):
    index = FakeIndex(index_name, config)
    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_refresh", headers=headers, status=200
    )

    mock_responses.post(
        f"http://nowhere.com:9200/{index_name}/_search?expand_wildcards=hidden",
        headers=headers,
        status=500,
        repeat=True,
    )

    with pytest.raises(ApiError) as e:
        [doc async for doc in index.get_all_docs()]
        assert e.status == 500
        patch_logger.assert_present(
            f"Elasticsearch returned 500 for 'GET {index_name}/_search'"
        )

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
async def test_es_api_connector_check_in():
    connector_id = "id"

    es_api = ESApi(elastic_config=config)
    es_api.client = AsyncMock()

    await es_api.connector_check_in(connector_id)

    es_api.client.connector.check_in.assert_called_once_with(
        connector_id=connector_id)


@pytest.mark.asyncio
async def test_es_api_connector_put():
    connector_id = "id"
    service_type = "service_type"
    connector_name = "connector_name"
    index_name = "index_name"
    is_native = True

    es_api = ESApi(elastic_config=config)
    es_api.client = AsyncMock()

    await es_api.connector_put(
        connector_id, service_type, connector_name, index_name, is_native
    )

    es_api.client.connector.put.assert_called_once_with(
        connector_id=connector_id,
        service_type=service_type,
        name=connector_name,
        index_name=index_name,
        is_native=is_native,
    )


@pytest.mark.asyncio
async def test_es_api_connector_update_scheduling():
    connector_id = "id"
    scheduling = {
        "enabled": "true",
        "interval": "0 4 5 1 *"
    }

    es_api = ESApi(elastic_config=config)
    es_api.client = AsyncMock()

    await es_api.connector_update_scheduling(
        connector_id, scheduling
    )

    es_api.client.connector.update_scheduling.assert_called_once_with(
        connector_id=connector_id,
        scheduling=scheduling
    )


@pytest.mark.asyncio
async def test_es_api_connector_update_configuration():
    connector_id = "id"
    configuration = {"config_key": "config_value"}
    values = {}

    es_api = ESApi(elastic_config=config)
    es_api.client = AsyncMock()

    await es_api.connector_update_configuration(
        connector_id,
        configuration,
        values
    )

    es_api.client.connector.update_configuration.assert_called_once_with(
        connector_id=connector_id,
        configuration=configuration,
        values=values
    )


@pytest.mark.asyncio
async def test_es_api_connector_filtering_draft_validation():
    connector_id = "id"
    validation_result = {"validation": "result"}

    es_api = ESApi(elastic_config=config)
    es_api.client = AsyncMock()

    await es_api.connector_update_filtering_draft_validation(
        connector_id,
        validation_result
    )

    es_api.client.connector.update_filtering_validation.assert_called_once_with(
        connector_id=connector_id,
        validation=validation_result
    )


@pytest.mark.asyncio
async def test_es_api_connector_activate_filtering_draft():
    connector_id = "id"

    es_api = ESApi(elastic_config=config)
    es_api.client = AsyncMock()

    await es_api.connector_activate_filtering_draft(
        connector_id
    )

    es_api.client.connector.update_active_filtering.assert_called_once_with(
        connector_id=connector_id
    )


@pytest.mark.asyncio
async def test_es_api_connector_sync_job_create():
    pass

@pytest.mark.asyncio
async def test_es_api_connector_sync_job_claim():
    sync_job_id = "sync_job_id_test"
    worker_hostname = "workerhostname"
    sync_cursor = {"foo": "bar"}

    es_api = ESApi(elastic_config=config)
    es_api._api_wrapper = AsyncMock()

    await es_api.connector_sync_job_claim(
        sync_job_id,
        worker_hostname,
        sync_cursor
    )

    es_api._api_wrapper.connector_sync_job_claim.assert_called_once_with(
        sync_job_id,
        worker_hostname,
        sync_cursor
    )

@pytest.mark.asyncio
async def test_es_api_connector_sync_job_update_stats():
    sync_job_id = "sync_job_id_test"
    ingestion_stats = {"ingestion": "stat"}
    metadata = {"meta": "data"}

    es_api = ESApi(elastic_config=config)
    es_api._api_wrapper = AsyncMock()

    await es_api.connector_sync_job_update_stats(
        sync_job_id,
        ingestion_stats,
        metadata
    )

    es_api._api_wrapper.connector_sync_job_update_stats.assert_called_once_with(
        sync_job_id,
        ingestion_stats,
        metadata
    )
