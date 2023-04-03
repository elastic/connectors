#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from unittest.mock import ANY, AsyncMock, Mock, patch

import pytest

from connectors.byoc import Filter, Pipeline
from connectors.filtering.validation import InvalidFilteringError
from connectors.sync_job_runner import JobClaimError, SyncJobRunner
from connectors.tests.commons import AsyncIterator

total_document_count = 100


def create_runner(
    source_changed=True,
    source_available=True,
    validate_config_exception=None,
):
    source_klass = Mock()
    data_provider = Mock()
    data_provider.tweak_bulk_options = Mock()
    data_provider.changed = AsyncMock(return_value=source_changed)
    data_provider.validate_config = AsyncMock(side_effect=validate_config_exception)
    data_provider.ping = AsyncMock()
    if not source_available:
        data_provider.ping.side_effect = Exception()
    data_provider.close = AsyncMock()
    source_klass.return_value = data_provider

    sync_job = Mock()
    sync_job.id = "1"
    sync_job.configuration = {}
    sync_job.service_type = "mysql"
    sync_job.index_name = "search-mysql"
    sync_job.pipeline = Pipeline({})
    sync_job.filtering = Filter()
    sync_job.claim = AsyncMock()
    sync_job.done = AsyncMock()
    sync_job.fail = AsyncMock()
    sync_job.cancel = AsyncMock()
    sync_job.suspend = AsyncMock()
    sync_job.reload = AsyncMock(return_value=sync_job)
    sync_job.validate_filtering = AsyncMock()
    sync_job.update_metadata = AsyncMock()

    connector = Mock()
    connector.id = "1"
    connector.features.sync_rules_enabled.return_value = True
    connector.document_count = AsyncMock(return_value=total_document_count)
    connector.sync_starts = AsyncMock()
    connector.sync_done = AsyncMock()

    es_config = {}

    return SyncJobRunner(
        source_klass=source_klass,
        sync_job=sync_job,
        connector=connector,
        es_config=es_config,
    )


@pytest.fixture(autouse=True)
def elastic_server_mock():
    with patch("connectors.sync_job_runner.ElasticServer") as elastic_server_klass_mock:
        elastic_server_mock = Mock()
        elastic_server_mock.prepare_content_index = AsyncMock()
        elastic_server_mock.async_bulk = AsyncMock()
        elastic_server_mock.done = Mock(return_value=True)
        elastic_server_mock.fetch_error = Mock(return_value=None)
        elastic_server_mock.cancel = AsyncMock()
        elastic_server_mock.ingestion_stats = Mock()
        elastic_server_mock.close = AsyncMock()
        elastic_server_klass_mock.return_value = elastic_server_mock

        yield elastic_server_mock


def create_runner_yielding_docs(docs=None):
    if docs is None:
        docs = []

    sync_job_runner = create_runner()

    # initialize explicitly to not rely on a call to `execute`
    data_provider = Mock()
    data_provider.get_docs.return_value = AsyncIterator(docs)
    sync_job_runner.data_provider = data_provider

    return sync_job_runner


@pytest.mark.asyncio
async def test_job_claim_fail(patch_logger):
    sync_job_runner = create_runner()
    sync_job_runner.sync_job.claim.side_effect = Exception()
    with pytest.raises(JobClaimError):
        await sync_job_runner.execute()

    assert sync_job_runner.elastic_server is None
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_not_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_starts_fail(patch_logger):
    sync_job_runner = create_runner()
    sync_job_runner.connector.sync_starts.side_effect = Exception()
    with pytest.raises(JobClaimError):
        await sync_job_runner.execute()

    assert sync_job_runner.elastic_server is None
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_not_changed(patch_logger):
    sync_job_runner = create_runner(source_changed=False)
    await sync_job_runner.execute()

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": total_document_count,
    }

    assert sync_job_runner.elastic_server is None
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.sync_job.done.assert_awaited_with(ingestion_stats=ingestion_stats)
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(sync_job_runner.sync_job)


@pytest.mark.asyncio
async def test_source_invalid_config(patch_logger):
    sync_job_runner = create_runner(validate_config_exception=Exception())
    await sync_job_runner.execute()

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": total_document_count,
    }

    assert sync_job_runner.elastic_server is None
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited
    sync_job_runner.sync_job.fail.assert_awaited_with(
        ANY, ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(sync_job_runner.sync_job)


@pytest.mark.asyncio
async def test_source_not_available(patch_logger):
    sync_job_runner = create_runner(source_available=False)
    await sync_job_runner.execute()

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": total_document_count,
    }

    assert sync_job_runner.elastic_server is None
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited
    sync_job_runner.sync_job.fail.assert_awaited_with(
        ANY, ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(sync_job_runner.sync_job)


@pytest.mark.asyncio
async def test_invalid_filtering(patch_logger):
    sync_job_runner = create_runner()
    sync_job_runner.sync_job.validate_filtering.side_effect = InvalidFilteringError()
    await sync_job_runner.execute()

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": total_document_count,
    }

    assert sync_job_runner.elastic_server is None
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited
    sync_job_runner.sync_job.fail.assert_awaited_with(
        ANY, ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(sync_job_runner.sync_job)


@pytest.mark.asyncio
async def test_async_bulk_error(elastic_server_mock, patch_logger):
    error = "something wrong"
    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
    }
    elastic_server_mock.fetch_error.return_value = error
    elastic_server_mock.ingestion_stats.return_value = ingestion_stats
    sync_job_runner = create_runner()
    await sync_job_runner.execute()

    ingestion_stats["total_document_count"] = total_document_count

    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.elastic_server.async_bulk.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited
    sync_job_runner.sync_job.fail.assert_awaited_with(
        error, ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(sync_job_runner.sync_job)


@pytest.mark.asyncio
async def test_sync_job_runner(elastic_server_mock, patch_logger):
    ingestion_stats = {
        "indexed_document_count": 25,
        "indexed_document_volume": 30,
        "deleted_document_count": 20,
    }
    elastic_server_mock.ingestion_stats.return_value = ingestion_stats
    sync_job_runner = create_runner()
    await sync_job_runner.execute()

    ingestion_stats["total_document_count"] = total_document_count

    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.elastic_server.async_bulk.assert_awaited()
    sync_job_runner.sync_job.done.assert_awaited_with(ingestion_stats=ingestion_stats)
    sync_job_runner.sync_job.fail.assert_not_awaited
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(sync_job_runner.sync_job)


@pytest.mark.asyncio
async def test_sync_job_runner_suspend(elastic_server_mock, patch_logger):
    ingestion_stats = {
        "indexed_document_count": 25,
        "indexed_document_volume": 30,
        "deleted_document_count": 20,
    }
    elastic_server_mock.done.return_value = False
    elastic_server_mock.ingestion_stats.return_value = ingestion_stats
    sync_job_runner = create_runner()
    task = asyncio.create_task(sync_job_runner.execute())
    asyncio.get_event_loop().call_later(0.1, task.cancel)
    await task

    ingestion_stats["total_document_count"] = total_document_count

    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.elastic_server.async_bulk.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_awaited_with(
        ingestion_stats=ingestion_stats
    )
    sync_job_runner.connector.sync_done.assert_awaited_with(sync_job_runner.sync_job)


@patch("connectors.sync_job_runner.ES_ID_SIZE_LIMIT", 1)
@pytest.mark.asyncio
async def test_prepare_docs_when_id_too_long_then_skip_doc():
    _id_too_long = "ab"

    sync_job_runner = create_runner_yielding_docs(docs=[({"_id": _id_too_long}, None)])

    docs = []
    async for doc, _ in sync_job_runner.prepare_docs():
        docs.append(doc)

    assert len(docs) == 0


@patch("connectors.sync_job_runner.ES_ID_SIZE_LIMIT", 10)
@pytest.mark.parametrize("_id", ["ab", 1, 1.5])
@pytest.mark.asyncio
async def test_prepare_docs_when_id_below_limit_then_yield_doc(_id):
    sync_job_runner = create_runner_yielding_docs(docs=[({"_id": _id}, None)])

    docs = []
    async for doc, _ in sync_job_runner.prepare_docs():
        docs.append(doc)

    assert len(docs) == 1
    assert docs[0]["_id"] == _id


@pytest.mark.asyncio
@patch("connectors.sync_job_runner.JOB_REPORTING_INTERVAL", 0)
@patch("connectors.sync_job_runner.JOB_CHECK_INTERVAL", 0)
async def test_sync_job_runner_reporting_metadata(elastic_server_mock, patch_logger):
    ingestion_stats = {
        "indexed_document_count": 15,
        "indexed_document_volume": 230,
        "deleted_document_count": 10,
    }
    elastic_server_mock.ingestion_stats.return_value = ingestion_stats
    elastic_server_mock.done.return_value = False
    sync_job_runner = create_runner()
    task = asyncio.create_task(sync_job_runner.execute())
    asyncio.get_event_loop().call_later(0.1, task.cancel)
    await task

    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.elastic_server.async_bulk.assert_awaited()
    sync_job_runner.sync_job.update_metadata.assert_awaited_with(
        ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_awaited_with(
        ingestion_stats=ingestion_stats | {"total_document_count": total_document_count}
    )
    sync_job_runner.connector.sync_done.assert_awaited_with(sync_job_runner.sync_job)
