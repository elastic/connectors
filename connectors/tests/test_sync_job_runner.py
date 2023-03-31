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
    async_bulk_result=None,
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

    connector = Mock()
    connector.id = "1"
    connector.features.sync_rules_enabled.return_value = True
    connector.document_count = AsyncMock(return_value=total_document_count)
    connector.sync_starts = AsyncMock()
    connector.sync_done = AsyncMock()

    elastic_server = Mock()
    elastic_server.prepare_content_index = AsyncMock()
    elastic_server.async_bulk = AsyncMock(return_value=async_bulk_result)

    bulk_options = {}

    return SyncJobRunner(
        source_klass=source_klass,
        sync_job=sync_job,
        connector=connector,
        elastic_server=elastic_server,
        bulk_options=bulk_options,
    )


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

    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_not_awaited()
    sync_job_runner.elastic_server.async_bulk.assert_not_awaited()
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

    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.elastic_server.async_bulk.assert_not_awaited()
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

    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.elastic_server.async_bulk.assert_not_awaited()
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

    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.elastic_server.async_bulk.assert_not_awaited()
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

    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.elastic_server.async_bulk.assert_not_awaited()
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

    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.elastic_server.async_bulk.assert_not_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited
    sync_job_runner.sync_job.fail.assert_awaited_with(
        ANY, ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(sync_job_runner.sync_job)


@pytest.mark.asyncio
async def test_async_bulk_error(patch_logger):
    error = "something wrong"
    async_bulk_result = {"fetch_error": error}
    sync_job_runner = create_runner(async_bulk_result=async_bulk_result)
    await sync_job_runner.execute()

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": total_document_count,
    }

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
async def test_sync_job_runner(patch_logger):
    doc_updated = 10
    doc_created = 15
    doc_deleted = 20
    async_bulk_result = {
        "doc_updated": doc_updated,
        "doc_created": doc_created,
        "doc_deleted": doc_deleted,
    }
    sync_job_runner = create_runner(async_bulk_result=async_bulk_result)
    await sync_job_runner.execute()

    ingestion_stats = {
        "indexed_document_count": doc_updated + doc_created,
        "indexed_document_volume": 0,
        "deleted_document_count": doc_deleted,
        "total_document_count": total_document_count,
    }

    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.elastic_server.async_bulk.assert_awaited()
    sync_job_runner.sync_job.done.assert_awaited_with(ingestion_stats=ingestion_stats)
    sync_job_runner.sync_job.fail.assert_not_awaited
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(sync_job_runner.sync_job)


@pytest.mark.asyncio
async def test_sync_job_runner_suspend(patch_logger):
    sync_job_runner = create_runner()

    async def _simulate_sync(*args, **kwargs):
        await asyncio.sleep(0.3)
        return {}

    sync_job_runner.elastic_server.async_bulk.side_effect = _simulate_sync
    task = asyncio.create_task(sync_job_runner.execute())
    asyncio.get_event_loop().call_later(0.1, task.cancel)
    await task

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": total_document_count,
    }

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


@patch("connectors.sync_job_runner.ES_ID_SIZE_LIMIT", 2)
@pytest.mark.asyncio
async def test_prepare_docs_when_id_below_limit_then_yield_doc():
    _id_within_limit = "ab"

    sync_job_runner = create_runner_yielding_docs(
        docs=[({"_id": _id_within_limit}, None)]
    )

    docs = []
    async for doc, _ in sync_job_runner.prepare_docs():
        docs.append(doc)

    assert len(docs) == 1
    assert docs[0]["_id"] == _id_within_limit
