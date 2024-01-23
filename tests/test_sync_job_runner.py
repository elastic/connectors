#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from unittest.mock import ANY, AsyncMock, Mock, patch

import pytest
from elasticsearch import ConflictError

from connectors.es.client import License
from connectors.es.index import DocumentNotFoundError
from connectors.filtering.validation import InvalidFilteringError
from connectors.protocol import Filter, JobStatus, JobType, Pipeline
from connectors.sync_job_runner import SyncJobRunner, SyncJobStartError
from tests.commons import AsyncIterator

SEARCH_INDEX_NAME = "search-mysql"
ACCESS_CONTROL_INDEX_NAME = "search-acl-filter-mysql"
TOTAL_DOCUMENT_COUNT = 100
SYNC_CURSOR = {"foo": "bar"}


def mock_connector():
    connector = Mock()
    connector.id = "1"
    connector.last_sync_status = JobStatus.COMPLETED
    connector.features.sync_rules_enabled.return_value = True
    connector.sync_cursor = SYNC_CURSOR
    connector.document_count = AsyncMock(return_value=TOTAL_DOCUMENT_COUNT)
    connector.sync_starts = AsyncMock(return_value=True)
    connector.sync_done = AsyncMock()
    connector.reload = AsyncMock()

    return connector


def mock_sync_job(job_type, index_name):
    sync_job = Mock()
    sync_job.id = "1"
    sync_job.configuration = {}
    sync_job.service_type = "mysql"
    sync_job.index_name = index_name
    sync_job.status = JobStatus.IN_PROGRESS
    sync_job.job_type = job_type
    sync_job.pipeline = Pipeline({})
    sync_job.filtering = Filter()
    sync_job.is_content_sync = Mock(
        return_value=job_type in (JobType.FULL, JobType.INCREMENTAL)
    )
    sync_job.claim = AsyncMock()
    sync_job.done = AsyncMock()
    sync_job.fail = AsyncMock()
    sync_job.cancel = AsyncMock()
    sync_job.suspend = AsyncMock()
    sync_job.reload = AsyncMock()
    sync_job.validate_filtering = AsyncMock()
    sync_job.update_metadata = AsyncMock()

    return sync_job


def create_runner(
    source_changed=True,
    source_available=True,
    validate_config_exception=None,
    job_type=JobType.FULL,
    index_name=SEARCH_INDEX_NAME,
    sync_cursor=SYNC_CURSOR,
):
    source_klass = Mock()
    data_provider = Mock()
    data_provider.tweak_bulk_options = Mock()
    data_provider.changed = AsyncMock(return_value=source_changed)
    data_provider.set_features = Mock()
    data_provider.validate_config_fields = Mock()
    data_provider.validate_config = AsyncMock(side_effect=validate_config_exception)
    data_provider.ping = AsyncMock()
    if not source_available:
        data_provider.ping.side_effect = Exception()
    data_provider.sync_cursor = Mock(return_value=sync_cursor)
    data_provider.close = AsyncMock()
    source_klass.return_value = data_provider

    sync_job = mock_sync_job(job_type=job_type, index_name=index_name)
    connector = mock_connector()
    es_config = {}

    return SyncJobRunner(
        source_klass=source_klass,
        sync_job=sync_job,
        connector=connector,
        es_config=es_config,
    )


@pytest.fixture(autouse=True)
def sync_orchestrator_mock():
    with patch(
        "connectors.sync_job_runner.SyncOrchestrator"
    ) as sync_orchestrator_klass_mock:
        sync_orchestrator_mock = Mock()
        sync_orchestrator_mock.prepare_content_index = AsyncMock()
        sync_orchestrator_mock.async_bulk = AsyncMock()
        sync_orchestrator_mock.done = Mock(return_value=True)
        sync_orchestrator_mock.fetch_error = Mock(return_value=None)
        sync_orchestrator_mock.cancel = AsyncMock()
        sync_orchestrator_mock.ingestion_stats = Mock()
        sync_orchestrator_mock.close = AsyncMock()
        sync_orchestrator_mock.has_active_license_enabled = AsyncMock(
            return_value=(True, License.PLATINUM)
        )
        sync_orchestrator_klass_mock.return_value = sync_orchestrator_mock

        yield sync_orchestrator_mock


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
async def test_connector_content_sync_starts_fail():
    sync_job_runner = create_runner()

    # Do nothing in the first call, and the last_sync_status is set to `in_progress` by another instance in the subsequent calls
    def _reset_last_sync_status():
        if sync_job_runner.connector.reload.await_count > 1:
            sync_job_runner.connector.last_sync_status = JobStatus.IN_PROGRESS

    sync_job_runner.connector.reload.side_effect = _reset_last_sync_status
    sync_job_runner.connector.sync_starts.side_effect = ConflictError(
        message="This is an error message from test_connector_sync_starts_fail",
        meta=None,
        body={},
    )

    with pytest.raises(SyncJobStartError):
        await sync_job_runner.execute()

    assert sync_job_runner.sync_orchestrator is None
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.sync_job.claim.assert_not_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_access_control_sync_starts_fail():
    sync_job_runner = create_runner(job_type=JobType.ACCESS_CONTROL)

    # Do nothing in the first call, and the last_access_control_sync_status is set to `in_progress` by another instance in the subsequent calls
    def _reset_last_sync_status():
        if sync_job_runner.connector.reload.await_count > 1:
            sync_job_runner.connector.last_access_control_sync_status = (
                JobStatus.IN_PROGRESS
            )

    sync_job_runner.connector.reload.side_effect = _reset_last_sync_status
    sync_job_runner.connector.sync_starts.side_effect = ConflictError(
        message="This is an error message from test_connector_sync_starts_fail",
        meta=None,
        body={},
    )

    with pytest.raises(SyncJobStartError):
        await sync_job_runner.execute()

    assert sync_job_runner.sync_orchestrator is None
    sync_job_runner.connector.sync_starts.assert_awaited_with(JobType.ACCESS_CONTROL)
    sync_job_runner.sync_job.claim.assert_not_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_not_awaited()


@pytest.mark.parametrize(
    "job_type, sync_cursor_to_claim, sync_cursor_to_update",
    [
        (JobType.FULL, None, SYNC_CURSOR),
        (JobType.INCREMENTAL, SYNC_CURSOR, SYNC_CURSOR),
        (JobType.ACCESS_CONTROL, None, None),
    ],
)
@pytest.mark.asyncio
async def test_source_not_changed(
    job_type, sync_cursor_to_claim, sync_cursor_to_update
):
    sync_job_runner = create_runner(source_changed=False, job_type=job_type)
    await sync_job_runner.execute()

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": TOTAL_DOCUMENT_COUNT,
    }

    assert sync_job_runner.sync_orchestrator is None
    sync_job_runner.connector.sync_starts.assert_awaited_with(job_type)
    sync_job_runner.sync_job.claim.assert_awaited_with(sync_cursor=sync_cursor_to_claim)
    sync_job_runner.sync_job.done.assert_awaited_with(ingestion_stats=ingestion_stats)
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(
        sync_job_runner.sync_job, cursor=sync_cursor_to_update
    )


@pytest.mark.parametrize(
    "job_type, sync_cursor",
    [
        (JobType.FULL, SYNC_CURSOR),
        (JobType.INCREMENTAL, SYNC_CURSOR),
        (JobType.ACCESS_CONTROL, None),
    ],
)
@pytest.mark.asyncio
async def test_source_invalid_config(job_type, sync_cursor):
    sync_job_runner = create_runner(
        job_type=job_type,
        validate_config_exception=Exception(),
        sync_cursor=sync_cursor,
    )
    await sync_job_runner.execute()

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": TOTAL_DOCUMENT_COUNT,
    }

    assert sync_job_runner.sync_orchestrator is None
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_awaited_with(
        ANY, ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(
        sync_job_runner.sync_job, cursor=sync_cursor
    )


@pytest.mark.parametrize(
    "job_type, sync_cursor",
    [
        (JobType.FULL, SYNC_CURSOR),
        (JobType.INCREMENTAL, SYNC_CURSOR),
        (JobType.ACCESS_CONTROL, None),
    ],
)
@pytest.mark.asyncio
async def test_source_not_available(job_type, sync_cursor):
    sync_job_runner = create_runner(
        job_type=job_type, source_available=False, sync_cursor=sync_cursor
    )
    await sync_job_runner.execute()

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": TOTAL_DOCUMENT_COUNT,
    }

    assert sync_job_runner.sync_orchestrator is None
    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_awaited_with(
        ANY, ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(
        sync_job_runner.sync_job, cursor=sync_cursor
    )


@pytest.mark.parametrize("job_type", [JobType.FULL, JobType.INCREMENTAL])
@pytest.mark.asyncio
async def test_invalid_filtering(job_type, sync_orchestrator_mock):
    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": TOTAL_DOCUMENT_COUNT,
    }
    sync_orchestrator_mock.ingestion_stats.return_value = ingestion_stats

    sync_job_runner = create_runner(job_type=job_type)
    sync_job_runner.sync_job.validate_filtering.side_effect = InvalidFilteringError()
    await sync_job_runner.execute()

    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_awaited_with(
        ANY, ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(
        sync_job_runner.sync_job, cursor=SYNC_CURSOR
    )


@pytest.mark.asyncio
async def test_invalid_filtering_access_control_sync_still_executed(
    sync_orchestrator_mock,
):
    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": TOTAL_DOCUMENT_COUNT,
    }
    sync_orchestrator_mock.ingestion_stats.return_value = ingestion_stats

    sync_job_runner = create_runner(job_type=JobType.ACCESS_CONTROL)
    sync_job_runner.sync_job.validate_filtering.side_effect = InvalidFilteringError()

    await sync_job_runner.execute()

    sync_job_runner.connector.sync_starts.assert_awaited_with(JobType.ACCESS_CONTROL)
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_job.done.assert_awaited()
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(
        sync_job_runner.sync_job, cursor=None
    )


@pytest.mark.parametrize(
    "job_type, sync_cursor",
    [
        (JobType.FULL, SYNC_CURSOR),
        (JobType.INCREMENTAL, SYNC_CURSOR),
        (JobType.ACCESS_CONTROL, None),
    ],
)
@pytest.mark.asyncio
async def test_async_bulk_error(job_type, sync_cursor, sync_orchestrator_mock):
    error = "something wrong"
    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
    }
    sync_orchestrator_mock.fetch_error.return_value = error
    sync_orchestrator_mock.ingestion_stats.return_value = ingestion_stats
    sync_job_runner = create_runner(job_type=job_type, sync_cursor=sync_cursor)
    await sync_job_runner.execute()

    ingestion_stats["total_document_count"] = TOTAL_DOCUMENT_COUNT

    sync_job_runner.connector.sync_starts.assert_awaited()
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_orchestrator.async_bulk.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_awaited_with(
        error, ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(
        sync_job_runner.sync_job, cursor=sync_cursor
    )


@pytest.mark.asyncio
async def test_access_control_sync_fails_with_insufficient_license(
    sync_orchestrator_mock,
):
    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": TOTAL_DOCUMENT_COUNT,
    }

    sync_orchestrator_mock.ingestion_stats.return_value = ingestion_stats
    sync_orchestrator_mock.has_active_license_enabled.return_value = (
        False,
        License.BASIC,
    )

    sync_job_runner = create_runner(job_type=JobType.ACCESS_CONTROL)
    await sync_job_runner.execute()

    sync_job_runner.connector.sync_starts.assert_awaited_with(JobType.ACCESS_CONTROL)
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_orchestrator.async_bulk.assert_not_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_awaited_with(
        ANY, ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(
        sync_job_runner.sync_job, cursor=None
    )


@pytest.mark.parametrize(
    "job_type, sync_cursor",
    [
        (JobType.FULL, SYNC_CURSOR),
        (JobType.INCREMENTAL, SYNC_CURSOR),
        (JobType.ACCESS_CONTROL, None),
    ],
)
@pytest.mark.asyncio
async def test_sync_job_runner(job_type, sync_cursor, sync_orchestrator_mock):
    ingestion_stats = {
        "indexed_document_count": 25,
        "indexed_document_volume": 30,
        "deleted_document_count": 20,
    }
    sync_orchestrator_mock.ingestion_stats.return_value = ingestion_stats
    sync_job_runner = create_runner(job_type=job_type, sync_cursor=sync_cursor)
    await sync_job_runner.execute()

    ingestion_stats["total_document_count"] = TOTAL_DOCUMENT_COUNT

    sync_job_runner.connector.sync_starts.assert_awaited_with(job_type)
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_orchestrator.async_bulk.assert_awaited()
    sync_job_runner.sync_job.done.assert_awaited_with(ingestion_stats=ingestion_stats)
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(
        sync_job_runner.sync_job, cursor=sync_cursor
    )


@pytest.mark.parametrize(
    "job_type, sync_cursor",
    [
        (JobType.FULL, SYNC_CURSOR),
        (JobType.INCREMENTAL, SYNC_CURSOR),
        (JobType.ACCESS_CONTROL, None),
    ],
)
@pytest.mark.asyncio
async def test_sync_job_runner_suspend(job_type, sync_cursor, sync_orchestrator_mock):
    ingestion_stats = {
        "indexed_document_count": 25,
        "indexed_document_volume": 30,
        "deleted_document_count": 20,
    }
    sync_orchestrator_mock.done.return_value = False
    sync_orchestrator_mock.ingestion_stats.return_value = ingestion_stats
    sync_job_runner = create_runner(job_type=job_type, sync_cursor=sync_cursor)
    task = asyncio.create_task(sync_job_runner.execute())
    asyncio.get_event_loop().call_later(0.1, task.cancel)
    await task

    ingestion_stats["total_document_count"] = TOTAL_DOCUMENT_COUNT

    sync_job_runner.connector.sync_starts.assert_awaited_with(job_type)
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_orchestrator.async_bulk.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_awaited_with(
        ingestion_stats=ingestion_stats
    )
    sync_job_runner.connector.sync_done.assert_awaited_with(
        sync_job_runner.sync_job, cursor=sync_cursor
    )


@patch("connectors.sync_job_runner.ES_ID_SIZE_LIMIT", 1)
@pytest.mark.asyncio
async def test_prepare_docs_when_original_id_and_hashed_id_too_long_then_skip_doc():
    _id_too_long = "ab"

    sync_job_runner = create_runner_yielding_docs(docs=[({"_id": _id_too_long}, None)])
    sync_job_runner.source_klass.hash_id.return_value = _id_too_long

    docs = []
    async for doc, _, _ in sync_job_runner.prepare_docs():
        docs.append(doc)

    assert len(docs) == 0


@patch("connectors.sync_job_runner.ES_ID_SIZE_LIMIT", 10)
@pytest.mark.parametrize("_id", ["ab", 1, 1.5])
@pytest.mark.asyncio
async def test_prepare_docs_when_original_id_below_limit_then_yield_doc_with_original_id(
    _id,
):
    sync_job_runner = create_runner_yielding_docs(docs=[({"_id": _id}, None)])

    docs = []
    async for doc, _, _ in sync_job_runner.prepare_docs():
        docs.append(doc)

    assert len(docs) == 1
    assert docs[0]["_id"] == _id


@patch("connectors.sync_job_runner.ES_ID_SIZE_LIMIT", 3)
@pytest.mark.asyncio
async def test_prepare_docs_when_original_id_above_limit_and_hashed_id_below_limit_then_yield_doc_with_hashed_id():
    _id_too_long = "abcd"
    hashed_id = "a"

    sync_job_runner = create_runner_yielding_docs(docs=[({"_id": _id_too_long}, None)])
    sync_job_runner.source_klass.hash_id.return_value = hashed_id

    docs = []
    async for doc, _, _ in sync_job_runner.prepare_docs():
        docs.append(doc)

    assert len(docs) == 1
    assert docs[0]["_id"] == hashed_id


@pytest.mark.parametrize(
    "job_type, sync_cursor",
    [
        (JobType.FULL, SYNC_CURSOR),
        (JobType.INCREMENTAL, SYNC_CURSOR),
        (JobType.ACCESS_CONTROL, None),
    ],
)
@pytest.mark.asyncio
@patch("connectors.sync_job_runner.JOB_REPORTING_INTERVAL", 0)
@patch("connectors.sync_job_runner.JOB_CHECK_INTERVAL", 0)
async def test_sync_job_runner_reporting_metadata(
    job_type, sync_cursor, sync_orchestrator_mock
):
    ingestion_stats = {
        "indexed_document_count": 15,
        "indexed_document_volume": 230,
        "deleted_document_count": 10,
    }
    sync_orchestrator_mock.ingestion_stats.return_value = ingestion_stats
    sync_orchestrator_mock.done.return_value = False
    sync_job_runner = create_runner(job_type=job_type, sync_cursor=sync_cursor)
    task = asyncio.create_task(sync_job_runner.execute())
    asyncio.get_event_loop().call_later(0.1, task.cancel)
    await task

    sync_job_runner.connector.sync_starts.assert_awaited_with(job_type)
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_orchestrator.async_bulk.assert_awaited()
    sync_job_runner.sync_job.update_metadata.assert_awaited_with(
        ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_awaited_with(
        ingestion_stats=ingestion_stats | {"total_document_count": TOTAL_DOCUMENT_COUNT}
    )
    sync_job_runner.connector.sync_done.assert_awaited_with(
        sync_job_runner.sync_job, cursor=sync_cursor
    )


@pytest.mark.parametrize(
    "job_type", [JobType.FULL, JobType.INCREMENTAL, JobType.ACCESS_CONTROL]
)
@pytest.mark.asyncio
@patch("connectors.sync_job_runner.JOB_REPORTING_INTERVAL", 0)
@patch("connectors.sync_job_runner.JOB_CHECK_INTERVAL", 0)
async def test_sync_job_runner_connector_not_found(job_type, sync_orchestrator_mock):
    ingestion_stats = {
        "indexed_document_count": 15,
        "indexed_document_volume": 230,
        "deleted_document_count": 10,
    }
    sync_orchestrator_mock.ingestion_stats.return_value = ingestion_stats
    sync_orchestrator_mock.done.return_value = False
    sync_job_runner = create_runner(job_type=job_type)

    # Do nothing in the first call(in sync_starts), then raise DocumentNotFoundError,
    def _raise_document_not_found_error():
        if sync_job_runner.connector.reload.await_count > 1:
            raise DocumentNotFoundError()

    sync_job_runner.connector.reload.side_effect = _raise_document_not_found_error
    await sync_job_runner.execute()

    sync_job_runner.connector.sync_starts.assert_awaited_with(job_type)
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_orchestrator.async_bulk.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_awaited_with(
        ANY, ingestion_stats=ingestion_stats
    )
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_not_awaited()


@pytest.mark.parametrize(
    "job_type, sync_cursor",
    [
        (JobType.FULL, SYNC_CURSOR),
        (JobType.INCREMENTAL, SYNC_CURSOR),
        (JobType.ACCESS_CONTROL, None),
    ],
)
@pytest.mark.asyncio
@patch("connectors.sync_job_runner.JOB_REPORTING_INTERVAL", 0)
@patch("connectors.sync_job_runner.JOB_CHECK_INTERVAL", 0)
async def test_sync_job_runner_sync_job_not_found(
    job_type, sync_cursor, sync_orchestrator_mock
):
    ingestion_stats = {
        "indexed_document_count": 15,
        "indexed_document_volume": 230,
        "deleted_document_count": 10,
    }
    sync_orchestrator_mock.ingestion_stats.return_value = ingestion_stats
    sync_orchestrator_mock.done.return_value = False
    sync_job_runner = create_runner(job_type=job_type, sync_cursor=sync_cursor)
    sync_job_runner.sync_job.reload.side_effect = DocumentNotFoundError()
    await sync_job_runner.execute()

    sync_job_runner.connector.sync_starts.assert_awaited_with(job_type)
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_orchestrator.async_bulk.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(None, cursor=sync_cursor)


@pytest.mark.parametrize(
    "job_type, sync_cursor",
    [
        (JobType.FULL, SYNC_CURSOR),
        (JobType.INCREMENTAL, SYNC_CURSOR),
        (JobType.ACCESS_CONTROL, None),
    ],
)
@pytest.mark.asyncio
@patch("connectors.sync_job_runner.JOB_REPORTING_INTERVAL", 0)
@patch("connectors.sync_job_runner.JOB_CHECK_INTERVAL", 0)
async def test_sync_job_runner_canceled(job_type, sync_cursor, sync_orchestrator_mock):
    ingestion_stats = {
        "indexed_document_count": 15,
        "indexed_document_volume": 230,
        "deleted_document_count": 10,
    }
    sync_orchestrator_mock.ingestion_stats.return_value = ingestion_stats
    sync_orchestrator_mock.done.return_value = False
    sync_job_runner = create_runner(job_type=job_type, sync_cursor=sync_cursor)

    def _update_job_status():
        sync_job_runner.sync_job.status = JobStatus.CANCELING

    sync_job_runner.sync_job.reload.side_effect = _update_job_status
    await sync_job_runner.execute()

    sync_job_runner.connector.sync_starts.assert_awaited_with(job_type)
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_orchestrator.async_bulk.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_not_awaited()
    sync_job_runner.sync_job.cancel.assert_awaited_with(
        ingestion_stats=ingestion_stats | {"total_document_count": TOTAL_DOCUMENT_COUNT}
    )
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(
        sync_job_runner.sync_job, cursor=sync_cursor
    )


@pytest.mark.parametrize(
    "job_type, sync_cursor",
    [
        (JobType.FULL, SYNC_CURSOR),
        (JobType.INCREMENTAL, SYNC_CURSOR),
        (JobType.ACCESS_CONTROL, None),
    ],
)
@pytest.mark.asyncio
@patch("connectors.sync_job_runner.JOB_REPORTING_INTERVAL", 0)
@patch("connectors.sync_job_runner.JOB_CHECK_INTERVAL", 0)
async def test_sync_job_runner_not_running(
    job_type, sync_cursor, sync_orchestrator_mock
):
    ingestion_stats = {
        "indexed_document_count": 15,
        "indexed_document_volume": 230,
        "deleted_document_count": 10,
    }
    sync_orchestrator_mock.ingestion_stats.return_value = ingestion_stats
    sync_orchestrator_mock.done.return_value = False
    sync_job_runner = create_runner(job_type=job_type, sync_cursor=sync_cursor)

    def _update_job_status():
        sync_job_runner.sync_job.status = JobStatus.COMPLETED

    sync_job_runner.sync_job.reload.side_effect = _update_job_status
    await sync_job_runner.execute()

    sync_job_runner.connector.sync_starts.assert_awaited_with(job_type)
    sync_job_runner.sync_job.claim.assert_awaited()
    sync_job_runner.sync_orchestrator.async_bulk.assert_awaited()
    sync_job_runner.sync_job.done.assert_not_awaited()
    sync_job_runner.sync_job.fail.assert_awaited_with(
        ANY,
        ingestion_stats=ingestion_stats
        | {"total_document_count": TOTAL_DOCUMENT_COUNT},
    )
    sync_job_runner.sync_job.cancel.assert_not_awaited()
    sync_job_runner.sync_job.suspend.assert_not_awaited()
    sync_job_runner.connector.sync_done.assert_awaited_with(
        sync_job_runner.sync_job, cursor=sync_cursor
    )


@pytest.mark.asyncio
async def test_sync_job_runner_sets_features_for_data_provider():
    sync_job_runner = create_runner()

    await sync_job_runner.execute()

    assert sync_job_runner.data_provider.set_features.called
