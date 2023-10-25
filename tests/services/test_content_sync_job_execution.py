#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import AsyncMock, Mock, patch

import pytest

from connectors.es.index import DocumentNotFoundError
from connectors.protocol import JobStatus, JobType
from connectors.services.content_sync_job_execution import (
    ContentSyncJobExecutionService,
    load_max_concurrent_content_syncs,
)
from tests.commons import AsyncIterator
from tests.services.test_job_execution import create_and_run_service

MAX_FIVE_CONCURRENT_SYNCS = 5


@pytest.fixture(autouse=True)
def connector_index_mock():
    with patch(
        "connectors.services.content_sync_job_execution.ConnectorIndex"
    ) as connector_index_klass_mock:
        connector_index_mock = Mock()
        connector_index_mock.stop_waiting = Mock()
        connector_index_mock.close = AsyncMock()
        connector_index_klass_mock.return_value = connector_index_mock

        yield connector_index_mock


@pytest.fixture(autouse=True)
def sync_job_index_mock():
    with patch(
        "connectors.services.content_sync_job_execution.SyncJobIndex"
    ) as sync_job_index_klass_mock:
        sync_job_index_mock = Mock()
        sync_job_index_mock.stop_waiting = Mock()
        sync_job_index_mock.close = AsyncMock()
        sync_job_index_klass_mock.return_value = sync_job_index_mock

        yield sync_job_index_mock


def concurrent_task_mock():
    task_mock = Mock()
    task_mock.put = AsyncMock()
    task_mock.join = AsyncMock()
    task_mock.cancel = Mock()

    return task_mock


@pytest.fixture(autouse=True)
def concurrent_tasks_mocks():
    with patch(
        "connectors.services.content_sync_job_execution.ConcurrentTasks"
    ) as concurrent_tasks_klass_mock:
        concurrent_content_syncs_tasks_mock = concurrent_task_mock()
        concurrent_tasks_klass_mock.return_value = concurrent_content_syncs_tasks_mock

        yield concurrent_content_syncs_tasks_mock


@pytest.fixture(autouse=True)
def sync_job_runner_mock():
    with patch(
        "connectors.services.content_sync_job_execution.SyncJobRunner"
    ) as sync_job_runner_klass_mock:
        sync_job_runner_mock = Mock()
        sync_job_runner_mock.execute = AsyncMock()
        sync_job_runner_klass_mock.return_value = sync_job_runner_mock

        yield sync_job_runner_mock


def mock_connector(
    last_sync_status=JobStatus.COMPLETED,
):
    connector = Mock()
    connector.id = "1"
    connector.last_sync_status = last_sync_status
    connector.features = Mock()

    return connector


def mock_sync_job(service_type="fake", job_type=JobType.FULL):
    sync_job = Mock()
    sync_job.service_type = service_type
    sync_job.connector_id = "1"
    sync_job.job_type = job_type

    return sync_job


@pytest.mark.asyncio
async def test_no_connector(connector_index_mock, concurrent_tasks_mocks, set_env):
    content_syncs_tasks_mock = concurrent_tasks_mocks

    connector_index_mock.supported_connectors.return_value = AsyncIterator([])
    await create_and_run_service(ContentSyncJobExecutionService)

    content_syncs_tasks_mock.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_pending_jobs(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    set_env,
):
    content_syncs_tasks_mock = concurrent_tasks_mocks

    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([])
    await create_and_run_service(ContentSyncJobExecutionService)

    content_syncs_tasks_mock.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_job_execution_content_sync_job(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    sync_job_runner_mock,
    set_env,
):
    content_syncs_tasks_mock = concurrent_tasks_mocks

    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    connector_index_mock.fetch_by_id = AsyncMock(return_value=connector)

    content_sync_job = mock_sync_job()
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([content_sync_job])

    await create_and_run_service(ContentSyncJobExecutionService)

    content_syncs_tasks_mock.put.assert_awaited_once_with(sync_job_runner_mock.execute)


@pytest.mark.asyncio
async def test_job_execution_with_unsupported_source(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    set_env,
):
    content_syncs_tasks_mock = concurrent_tasks_mocks

    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    sync_job = mock_sync_job(service_type="mysql")
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])
    await create_and_run_service(ContentSyncJobExecutionService)

    content_syncs_tasks_mock.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_job_execution_with_connector_not_found(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    sync_job_runner_mock,
    set_env,
):
    content_syncs_tasks_mock = concurrent_tasks_mocks

    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    connector_index_mock.fetch_by_id = AsyncMock(side_effect=DocumentNotFoundError())
    content_sync_job = mock_sync_job()
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([content_sync_job])
    await create_and_run_service(ContentSyncJobExecutionService)

    content_syncs_tasks_mock.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_content_job_execution_with_connector_still_syncing(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    sync_job_runner_mock,
    set_env,
):
    content_syncs_tasks_mock = concurrent_tasks_mocks

    connector = mock_connector(last_sync_status=JobStatus.IN_PROGRESS)
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    connector_index_mock.fetch_by_id = AsyncMock(return_value=connector)
    sync_job = mock_sync_job()
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])
    await create_and_run_service(ContentSyncJobExecutionService)

    content_syncs_tasks_mock.put.assert_not_awaited()


def test_load_max_concurrent_content_syncs_from_config():
    max_concurrent_content_syncs = 3

    assert (
        load_max_concurrent_content_syncs(
            {"max_concurrent_content_syncs": max_concurrent_content_syncs}
        )
        == max_concurrent_content_syncs
    )


@patch(
    "connectors.services.content_sync_job_execution.DEFAULT_MAX_CONCURRENT_CONTENT_SYNCS",
    MAX_FIVE_CONCURRENT_SYNCS,
)
def test_load_max_concurrent_content_syncs_fallback_on_default():
    assert load_max_concurrent_content_syncs({}) == MAX_FIVE_CONCURRENT_SYNCS
