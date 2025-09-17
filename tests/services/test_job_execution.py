#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import itertools
from unittest.mock import AsyncMock, Mock, patch

import pytest

from connectors.es.client import License
from connectors.es.index import DocumentNotFoundError
from connectors.protocol import JobStatus, JobType
from connectors.services.access_control_sync_job_execution import (
    AccessControlSyncJobExecutionService,
)
from connectors.services.content_sync_job_execution import (
    ContentSyncJobExecutionService,
)
from tests.commons import AsyncIterator
from tests.services.test_base import create_and_run_service


@pytest.fixture(autouse=True)
def connector_index_mock():
    with patch(
        "connectors.services.job_execution.ConnectorIndex"
    ) as connector_index_klass_mock:
        connector_index_mock = Mock()
        connector_index_mock.stop_waiting = Mock()
        connector_index_mock.close = AsyncMock()
        connector_index_mock.has_active_license_enabled = AsyncMock(
            return_value=(True, None)
        )
        connector_index_klass_mock.return_value = connector_index_mock

        yield connector_index_mock


@pytest.fixture(autouse=True)
def sync_job_index_mock():
    with patch(
        "connectors.services.job_execution.SyncJobIndex"
    ) as sync_job_index_klass_mock:
        sync_job_index_mock = Mock()
        sync_job_index_mock.stop_waiting = Mock()
        sync_job_index_mock.close = AsyncMock()
        sync_job_index_klass_mock.return_value = sync_job_index_mock

        yield sync_job_index_mock


def concurrent_task_mock():
    task_mock = Mock()
    task_mock.try_put = Mock()
    task_mock.join = AsyncMock()
    task_mock.cancel = Mock()

    return task_mock


@pytest.fixture(autouse=True)
def concurrent_tasks_mocks():
    with patch(
        "connectors.services.job_execution.ConcurrentTasks"
    ) as concurrent_tasks_klass_mock:
        concurrent_content_syncs_tasks_mock = concurrent_task_mock()
        concurrent_tasks_klass_mock.return_value = concurrent_content_syncs_tasks_mock

        yield concurrent_content_syncs_tasks_mock


@pytest.fixture(autouse=True)
def sync_job_runner_mock():
    with patch(
        "connectors.services.job_execution.SyncJobRunner"
    ) as sync_job_runner_klass_mock:
        sync_job_runner_mock = Mock()
        sync_job_runner_mock.execute = AsyncMock()
        sync_job_runner_klass_mock.return_value = sync_job_runner_mock

        yield sync_job_runner_mock


def mock_connector(
    last_sync_status=JobStatus.COMPLETED,
    last_access_control_sync_status=JobStatus.COMPLETED,
    document_level_security_enabled=True,
):
    connector = Mock()
    connector.id = "1"
    connector.last_sync_status = last_sync_status
    connector.last_access_control_sync_status = last_access_control_sync_status
    connector.features = Mock()
    connector.features.document_level_security_enabled = Mock(
        return_value=document_level_security_enabled
    )

    return connector


def mock_sync_job(service_type="fake", job_type=JobType.FULL):
    sync_job = Mock()
    sync_job.service_type = service_type
    sync_job.connector_id = "1"
    sync_job.job_type = job_type

    return sync_job


@pytest.mark.asyncio
async def test_no_connector(connector_index_mock, concurrent_tasks_mocks, set_env):
    sync_job_pool_mock = concurrent_tasks_mocks

    connector_index_mock.supported_connectors.return_value = AsyncIterator([])
    await create_and_run_service(ContentSyncJobExecutionService)

    sync_job_pool_mock.try_put.assert_not_called()


@pytest.mark.parametrize(
    "service_klass",
    [ContentSyncJobExecutionService, AccessControlSyncJobExecutionService],
)
@pytest.mark.asyncio
async def test_no_pending_jobs(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    service_klass,
    set_env,
):
    sync_job_pool_mock = concurrent_tasks_mocks

    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([])
    await create_and_run_service(service_klass)

    sync_job_pool_mock.try_put.assert_not_called()


@pytest.mark.parametrize(
    "service_klass, job_type",
    [
        (ContentSyncJobExecutionService, JobType.FULL),
        (AccessControlSyncJobExecutionService, JobType.ACCESS_CONTROL),
    ],
)
@pytest.mark.asyncio
async def test_job_execution_with_unsupported_source(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    service_klass,
    job_type,
    set_env,
):
    sync_job_pool_mock = concurrent_tasks_mocks

    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    sync_job = mock_sync_job(service_type="mysql", job_type=job_type)
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])
    await create_and_run_service(service_klass)

    sync_job_pool_mock.try_put.assert_not_called()


@pytest.mark.parametrize(
    "service_klass, job_type",
    [
        (ContentSyncJobExecutionService, JobType.FULL),
        (AccessControlSyncJobExecutionService, JobType.ACCESS_CONTROL),
    ],
)
@pytest.mark.asyncio
async def test_job_execution_with_connector_not_found(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    sync_job_runner_mock,
    service_klass,
    job_type,
    set_env,
):
    sync_job_pool_mock = concurrent_tasks_mocks

    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    connector_index_mock.fetch_by_id = AsyncMock(side_effect=DocumentNotFoundError())
    sync_job = mock_sync_job(job_type=job_type)
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])
    await create_and_run_service(service_klass)

    sync_job_pool_mock.try_put.assert_not_called()


@pytest.mark.asyncio
async def test_access_control_sync_job_execution_with_premium_connector(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    sync_job_runner_mock,
    set_env,
):
    sync_job_pool_mock = concurrent_tasks_mocks

    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    connector_index_mock.fetch_by_id = AsyncMock(return_value=connector)
    sync_job = mock_sync_job(job_type=JobType.ACCESS_CONTROL)
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])
    await create_and_run_service(AccessControlSyncJobExecutionService)

    sync_job_pool_mock.try_put.assert_called_once_with(sync_job_runner_mock.execute)


@pytest.mark.asyncio
async def test_access_control_sync_job_execution_with_insufficient_license(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    sync_job_runner_mock,
    set_env,
):
    sync_job_pool_mock = concurrent_tasks_mocks

    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    connector_index_mock.fetch_by_id = AsyncMock(return_value=connector)
    connector_index_mock.has_active_license_enabled = AsyncMock(
        return_value=(False, License.BASIC)
    )

    sync_job = mock_sync_job(job_type=JobType.ACCESS_CONTROL)
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])
    await create_and_run_service(AccessControlSyncJobExecutionService)

    sync_job_pool_mock.try_put.assert_not_called()


@pytest.mark.asyncio
async def test_access_control_sync_job_execution_with_dls_feature_flag_disabled(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    sync_job_runner_mock,
    set_env,
):
    sync_job_pool_mock = concurrent_tasks_mocks

    connector = mock_connector(
        document_level_security_enabled=False,
    )

    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    connector_index_mock.fetch_by_id = AsyncMock(return_value=connector)

    sync_job = mock_sync_job(job_type=JobType.ACCESS_CONTROL)
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])

    await create_and_run_service(AccessControlSyncJobExecutionService)

    sync_job_pool_mock.try_put.assert_not_called()


@pytest.mark.parametrize(
    "service_klass, job_type",
    [
        (ContentSyncJobExecutionService, JobType.FULL),
        (AccessControlSyncJobExecutionService, JobType.ACCESS_CONTROL),
    ],
)
@pytest.mark.asyncio
async def test_job_execution_with_connector_still_syncing(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    sync_job_runner_mock,
    service_klass,
    job_type,
    set_env,
):
    sync_job_pool_mock = concurrent_tasks_mocks

    connector = mock_connector(
        last_sync_status=JobStatus.IN_PROGRESS,
        last_access_control_sync_status=JobStatus.IN_PROGRESS,
    )
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    connector_index_mock.fetch_by_id = AsyncMock(return_value=connector)
    sync_job = mock_sync_job(job_type=job_type)
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])
    await create_and_run_service(service_klass)

    sync_job_pool_mock.try_put.assert_not_called()


@pytest.mark.parametrize(
    "service_klass, job_type",
    [
        (ContentSyncJobExecutionService, JobType.FULL),
        (AccessControlSyncJobExecutionService, JobType.ACCESS_CONTROL),
    ],
)
@pytest.mark.asyncio
async def test_job_execution(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    sync_job_runner_mock,
    service_klass,
    job_type,
    set_env,
):
    sync_job_pool_mock = concurrent_tasks_mocks

    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    connector_index_mock.fetch_by_id = AsyncMock(return_value=connector)

    sync_job = mock_sync_job(job_type=job_type)
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])

    await create_and_run_service(service_klass)

    sync_job_pool_mock.try_put.assert_called_once_with(sync_job_runner_mock.execute)


@pytest.mark.parametrize(
    "service_klass, job_type",
    [
        (ContentSyncJobExecutionService, JobType.FULL),
        (AccessControlSyncJobExecutionService, JobType.ACCESS_CONTROL),
    ],
)
@pytest.mark.asyncio
async def test_job_execution_new_sync_job_not_blocked(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mocks,
    sync_job_runner_mock,
    service_klass,
    job_type,
    set_env,
):
    sync_job_pool_mock = concurrent_tasks_mocks

    connector = mock_connector()
    connector_index_mock.supported_connectors.side_effect = itertools.chain(
        [AsyncIterator([connector]), AsyncIterator([connector])],
        itertools.repeat(AsyncIterator([])),
    )
    connector_index_mock.fetch_by_id = AsyncMock(return_value=connector)

    first_sync_job = mock_sync_job(job_type=job_type)
    second_sync_job = mock_sync_job(job_type=job_type)
    sync_job_index_mock.pending_jobs.side_effect = [
        AsyncIterator([first_sync_job]),
        AsyncIterator([second_sync_job]),
    ]

    await create_and_run_service(service_klass, stop_after=0.2)

    sync_job_pool_mock.try_put.assert_called_with(sync_job_runner_mock.execute)
    assert sync_job_pool_mock.try_put.call_count == 2
