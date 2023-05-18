#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
from unittest.mock import AsyncMock, Mock, patch

import pytest

from connectors.config import load_config
from connectors.es.index import DocumentNotFoundError
from connectors.protocol import JobStatus
from connectors.services.job_execution import JobExecutionService
from tests.commons import AsyncIterator

HERE = os.path.dirname(__file__)
FIXTURES_DIR = os.path.abspath(os.path.join(HERE, "..", "fixtures"))
CONFIG_FILE = os.path.join(FIXTURES_DIR, "config.yml")


def create_service(config_file):
    config = load_config(config_file)
    service = JobExecutionService(config)
    service.idling = 0.05

    return service


async def run_service_with_stop_after(service, stop_after=0):
    def _stop_running_service_without_cancelling():
        service.running = False

    async def _terminate():
        if stop_after == 0:
            # so we actually want the service
            # to run current loop without interruption
            asyncio.get_event_loop().call_soon(_stop_running_service_without_cancelling)
        else:
            # but if stop_after is provided we want to
            # interrupt the service after the timeout
            await asyncio.sleep(stop_after)
            service.stop()

        await asyncio.sleep(0)

    await asyncio.gather(service.run(), _terminate())


async def create_and_run_service(config_file=CONFIG_FILE, stop_after=0):
    service = create_service(config_file)
    await run_service_with_stop_after(service, stop_after)


@pytest.fixture(autouse=True)
def connector_index_mock():
    with patch(
        "connectors.services.job_execution.ConnectorIndex"
    ) as connector_index_klass_mock:
        connector_index_mock = Mock()
        connector_index_mock.stop_waiting = Mock()
        connector_index_mock.close = AsyncMock()
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


@pytest.fixture(autouse=True)
def concurrent_tasks_mock():
    with patch(
        "connectors.services.job_execution.ConcurrentTasks"
    ) as concurrent_tasks_klass_mock:
        concurrent_tasks_mock = Mock()
        concurrent_tasks_mock.put = AsyncMock()
        concurrent_tasks_mock.join = AsyncMock()
        concurrent_tasks_mock.cancel = Mock()
        concurrent_tasks_klass_mock.return_value = concurrent_tasks_mock

        yield concurrent_tasks_mock


@pytest.fixture(autouse=True)
def sync_job_runner_mock():
    with patch(
        "connectors.services.job_execution.SyncJobRunner"
    ) as sync_job_runner_klass_mock:
        sync_job_runner_mock = Mock()
        sync_job_runner_mock.execute = AsyncMock()
        sync_job_runner_klass_mock.return_value = sync_job_runner_mock

        yield sync_job_runner_mock


def mock_connector(last_sync_status=JobStatus.COMPLETED):
    connector = Mock()
    connector.id = "1"
    connector.last_sync_status = last_sync_status

    return connector


def mock_sync_job(service_type="fake"):
    sync_job = Mock()
    sync_job.service_type = service_type
    sync_job.connector_id = "1"

    return sync_job


@pytest.mark.asyncio
async def test_no_connector(connector_index_mock, concurrent_tasks_mock, set_env):
    connector_index_mock.supported_connectors.return_value = AsyncIterator([])
    await create_and_run_service()

    concurrent_tasks_mock.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_pending_jobs(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mock,
    set_env,
):
    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([])
    await create_and_run_service()

    concurrent_tasks_mock.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_job_execution(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mock,
    sync_job_runner_mock,
    set_env,
):
    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    connector_index_mock.fetch_by_id = AsyncMock(return_value=connector)
    sync_job = mock_sync_job()
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])
    await create_and_run_service()

    concurrent_tasks_mock.put.assert_awaited_once_with(sync_job_runner_mock.execute)


@pytest.mark.asyncio
async def test_job_execution_with_unsupported_source(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mock,
    set_env,
):
    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    sync_job = mock_sync_job(service_type="mysql")
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])
    await create_and_run_service()

    concurrent_tasks_mock.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_job_execution_with_connector_not_found(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mock,
    sync_job_runner_mock,
    set_env,
):
    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    connector_index_mock.fetch_by_id = AsyncMock(side_effect=DocumentNotFoundError())
    sync_job = mock_sync_job()
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])
    await create_and_run_service()

    concurrent_tasks_mock.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_job_execution_with_connector_still_syncing(
    connector_index_mock,
    sync_job_index_mock,
    concurrent_tasks_mock,
    sync_job_runner_mock,
    set_env,
):
    connector = mock_connector(last_sync_status=JobStatus.IN_PROGRESS)
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    connector_index_mock.fetch_by_id = AsyncMock(return_value=connector)
    sync_job = mock_sync_job()
    sync_job_index_mock.pending_jobs.return_value = AsyncIterator([sync_job])
    await create_and_run_service()

    concurrent_tasks_mock.put.assert_not_awaited()
