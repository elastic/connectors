#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
from unittest.mock import AsyncMock, Mock, patch

import pytest

from connectors.byoc import (
    SYNC_DISABLED,
    ConnectorUpdateError,
    DataSourceError,
    JobStatus,
    ServiceTypeNotConfiguredError,
    ServiceTypeNotSupportedError,
    Status,
)
from connectors.config import load_config
from connectors.services.job_scheduling import JobSchedulingService
from connectors.source import DataSourceConfiguration
from connectors.tests.commons import AsyncIterator

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.yml")


def create_service(config_file):
    config = load_config(config_file)
    service = JobSchedulingService(config)
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
        "connectors.services.job_scheduling.ConnectorIndex"
    ) as connector_index_klass_mock:
        connector_index_mock = Mock()
        connector_index_mock.stop_waiting = Mock()
        connector_index_mock.close = AsyncMock()
        connector_index_klass_mock.return_value = connector_index_mock

        yield connector_index_mock


@pytest.fixture(autouse=True)
def sync_job_index_mock():
    with patch(
        "connectors.services.job_scheduling.SyncJobIndex"
    ) as sync_job_index_klass_mock:
        sync_job_index_mock = Mock()
        sync_job_index_mock.create = AsyncMock(return_value="1")
        sync_job_index_mock.fetch_by_id = AsyncMock(return_value=Mock())
        sync_job_index_mock.stop_waiting = Mock()
        sync_job_index_mock.close = AsyncMock()
        sync_job_index_klass_mock.return_value = sync_job_index_mock

        yield sync_job_index_mock


@pytest.fixture(autouse=True)
def concurrent_tasks_mock():
    with patch(
        "connectors.services.job_scheduling.ConcurrentTasks"
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
        "connectors.services.job_scheduling.SyncJobRunner"
    ) as sync_job_runner_klass_mock:
        sync_job_runner_mock = Mock()
        sync_job_runner_mock.execute = AsyncMock()
        sync_job_runner_klass_mock.return_value = sync_job_runner_mock

        yield sync_job_runner_mock


def mock_connector(
    status=Status.CONNECTED,
    service_type="fake",
    next_sync=SYNC_DISABLED,
    last_sync_status=JobStatus.COMPLETED,
    sync_now=False,
    prepare_exception=None,
):
    connector = Mock()
    connector.native = True
    connector.service_type = service_type
    connector.status = status
    connector.configuration = DataSourceConfiguration({})
    connector.last_sync_status = last_sync_status
    connector.sync_now = sync_now

    connector.features.sync_rules_enabled = Mock(return_value=True)
    connector.validate_filtering = AsyncMock()
    connector.next_sync = Mock(return_value=next_sync)

    connector.prepare = AsyncMock(side_effect=prepare_exception)
    connector.heartbeat = AsyncMock()
    connector.error = AsyncMock()
    connector.reset_sync_now_flag = AsyncMock()

    return connector


@pytest.mark.asyncio
async def test_no_connector(
    connector_index_mock, concurrent_tasks_mock, patch_logger, set_env
):
    connector_index_mock.supported_connectors.return_value = AsyncIterator([])
    await create_and_run_service()

    concurrent_tasks_mock.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_sync_now(
    connector_index_mock,
    concurrent_tasks_mock,
    sync_job_runner_mock,
    patch_logger,
    set_env,
):
    connector = mock_connector(sync_now=True, next_sync=0)
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_awaited()
    connector.reset_sync_now_flag.assert_awaited()
    concurrent_tasks_mock.put.assert_awaited_once_with(sync_job_runner_mock.execute)


@pytest.mark.asyncio
async def test_connector_with_suspended_job(
    connector_index_mock,
    concurrent_tasks_mock,
    sync_job_runner_mock,
    patch_logger,
    set_env,
):
    connector = mock_connector(next_sync=100, last_sync_status=JobStatus.SUSPENDED)
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_awaited()
    connector.reset_sync_now_flag.assert_not_awaited()
    concurrent_tasks_mock.put.assert_awaited_once_with(sync_job_runner_mock.execute)


@pytest.mark.asyncio
async def test_connector_ready_to_sync(
    connector_index_mock,
    concurrent_tasks_mock,
    sync_job_runner_mock,
    patch_logger,
    set_env,
):
    connector = mock_connector(next_sync=0.01)
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_awaited
    connector.reset_sync_now_flag.assert_not_awaited()
    concurrent_tasks_mock.put.assert_awaited_once_with(sync_job_runner_mock.execute)


@pytest.mark.asyncio
async def test_connector_sync_disabled(
    connector_index_mock, concurrent_tasks_mock, patch_logger, set_env
):
    connector = mock_connector()
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_awaited()
    connector.reset_sync_now_flag.assert_not_awaited()
    concurrent_tasks_mock.put.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connector_status",
    [Status.CREATED, Status.NEEDS_CONFIGURATION],
)
async def test_connector_not_configured(
    connector_status,
    connector_index_mock,
    concurrent_tasks_mock,
    patch_logger,
    set_env,
):
    connector = mock_connector(status=connector_status)
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_awaited()
    connector.reset_sync_now_flag.assert_not_awaited()
    concurrent_tasks_mock.put.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prepare_exception",
    [
        ServiceTypeNotConfiguredError,
        ConnectorUpdateError,
        ServiceTypeNotSupportedError,
        DataSourceError,
    ],
)
async def test_connector_prepare_failed(
    prepare_exception,
    connector_index_mock,
    concurrent_tasks_mock,
    patch_logger,
    set_env,
):
    connector = mock_connector(prepare_exception=prepare_exception())
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_not_awaited()
    connector.reset_sync_now_flag.assert_not_awaited()
    concurrent_tasks_mock.put.assert_not_awaited()
