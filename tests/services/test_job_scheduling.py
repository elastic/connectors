#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest
from elasticsearch import ConflictError

from connectors.config import load_config
from connectors.es.index import DocumentNotFoundError
from connectors.protocol import (
    DataSourceError,
    JobTriggerMethod,
    ServiceTypeNotConfiguredError,
    ServiceTypeNotSupportedError,
    Status,
)
from connectors.services.job_scheduling import JobSchedulingService
from connectors.source import DataSourceConfiguration
from tests.commons import AsyncIterator

HERE = os.path.dirname(__file__)
FIXTURES_DIR = os.path.abspath(os.path.join(HERE, "..", "fixtures"))
CONFIG_FILE = os.path.join(FIXTURES_DIR, "config.yml")


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
        sync_job_index_mock.stop_waiting = Mock()
        sync_job_index_mock.close = AsyncMock()
        sync_job_index_klass_mock.return_value = sync_job_index_mock

        yield sync_job_index_mock


default_next_sync = datetime.utcnow() + timedelta(hours=1)


def mock_connector(
    status=Status.CONNECTED,
    service_type="fake",
    next_sync=default_next_sync,
    sync_now=False,
    prepare_exception=None,
    last_sync_scheduled_at=None,
):
    connector = Mock()
    connector.native = True
    connector.service_type = service_type
    connector.status = status
    connector.configuration = DataSourceConfiguration({})
    connector.sync_now = sync_now
    connector.last_sync_scheduled_at = last_sync_scheduled_at

    connector.features.sync_rules_enabled = Mock(return_value=True)
    connector.validate_filtering = AsyncMock()
    connector.next_sync = Mock(return_value=next_sync)

    connector.prepare = AsyncMock(side_effect=prepare_exception)
    connector.heartbeat = AsyncMock()
    connector.reload = AsyncMock()
    connector.error = AsyncMock()
    connector.reset_sync_now_flag = AsyncMock()
    connector.update_last_sync_scheduled_at = AsyncMock()

    return connector


@pytest.mark.asyncio
async def test_no_connector(connector_index_mock, sync_job_index_mock, set_env):
    connector_index_mock.supported_connectors.return_value = AsyncIterator([])
    await create_and_run_service()

    sync_job_index_mock.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_sync_now(
    connector_index_mock,
    sync_job_index_mock,
    set_env,
):
    connector = mock_connector(sync_now=True)
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_awaited()
    connector.reset_sync_now_flag.assert_awaited()
    connector.update_last_sync_scheduled_at.assert_not_awaited()
    sync_job_index_mock.create.assert_awaited_once_with(
        connector=connector, trigger_method=JobTriggerMethod.ON_DEMAND
    )


@pytest.mark.asyncio
async def test_connector_sync_now_with_race_condition(
    connector_index_mock,
    sync_job_index_mock,
    set_env,
):
    connector = mock_connector(sync_now=True)

    # Do nothing in the first call, and the sync_now flag is reset by another instance in the subsequent calls
    def _reset_sync_now():
        if connector.reload.await_count > 1:
            connector.sync_now = False

    connector.reload.side_effect = _reset_sync_now
    connector.reset_sync_now_flag.side_effect = ConflictError(
        message="This is an error message from test_connector_sync_now_with_race_condition",
        meta=None,
        body={},
    )
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_awaited()
    connector.reset_sync_now_flag.assert_awaited()
    connector.update_last_sync_scheduled_at.assert_not_awaited()
    sync_job_index_mock.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_ready_to_sync(
    connector_index_mock,
    sync_job_index_mock,
    set_env,
):
    connector = mock_connector(next_sync=datetime.utcnow())
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_awaited
    connector.reset_sync_now_flag.assert_not_awaited()
    connector.update_last_sync_scheduled_at.assert_awaited()
    sync_job_index_mock.create.assert_awaited_once_with(
        connector=connector, trigger_method=JobTriggerMethod.SCHEDULED
    )


@pytest.mark.asyncio
async def test_connector_ready_to_sync_with_race_condition(
    connector_index_mock,
    sync_job_index_mock,
    set_env,
):
    connector = mock_connector(next_sync=datetime.utcnow())

    # Do nothing in the first call(in _should_schedule_on_demand_sync) and second call(in _should_schedule_scheduled_sync), and the last_sync_scheduled_at is updated by another instance in the subsequent calls
    def _reset_last_sync_scheduled_at():
        if connector.reload.await_count > 2:
            connector.last_sync_scheduled_at = datetime.utcnow() + timedelta(seconds=20)

    connector.reload.side_effect = _reset_last_sync_scheduled_at
    connector.update_last_sync_scheduled_at.side_effect = ConflictError(
        message="This is an error message from test_connector_ready_to_sync_with_race_condition",
        meta=None,
        body={},
    )
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_awaited
    connector.reset_sync_now_flag.assert_not_awaited()
    connector.update_last_sync_scheduled_at.assert_awaited()
    sync_job_index_mock.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_sync_disabled(
    connector_index_mock, sync_job_index_mock, set_env
):
    connector = mock_connector(next_sync=None)
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_awaited()
    connector.reset_sync_now_flag.assert_not_awaited()
    connector.update_last_sync_scheduled_at.assert_not_awaited()
    sync_job_index_mock.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_connector_both_on_demand_and_scheduled(
    connector_index_mock,
    sync_job_index_mock,
    set_env,
):
    connector = mock_connector(sync_now=True, next_sync=datetime.utcnow())
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_awaited
    connector.reset_sync_now_flag.assert_awaited()
    connector.update_last_sync_scheduled_at.assert_awaited()
    sync_job_index_mock.create.assert_any_await(
        connector=connector, trigger_method=JobTriggerMethod.ON_DEMAND
    )
    sync_job_index_mock.create.assert_any_await(
        connector=connector, trigger_method=JobTriggerMethod.SCHEDULED
    )
    assert sync_job_index_mock.create.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connector_status",
    [Status.CREATED, Status.NEEDS_CONFIGURATION],
)
async def test_connector_not_configured(
    connector_status,
    connector_index_mock,
    sync_job_index_mock,
    set_env,
):
    connector = mock_connector(status=connector_status)
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_awaited()
    connector.reset_sync_now_flag.assert_not_awaited()
    connector.update_last_sync_scheduled_at.assert_not_awaited()
    sync_job_index_mock.create.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prepare_exception",
    [
        ServiceTypeNotConfiguredError,
        DocumentNotFoundError,
        ServiceTypeNotSupportedError,
        DataSourceError,
    ],
)
async def test_connector_prepare_failed(
    prepare_exception,
    connector_index_mock,
    sync_job_index_mock,
    set_env,
):
    connector = mock_connector(prepare_exception=prepare_exception())
    connector_index_mock.supported_connectors.return_value = AsyncIterator([connector])
    await create_and_run_service()

    connector.prepare.assert_awaited()
    connector.heartbeat.assert_not_awaited()
    connector.reset_sync_now_flag.assert_not_awaited()
    connector.update_last_sync_scheduled_at.assert_not_awaited()
    sync_job_index_mock.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_when_sync_fails_then_continues_service_execution(
    connector_index_mock, set_env
):
    connector = mock_connector(sync_now=True)
    another_connector = mock_connector(sync_now=True)
    connector_index_mock.supported_connectors.return_value = AsyncIterator(
        [connector, another_connector]
    )

    connector.heartbeat.side_effect = Exception("Something went wrong!")

    # 0.15 is a bit arbitrary here
    # It should be enough to make the loop execute a couple of times
    # but is there a better way to tell service to execute loop a couple of times?
    await create_and_run_service(stop_after=0.15)

    # assert that service tried to call connector heartbeat for all connectors
    connector.heartbeat.assert_awaited()
    another_connector.heartbeat.assert_awaited()

    # assert that service did not crash and kept asking index for connectors
    # we don't have a good criteria of what a "crashed service is"
    assert connector_index_mock.supported_connectors.call_count > 1
