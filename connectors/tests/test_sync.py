#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
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
from connectors.services.sync import SyncService
from connectors.tests.fake_sources import FakeSourceTS
from connectors.utils import e2str

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.yml")


class Args:
    def __init__(self, **options):
        self.one_sync = options.get("one_sync", False)
        self.sync_now = options.get("sync_now", False)


def create_service(config_file, **options):
    config = load_config(config_file)
    service = SyncService(config, Args(**options))
    service.idling = 5

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


async def create_and_run_service(config_file, stop_after=0):
    service = create_service(config_file)
    await run_service_with_stop_after(service, stop_after)


def mock_connector(
    status=Status.CONNECTED,
    service_type="fake",
    sync_rule_enabled=False,
    next_sync=SYNC_DISABLED,
    last_sync_status=JobStatus.COMPLETED,
    sync_now=False,
):
    connector = Mock()
    connector.native = True
    connector.service_type = service_type
    connector.status = status
    connector.configuration = {}
    connector.last_sync_status = last_sync_status
    connector.sync_now = sync_now

    connector.features.sync_rules_enabled.return_value = sync_rule_enabled
    connector.next_sync.return_value = next_sync

    connector.prepare = AsyncMock()
    connector.heartbeat = AsyncMock()
    connector.error = AsyncMock()
    connector.reset_sync_now_flag = AsyncMock()

    return connector


@pytest.mark.asyncio
@patch("connectors.sync_job_runner.SyncJobRunner.execute")
@patch("connectors.byoc.SyncJobIndex.create")
@patch("connectors.byoc.ConnectorIndex.supported_connectors")
async def test_no_connector(
    supported_connectors, create_job, job_execute, patch_logger, set_env
):
    supported_connectors.return_value = AsyncGeneratorFake([])
    service = create_service(CONFIG_FILE, one_sync=True)
    await service.run()

    create_job.assert_not_called()
    job_execute.assert_not_called()


@pytest.mark.asyncio
@patch("connectors.sync_job_runner.SyncJobRunner.execute")
@patch("connectors.byoc.SyncJobIndex.fetch_by_id")
@patch("connectors.byoc.SyncJobIndex.create")
@patch("connectors.byoc.ConnectorIndex.supported_connectors")
async def test_connector_with_cli_sync_now_flag(
    supported_connectors, create_job, fetch_job, job_execute, patch_logger, set_env
):
    connector = mock_connector()
    supported_connectors.return_value = AsyncGeneratorFake([connector])
    job = Mock()
    create_job.return_value = "1"
    fetch_job.return_value = job
    service = create_service(CONFIG_FILE, sync_now=True, one_sync=True)
    await service.run()

    connector.prepare.assert_called()
    connector.heartbeat.assert_called()
    create_job.assert_called_with(connector, True)
    connector.reset_sync_now_flag.assert_not_called()
    job_execute.assert_called()


@pytest.mark.asyncio
@patch("connectors.sync_job_runner.SyncJobRunner.execute")
@patch("connectors.byoc.SyncJobIndex.fetch_by_id")
@patch("connectors.byoc.SyncJobIndex.create")
@patch("connectors.byoc.ConnectorIndex.supported_connectors")
async def test_connector_with_connector_sync_now_flag(
    supported_connectors, create_job, fetch_job, job_execute, patch_logger, set_env
):
    connector = mock_connector(sync_now=True, next_sync=0)
    supported_connectors.return_value = AsyncGeneratorFake([connector])
    job = Mock()
    create_job.return_value = "1"
    fetch_job.return_value = job
    service = create_service(CONFIG_FILE, one_sync=True)
    await service.run()

    connector.prepare.assert_called()
    connector.heartbeat.assert_called()
    create_job.assert_called_with(connector, False)
    connector.reset_sync_now_flag.assert_called()
    job_execute.assert_called()


@pytest.mark.asyncio
@patch("connectors.sync_job_runner.SyncJobRunner.execute")
@patch("connectors.byoc.SyncJobIndex.fetch_by_id")
@patch("connectors.byoc.SyncJobIndex.create")
@patch("connectors.byoc.ConnectorIndex.supported_connectors")
async def test_connector_with_suspended_job(
    supported_connectors, create_job, fetch_job, job_execute, patch_logger, set_env
):
    connector = mock_connector(next_sync=100, last_sync_status=JobStatus.SUSPENDED)
    supported_connectors.return_value = AsyncGeneratorFake([connector])
    job = Mock()
    create_job.return_value = "1"
    fetch_job.return_value = job
    service = create_service(CONFIG_FILE, one_sync=True)
    await service.run()

    connector.prepare.assert_called()
    connector.heartbeat.assert_called()
    create_job.assert_called_with(connector, False)
    connector.reset_sync_now_flag.assert_not_called()
    job_execute.assert_called()


@pytest.mark.asyncio
@patch("connectors.sync_job_runner.SyncJobRunner.execute")
@patch("connectors.byoc.SyncJobIndex.fetch_by_id")
@patch("connectors.byoc.SyncJobIndex.create")
@patch("connectors.byoc.ConnectorIndex.supported_connectors")
async def test_connector_ready_to_sync(
    supported_connectors, create_job, fetch_job, job_execute, patch_logger, set_env
):
    connector = mock_connector(next_sync=4)
    supported_connectors.return_value = AsyncGeneratorFake([connector])
    job = Mock()
    create_job.return_value = "1"
    fetch_job.return_value = job
    service = create_service(CONFIG_FILE, one_sync=True)
    await service.run()

    connector.prepare.assert_called()
    connector.heartbeat.assert_called()
    create_job.assert_called_with(connector, False)
    connector.reset_sync_now_flag.assert_not_called()
    job_execute.assert_called()


@pytest.mark.asyncio
@patch("connectors.sync_job_runner.SyncJobRunner.execute")
@patch("connectors.byoc.SyncJobIndex.fetch_by_id")
@patch("connectors.byoc.SyncJobIndex.create")
@patch("connectors.byoc.ConnectorIndex.supported_connectors")
async def test_connector_sync_disabled(
    supported_connectors, create_job, fetch_job, job_execute, patch_logger, set_env
):
    connector = mock_connector()
    supported_connectors.return_value = AsyncGeneratorFake([connector])
    job = Mock()
    create_job.return_value = "1"
    fetch_job.return_value = job
    service = create_service(CONFIG_FILE, one_sync=True)
    await service.run()

    connector.prepare.assert_called()
    connector.heartbeat.assert_called()
    create_job.assert_not_called()
    connector.reset_sync_now_flag.assert_not_called()
    job_execute.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connector_status",
    [Status.CREATED, Status.NEEDS_CONFIGURATION],
)
@patch("connectors.sync_job_runner.SyncJobRunner.execute")
@patch("connectors.byoc.SyncJobIndex.fetch_by_id")
@patch("connectors.byoc.SyncJobIndex.create")
@patch("connectors.byoc.ConnectorIndex.supported_connectors")
async def test_connector_not_configured(
    supported_connectors,
    create_job,
    fetch_job,
    job_execute,
    connector_status,
    patch_logger,
    set_env,
):
    connector = mock_connector(status=connector_status)
    supported_connectors.return_value = AsyncGeneratorFake([connector])
    job = Mock()
    create_job.return_value = "1"
    fetch_job.return_value = job
    service = create_service(CONFIG_FILE, sync_now=True, one_sync=True)
    await service.run()

    connector.prepare.assert_called()
    connector.heartbeat.assert_called()
    create_job.assert_not_called()
    connector.reset_sync_now_flag.assert_not_called()
    job_execute.assert_not_called()


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
@patch("connectors.sync_job_runner.SyncJobRunner.execute")
@patch("connectors.byoc.SyncJobIndex.fetch_by_id")
@patch("connectors.byoc.SyncJobIndex.create")
@patch("connectors.byoc.ConnectorIndex.supported_connectors")
async def test_connector_prepare_failed(
    supported_connectors,
    create_job,
    fetch_job,
    job_execute,
    prepare_exception,
    patch_logger,
    set_env,
):
    connector = mock_connector()
    supported_connectors.return_value = AsyncGeneratorFake([connector])
    job = Mock()
    create_job.return_value = "1"
    fetch_job.return_value = job
    connector.prepare.side_effect = prepare_exception()
    service = create_service(CONFIG_FILE, sync_now=True, one_sync=True)
    await service.run()

    connector.prepare.assert_called()
    connector.heartbeat.assert_not_called()
    create_job.assert_not_called()
    connector.reset_sync_now_flag.assert_not_called()
    job_execute.assert_not_called()
