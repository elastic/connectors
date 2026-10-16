#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from unittest.mock import AsyncMock, Mock, patch

import pytest

from connectors.es.index import DocumentNotFoundError
from connectors.services.job_cleanup import IDLE_JOB_ERROR, JobCleanUpService
from tests.commons import AsyncIterator
from tests.services.test_base import create_and_run_service

CONFIG = {
    "elasticsearch": {
        "host": "http://nowhere.com:9200",
        "user": "elastic",
        "password": "changeme",
    },
    "service": {
        "max_errors": 20,
        "max_errors_span": 600,
        "job_cleanup_interval": 1,
    },
    "native_service_types": ["mongodb"],
}


def mock_connector(connector_id="1"):
    connector = Mock()
    connector.id = connector_id
    connector.sync_done = AsyncMock()
    return connector


def mock_sync_job(
    sync_job_id="1",
    connector_id="1",
):
    job = Mock()
    job.job_id = sync_job_id
    job.connector_id = connector_id
    job.fail = AsyncMock()
    job.reload = AsyncMock()
    return job


@pytest.mark.asyncio
@patch("connectors.protocol.SyncJobIndex.idle_jobs")
@patch("connectors.protocol.SyncJobIndex.orphaned_idle_jobs")
@patch("connectors.protocol.ConnectorIndex.fetch_by_id")
@patch("connectors.protocol.ConnectorIndex.supported_connectors")
@patch("connectors.protocol.ConnectorIndex.all_connectors")
async def test_cleanup_jobs(
    all_connectors,
    supported_connectors,
    connector_fetch_by_id,
    orphaned_idle_jobs,
    idle_jobs,
):
    connector = mock_connector()
    orphaned_idle_sync_job = mock_sync_job()
    idle_sync_job = mock_sync_job()

    all_connectors.return_value = AsyncIterator([connector])
    supported_connectors.return_value = AsyncIterator([connector])
    connector_fetch_by_id.return_value = connector
    orphaned_idle_jobs.return_value = AsyncIterator([orphaned_idle_sync_job])
    idle_jobs.return_value = AsyncIterator([idle_sync_job])

    await create_and_run_service(JobCleanUpService, config=CONFIG, stop_after=0.1)

    orphaned_idle_sync_job.fail.assert_called_with(IDLE_JOB_ERROR)
    idle_sync_job.fail.assert_called_with(IDLE_JOB_ERROR)
    connector.sync_done.assert_called_with(job=idle_sync_job)


@pytest.mark.asyncio
@patch("connectors.protocol.SyncJobIndex.idle_jobs")
@patch("connectors.protocol.SyncJobIndex.orphaned_idle_jobs")
@patch("connectors.protocol.ConnectorIndex.fetch_by_id")
@patch("connectors.protocol.ConnectorIndex.supported_connectors")
@patch("connectors.protocol.ConnectorIndex.all_connectors")
async def test_orphaned_idle_job_failures_do_not_stop_cleanup(
    all_connectors,
    supported_connectors,
    connector_fetch_by_id,
    orphaned_idle_jobs,
    idle_jobs,
):
    connector = mock_connector()
    failing_job = mock_sync_job(sync_job_id="orphan-fail")
    failing_job.fail = AsyncMock(side_effect=RuntimeError("fail failed"))

    all_connectors.return_value = AsyncIterator([connector])
    supported_connectors.return_value = AsyncIterator([])
    orphaned_idle_jobs.return_value = AsyncIterator([failing_job])
    idle_jobs.return_value = AsyncIterator([])

    await create_and_run_service(JobCleanUpService, config=CONFIG, stop_after=0.1)

    failing_job.fail.assert_awaited_once_with(IDLE_JOB_ERROR)
    connector.sync_done.assert_not_called()


@pytest.mark.asyncio
@patch("connectors.protocol.SyncJobIndex.idle_jobs")
@patch("connectors.protocol.SyncJobIndex.orphaned_idle_jobs")
@patch("connectors.protocol.ConnectorIndex.fetch_by_id")
@patch("connectors.protocol.ConnectorIndex.supported_connectors")
@patch("connectors.protocol.ConnectorIndex.all_connectors")
async def test_idle_job_skips_sync_done_when_connector_missing(
    all_connectors,
    supported_connectors,
    connector_fetch_by_id,
    orphaned_idle_jobs,
    idle_jobs,
):
    connector = mock_connector()
    idle_sync_job = mock_sync_job()

    all_connectors.return_value = AsyncIterator([connector])
    supported_connectors.return_value = AsyncIterator([connector])
    connector_fetch_by_id.side_effect = DocumentNotFoundError("gone")
    orphaned_idle_jobs.return_value = AsyncIterator([])
    idle_jobs.return_value = AsyncIterator([idle_sync_job])

    await create_and_run_service(JobCleanUpService, config=CONFIG, stop_after=0.1)

    idle_sync_job.fail.assert_called_with(IDLE_JOB_ERROR)
    connector.sync_done.assert_not_called()


@pytest.mark.asyncio
@patch("connectors.protocol.SyncJobIndex.idle_jobs")
@patch("connectors.protocol.SyncJobIndex.orphaned_idle_jobs")
@patch("connectors.protocol.ConnectorIndex.fetch_by_id")
@patch("connectors.protocol.ConnectorIndex.supported_connectors")
@patch("connectors.protocol.ConnectorIndex.all_connectors")
async def test_idle_job_sync_done_when_job_reload_fails(
    all_connectors,
    supported_connectors,
    connector_fetch_by_id,
    orphaned_idle_jobs,
    idle_jobs,
):
    connector = mock_connector()
    idle_sync_job = mock_sync_job()
    idle_sync_job.reload = AsyncMock(side_effect=DocumentNotFoundError("gone"))

    all_connectors.return_value = AsyncIterator([connector])
    supported_connectors.return_value = AsyncIterator([connector])
    connector_fetch_by_id.return_value = connector
    orphaned_idle_jobs.return_value = AsyncIterator([])
    idle_jobs.return_value = AsyncIterator([idle_sync_job])

    await create_and_run_service(JobCleanUpService, config=CONFIG, stop_after=0.1)

    connector.sync_done.assert_called_once_with(job=None)
