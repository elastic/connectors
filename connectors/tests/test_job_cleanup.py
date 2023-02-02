#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio
from unittest.mock import AsyncMock, Mock, call, patch

import pytest

from connectors.services.job_cleanup import STUCK_JOB_ERROR, JobCleanUpService
from connectors.tests.commons import AsyncGeneratorFake

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


def create_service():
    return JobCleanUpService(CONFIG)


def mock_connector():
    connector = Mock()
    connector.id.return_value = "1"
    connector._sync_done = AsyncMock()
    return connector


def mock_sync_job():
    job = Mock()
    job.job_id.return_value = "1"
    job.index_name.return_value = "index_name"
    job.connector_id.return_value = "1"
    return job


@pytest.mark.asyncio
@patch("connectors.byoc.SyncJobIndex.delete_jobs")
@patch("connectors.byoc.SyncJobIndex.delete_indices")
@patch("connectors.byoc.SyncJobIndex.stuck_jobs")
@patch("connectors.byoc.SyncJobIndex.orphaned_jobs")
@patch("connectors.byoc.ConnectorIndex.fetch_by_id")
@patch("connectors.byoc.ConnectorIndex.supported_connectors")
@patch("connectors.byoc.ConnectorIndex.all_connectors")
async def test_cleanup_jobs(
    all_connectors,
    supported_connectors,
    fetch_by_id,
    orphaned_jobs,
    stuck_jobs,
    delete_indices,
    delete_jobs,
):
    connector = mock_connector()
    sync_job = mock_sync_job()

    all_connectors.return_value = AsyncGeneratorFake([connector])
    supported_connectors.return_value = AsyncGeneratorFake([connector])
    fetch_by_id.return_value = connector
    orphaned_jobs.return_value = AsyncGeneratorFake([sync_job])
    stuck_jobs.return_value = AsyncGeneratorFake([sync_job])
    delete_jobs.return_value = {"deleted": 1, "failures": [], "total": 1}

    service = create_service()
    asyncio.get_event_loop().call_later(0.5, service.stop)
    await service.run()

    assert delete_indices.call_args_list == [call(indices=[sync_job.index_name])]
    assert delete_jobs.call_args_list == [call(job_ids=[sync_job.job_id])]
    assert connector._sync_done.call_args_list == [
        call(job=sync_job, result={}, exception=STUCK_JOB_ERROR)
    ]
