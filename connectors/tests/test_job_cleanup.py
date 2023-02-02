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


def mock_connector(id="1", index_name="index_name"):
    connector = Mock()
    connector.id = id
    connector.index_name = index_name
    connector._sync_done = AsyncMock()
    return connector


def mock_sync_job(id="1", connector_id="1", index_name="index_name"):
    job = Mock()
    job.job_id = id
    job.connector_id = connector_id
    job.index_name = index_name
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
    existing_index_name = "foo"
    to_be_deleted_index_name = "bar"
    connector = mock_connector(index_name=existing_index_name)
    sync_job = mock_sync_job(index_name=to_be_deleted_index_name)
    another_sync_job = mock_sync_job(index_name=existing_index_name)

    all_connectors.return_value = AsyncGeneratorFake([connector])
    supported_connectors.return_value = AsyncGeneratorFake([connector])
    fetch_by_id.return_value = connector
    orphaned_jobs.return_value = AsyncGeneratorFake([sync_job, another_sync_job])
    stuck_jobs.return_value = AsyncGeneratorFake([sync_job])
    delete_jobs.return_value = {"deleted": 1, "failures": [], "total": 1}

    service = create_service()
    asyncio.get_event_loop().call_later(0.5, service.stop)
    await service.run()

    assert delete_indices.call_args_list == [call(indices=[to_be_deleted_index_name])]
    assert delete_jobs.call_args_list == [
        call(job_ids=[sync_job.job_id, another_sync_job.job_id])
    ]
    assert connector._sync_done.call_args_list == [
        call(job=sync_job, result={}, exception=STUCK_JOB_ERROR)
    ]
