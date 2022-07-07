#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import json
from datetime import datetime

from aioresponses import aioresponses
from elasticsearch import AsyncElasticsearch
import pytest

from connectors.byoc import e2str, Status, iso_utc, SyncJob, JobStatus


@pytest.fixture
def mock_responses():
    with aioresponses() as m:
        yield m


def test_e2str():
    # The BYOC protocol uses lower case
    assert e2str(Status.NEEDS_CONFIGURATION) == "needs_configuration"


def test_utc():
    # All dates are in ISO 8601 UTC so we can serialize them
    now = datetime.utcnow()
    then = json.loads(json.dumps({"date": iso_utc(when=now)}))["date"]
    assert now.isoformat() == then


@pytest.mark.asyncio
async def test_sync_job(mock_responses):
    client = AsyncElasticsearch(hosts=["http://nowhere.com:9200"])

    job = SyncJob("connector-id", client)

    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors-sync-jobs/_doc",
        payload={"_id": "1"},
        headers=headers,
    )

    mock_responses.put(
        "http://nowhere.com:9200/.elastic-connectors-sync-jobs/_doc/1",
        payload={"_id": "1"},
        headers=headers,
    )
    await job.start()
    assert job.status == JobStatus.IN_PROGRESS
    assert job.job_id is not None

    await job.done(12, 34)
    assert job.status == JobStatus.COMPLETED
