#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import pytest

from connectors.services.content_sync_job_execution import (
    DEFAULT_MAX_CONCURRENT_CONTENT_SYNCS,
    ContentSyncJobExecutionService,
)
from tests.services.test_base import create_service


@pytest.mark.parametrize(
    "service_config, expected_max_concurrency",
    [
        ({"max_concurrent_content_syncs": 3, "max_concurrent_syncs": 2}, 3),
        ({"max_concurrent_syncs": 2}, 2),
        ({}, DEFAULT_MAX_CONCURRENT_CONTENT_SYNCS),
    ],
)
def test_content_sync_max_concurrency(service_config, expected_max_concurrency):
    config = {
        "elasticsearch": {},
        "service": {"idling": 30} | service_config,
        "sources": [],
    }
    service = create_service(ContentSyncJobExecutionService, config=config)
    assert service.max_concurrency == expected_max_concurrency
