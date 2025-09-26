#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import Mock

import pytest

from connectors.es.license import requires_platinum_license
from connectors.protocol import JobType


def mock_source_klass(is_premium):
    source_klass = Mock()
    source_klass.is_premium = Mock(return_value=is_premium)

    return source_klass


def mock_connector(document_level_security_enabled):
    connector = Mock()
    connector.features = Mock()
    connector.features.document_level_security_enabled = Mock(
        return_value=document_level_security_enabled
    )

    return connector


def mock_sync_job(job_type):
    sync_job = Mock()
    sync_job.job_type = job_type

    return sync_job


@pytest.mark.parametrize(
    "job_type, document_level_security_enabled, is_premium",
    [
        (JobType.UNSET, False, True),
        (JobType.ACCESS_CONTROL, True, False),
        (JobType.ACCESS_CONTROL, True, True),
    ],
)
def test_requires_platinum_license(
    job_type, document_level_security_enabled, is_premium
):
    sync_job = mock_sync_job(job_type)
    connector = mock_connector(document_level_security_enabled)
    source_klass = mock_source_klass(is_premium)

    assert requires_platinum_license(sync_job, connector, source_klass)


@pytest.mark.parametrize(
    "job_type, document_level_security_enabled, is_premium",
    [
        (JobType.FULL, True, False),
        (JobType.INCREMENTAL, True, False),
        (JobType.ACCESS_CONTROL, False, False),
    ],
)
def test_does_not_require_platinum_license(
    job_type, document_level_security_enabled, is_premium
):
    sync_job = mock_sync_job(job_type)
    connector = mock_connector(document_level_security_enabled)
    source_klass = mock_source_klass(is_premium)

    assert not requires_platinum_license(sync_job, connector, source_klass)
