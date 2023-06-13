#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from unittest.mock import patch

import pytest

from connectors.preflight_check import PreflightCheck
from connectors.protocol import CONNECTORS_INDEX, JOBS_INDEX

headers = {"X-Elastic-Product": "Elasticsearch"}
host = "http://localhost:9200"
config = {
    "elasticsearch": {
        "host": host,
        "username": "elastic",
        "password": "changeme",
        "max_wait_duration": 0.1,
        "initial_backoff_duration": 0.1,
    },
    "service": {"preflight_max_attempts": 4, "preflight_idle": 0.1},
}


def mock_es_info(mock_responses, healthy=True, repeat=False):
    status = 200 if healthy else 503
    mock_responses.get(host, status=status, headers=headers, repeat=repeat)


def mock_index_exists(mock_responses, index, exist=True, repeat=False):
    status = 200 if exist else 404
    mock_responses.head(f"{host}/{index}", status=status, repeat=repeat)


@pytest.mark.asyncio
async def test_es_unavailable(mock_responses):
    mock_es_info(mock_responses, healthy=False, repeat=True)
    preflight = PreflightCheck(config)
    result = await preflight.run()
    assert result is False


@pytest.mark.asyncio
async def test_connector_index_missing(mock_responses):
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONNECTORS_INDEX, exist=False)
    mock_index_exists(mock_responses, JOBS_INDEX, exist=True)
    preflight = PreflightCheck(config)
    result = await preflight.run()
    assert result is False


@pytest.mark.asyncio
async def test_job_index_missing(mock_responses):
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONNECTORS_INDEX, exist=True)
    mock_index_exists(mock_responses, JOBS_INDEX, exist=False)
    preflight = PreflightCheck(config)
    result = await preflight.run()
    assert result is False


@pytest.mark.asyncio
async def test_both_indices_missing(mock_responses):
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONNECTORS_INDEX, exist=False)
    mock_index_exists(mock_responses, JOBS_INDEX, exist=False)
    preflight = PreflightCheck(config)
    result = await preflight.run()
    assert result is False


@pytest.mark.asyncio
async def test_pass(mock_responses):
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONNECTORS_INDEX)
    mock_index_exists(mock_responses, JOBS_INDEX)
    preflight = PreflightCheck(config)
    result = await preflight.run()
    assert result is True


@pytest.mark.asyncio
async def test_es_transient_error(mock_responses):
    mock_es_info(mock_responses, healthy=False)
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONNECTORS_INDEX)
    mock_index_exists(mock_responses, JOBS_INDEX)
    preflight = PreflightCheck(config)
    result = await preflight.run()
    assert result is True


@pytest.mark.asyncio
async def test_index_exist_transient_error(mock_responses):
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONNECTORS_INDEX, exist=False)
    mock_index_exists(mock_responses, CONNECTORS_INDEX, repeat=True)
    mock_index_exists(mock_responses, JOBS_INDEX, exist=False)
    mock_index_exists(mock_responses, JOBS_INDEX, repeat=True)
    preflight = PreflightCheck(config)
    result = await preflight.run()
    assert result is True


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_native_config_is_warned(patched_logger, mock_responses):
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONNECTORS_INDEX)
    mock_index_exists(mock_responses, JOBS_INDEX)
    local_config = config.copy()
    local_config["native_service_types"] = ["foo", "bar"]
    preflight = PreflightCheck(local_config)
    result = await preflight.run()
    assert result is True
    patched_logger.warning.assert_any_call(
        "The configuration 'native_service_types' has been deprecated. Please remove this configuration."
    )
    patched_logger.warning.assert_any_call(
        "Native Connectors are only supported internal to Elastic Cloud deployments, which this process is not."
    )
    patched_logger.warning.assert_any_call(
        "Please update your config.yml to explicitly configure a 'connector_id' and a 'service_type'"
    )


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_native_config_is_forced(patched_logger, mock_responses):
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONNECTORS_INDEX)
    mock_index_exists(mock_responses, JOBS_INDEX)
    local_config = config.copy()
    local_config["native_service_types"] = ["foo", "bar"]
    local_config["_force_allow_native"] = True
    preflight = PreflightCheck(local_config)
    result = await preflight.run()
    assert result is True
    patched_logger.warning.assert_not_called()


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_client_config(patched_logger, mock_responses):
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONNECTORS_INDEX)
    mock_index_exists(mock_responses, JOBS_INDEX)
    local_config = config.copy()
    local_config["connector_id"] = "foo"
    local_config["service_type"] = "bar"
    preflight = PreflightCheck(local_config)
    result = await preflight.run()
    assert result is True
    patched_logger.warning.assert_not_called()
