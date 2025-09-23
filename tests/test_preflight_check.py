#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from copy import deepcopy
from typing import Dict, List, Union
from unittest.mock import patch

import pytest

from connectors.preflight_check import PreflightCheck
from connectors.protocol import CONCRETE_CONNECTORS_INDEX, CONCRETE_JOBS_INDEX

connectors_version = "1.2.3.4"
headers = {"X-Elastic-Product": "Elasticsearch"}
host = "http://localhost:9200"
config: Dict[
    str, Union[Dict[str, float], Dict[str, Union[float, str]], List[Dict[str, str]]]
] = {
    "elasticsearch": {
        "host": host,
        "username": "elastic",
        "password": "changeme",
        "max_wait_duration": 0.1,
        "initial_backoff_duration": 0.1,
    },
    "service": {"preflight_max_attempts": 4, "preflight_idle": 0.1},
    "connectors": [
        {
            "connector_id": "connector_1",
            "service_type": "some_type",
        },
    ],
}


def mock_es_info(
    mock_responses,
    healthy: bool = True,
    repeat: bool = False,
    es_version: str = connectors_version,
    serverless: bool = False,
) -> None:
    status = 200 if healthy else 503
    payload = {
        "version": {
            "number": es_version,
            "build_flavor": ("serverless" if serverless else "default"),
        }
    }
    mock_responses.get(
        host, status=status, headers=headers, repeat=repeat, payload=payload
    )


def mock_index_exists(
    mock_responses, index, exist: bool = True, repeat: bool = False
) -> None:
    status = 200 if exist else 404
    mock_responses.head(f"{host}/{index}", status=status, repeat=repeat)


def mock_index(mock_responses, index, doc_id, repeat: bool = False) -> None:
    status = 200
    mock_responses.put(f"{host}/{index}/_doc/{doc_id}", status=status, repeat=repeat)


def mock_create_index(mock_responses, index, repeat: bool = False) -> None:
    status = 200
    mock_responses.put(f"{host}/{index}", status=status, repeat=repeat)


def mock_delete(mock_responses, index, doc_id, repeat: bool = False) -> None:
    status = 200
    mock_responses.delete(f"{host}/{index}/_doc/{doc_id}", status=status, repeat=repeat)


@pytest.mark.asyncio
async def test_es_unavailable(mock_responses) -> None:
    mock_es_info(mock_responses, healthy=False, repeat=True)
    preflight = PreflightCheck(config, connectors_version)
    result = await preflight.run()
    assert result == (False, False)


@pytest.mark.asyncio
async def test_connectors_index_missing(mocker, mock_responses) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX, exist=False)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX, exist=True)
    mock_create_index(mock_responses, CONCRETE_CONNECTORS_INDEX)
    preflight = PreflightCheck(config, connectors_version)
    spy = mocker.spy(preflight.es_management_client.client.indices, "create")
    result = await preflight.run()
    assert result == (True, False)
    spy.assert_called_with(index=CONCRETE_CONNECTORS_INDEX)


@pytest.mark.asyncio
async def test_jobs_index_missing(mocker, mock_responses) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX, exist=True)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX, exist=False)
    mock_create_index(mock_responses, CONCRETE_JOBS_INDEX)
    preflight = PreflightCheck(config, connectors_version)
    spy = mocker.spy(preflight.es_management_client.client.indices, "create")
    await preflight.run()
    spy.assert_called_with(index=CONCRETE_JOBS_INDEX)


@pytest.mark.asyncio
async def test_both_indices_missing(mocker, mock_responses) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX, exist=False)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX, exist=False)
    mock_create_index(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_create_index(mock_responses, CONCRETE_JOBS_INDEX)
    preflight = PreflightCheck(config, connectors_version)
    spy = mocker.spy(preflight.es_management_client.client.indices, "create")
    await preflight.run()
    assert spy.call_count == 2


@pytest.mark.asyncio
async def test_pass(mock_responses) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    preflight = PreflightCheck(config, connectors_version)
    result = await preflight.run()
    assert result == (True, False)


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_pass_serverless(patched_logger, mock_responses) -> None:
    mock_es_info(mock_responses, serverless=True)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    preflight = PreflightCheck(config, connectors_version)
    result = await preflight.run()
    assert result == (True, True)
    patched_logger.info.assert_any_call(
        "Elasticsearch server is serverless, skipping version compatibility check."
    )


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_pass_serverless_mismatched_versions(
    patched_logger, mock_responses
) -> None:
    mock_es_info(mock_responses, es_version="2.0.0-SNAPSHOT", serverless=True)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    preflight = PreflightCheck(config, connectors_version)
    result = await preflight.run()
    assert result == (True, True)
    patched_logger.info.assert_any_call(
        "Elasticsearch server is serverless, skipping version compatibility check."
    )


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
@pytest.mark.parametrize(
    "es_version, expected_log",
    [
        (
            "2.0.0-SNAPSHOT",
            f"Elasticsearch 2.0.0-SNAPSHOT and Connectors {connectors_version} are incompatible: major versions are different",
        ),
        (
            "1.0.3",
            f"Elasticsearch 1.0.3 and Connectors {connectors_version} are incompatible: Elasticsearch minor version is older than Connectors",
        ),
    ],
)
async def test_fail_mismatched_version(
    patched_logger, mock_responses, es_version: str, expected_log
) -> None:
    mock_es_info(mock_responses, es_version=es_version)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    preflight = PreflightCheck(config, connectors_version)
    result = await preflight.run()
    assert result == (False, False)
    patched_logger.critical.assert_any_call(expected_log)


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
@pytest.mark.parametrize(
    "es_version, expected_log",
    [
        (
            "1.3.3",
            f"Elasticsearch 1.3.3 minor version is newer than Connectors {connectors_version} which can lead to unexpected behavior",
        ),
        (
            "1.2.0",
            f"Elasticsearch 1.2.0 patch version is different than Connectors {connectors_version} which can lead to unexpected behavior",
        ),
    ],
)
async def test_warn_mismatched_version(
    patched_logger, mock_responses, es_version: str, expected_log
) -> None:
    mock_es_info(mock_responses, es_version=es_version)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    preflight = PreflightCheck(config, connectors_version)
    result = await preflight.run()
    assert result == (True, False)
    patched_logger.warning.assert_any_call(expected_log)


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
@pytest.mark.parametrize(
    "es_version, connectors_version, expected_log",
    [
        (
            "1.2.3-SNAPSHOT",
            "1.2.3.4",
            "Elasticsearch 1.2.3-SNAPSHOT and Connectors 1.2.3.4 are compatible",
        ),
        (
            "1.2.3",
            "1.2.3.0",
            "Elasticsearch 1.2.3 and Connectors 1.2.3.0 are compatible",
        ),
        (
            "1.2.3",
            "1.2.3+abc",
            "Elasticsearch 1.2.3 and Connectors 1.2.3+abc are compatible",
        ),
        (
            "1.2.3",
            "1.2.3",
            "Elasticsearch 1.2.3 and Connectors 1.2.3 are compatible",
        ),
        (
            "1.2.3-SNAPSHOT",
            "1.2.3",
            "Elasticsearch 1.2.3-SNAPSHOT and Connectors 1.2.3 are compatible",
        ),
        (
            "1.2.3-beta1",
            "1.2.3",
            "Elasticsearch 1.2.3-beta1 and Connectors 1.2.3 are compatible",
        ),
        (
            "1.2.3-rc1",
            "1.2.3+build123",
            "Elasticsearch 1.2.3-rc1 and Connectors 1.2.3+build123 are compatible",
        ),
    ],
)
async def test_pass_mismatched_version(
    patched_logger, mock_responses, es_version: str, connectors_version, expected_log
) -> None:
    mock_es_info(mock_responses, es_version=es_version)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    preflight = PreflightCheck(config, connectors_version)
    result = await preflight.run()
    assert result == (True, False)
    patched_logger.info.assert_any_call(expected_log)


@pytest.mark.asyncio
async def test_es_transient_error(mock_responses) -> None:
    mock_es_info(mock_responses, healthy=False)
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    preflight = PreflightCheck(config, connectors_version)
    result = await preflight.run()
    assert result == (True, False)


@pytest.mark.asyncio
async def test_index_exist_transient_error(mock_responses) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX, exist=False)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX, repeat=True)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX, exist=False)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX, repeat=True)
    preflight = PreflightCheck(config, connectors_version)
    result = await preflight.run()
    assert result == (True, False)


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_native_config_is_warned(patched_logger, mock_responses) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    local_config = deepcopy(config)
    local_config["native_service_types"] = ["foo", "bar"]
    del local_config["connectors"]
    preflight = PreflightCheck(local_config, connectors_version)
    result = await preflight.run()
    assert result == (True, False)
    patched_logger.warning.assert_any_call(
        "The configuration 'native_service_types' has been deprecated. Please remove this configuration."
    )
    patched_logger.warning.assert_any_call(
        "Native Connectors are only supported internal to Elastic Cloud deployments, which this process is not."
    )
    patched_logger.warning.assert_any_call(
        "Please update your config.yml to configure at least one connector"
    )


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_native_config_is_forced(patched_logger, mock_responses) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    local_config = deepcopy(config)
    local_config["native_service_types"] = ["foo", "bar"]
    local_config["_force_allow_native"] = True
    preflight = PreflightCheck(local_config, connectors_version)
    result = await preflight.run()
    assert result == (True, False)
    patched_logger.warning.assert_not_called()


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_client_config(patched_logger, mock_responses) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    local_config = deepcopy(config)
    local_config["connectors"][0]["connector_id"] = "foo"
    local_config["connectors"][0]["service_type"] = "bar"
    preflight = PreflightCheck(local_config, connectors_version)
    result = await preflight.run()
    assert result == (True, False)
    patched_logger.warning.assert_not_called()


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_unmodified_default_config(patched_logger, mock_responses) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    local_config = deepcopy(config)
    local_config["connectors"][0]["connector_id"] = "changeme"
    local_config["connectors"][0]["service_type"] = "changeme"
    preflight = PreflightCheck(local_config, connectors_version)
    result = await preflight.run()
    assert result == (False, False)
    patched_logger.errorassert_any_call(
        "In your configuration, you must change 'connector_id' and 'service_type' to not be 'changeme'"
    )


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_missing_mode_config(patched_logger, mock_responses) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    local_config = deepcopy(config)
    del local_config["connectors"]
    preflight = PreflightCheck(local_config, connectors_version)
    result = await preflight.run()
    assert result == (False, False)
    patched_logger.errorassert_any_call("You must configure at least one connector")


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_extraction_service_enabled_and_found_writes_info_log(
    patched_logger, mock_responses
) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    local_config = deepcopy(config)
    local_config["extraction_service"] = {"host": "http://localhost:8090"}
    preflight = PreflightCheck(local_config, connectors_version)

    mock_responses.get(
        f"{local_config['extraction_service']['host']}/ping/", status=200
    )

    result = await preflight.run()
    assert result == (True, False)

    patched_logger.info.assert_any_call(
        f"Data extraction service found at {local_config['extraction_service']['host']}."
    )


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_extraction_service_enabled_but_missing_logs_warning(
    patched_logger, mock_responses
) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    local_config = deepcopy(config)
    local_config["extraction_service"] = {"host": "http://localhost:8090"}
    preflight = PreflightCheck(local_config, connectors_version)

    mock_responses.get(
        f"{local_config['extraction_service']['host']}/ping/", status=404
    )

    result = await preflight.run()
    assert result == (True, False)

    patched_logger.warning.assert_any_call(
        f"Data extraction service was found at {local_config['extraction_service']['host']} but health-check returned `404'."
    )


@pytest.mark.asyncio
@patch("connectors.preflight_check.logger")
async def test_extraction_service_enabled_but_missing_logs_critical(
    patched_logger, mock_responses
) -> None:
    mock_es_info(mock_responses)
    mock_index_exists(mock_responses, CONCRETE_CONNECTORS_INDEX)
    mock_index_exists(mock_responses, CONCRETE_JOBS_INDEX)
    local_config = deepcopy(config)
    local_config["extraction_service"] = {"host": "http://localhost:8090"}
    preflight = PreflightCheck(local_config, connectors_version)

    result = await preflight.run()
    assert result == (True, False)

    patched_logger.critical.assert_any_call(
        f"Expected to find a running instance of data extraction service at {local_config['extraction_service']['host']} but failed. Connection refused: GET {local_config['extraction_service']['host']}/ping/."
    )
