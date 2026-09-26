#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import logging
import os
import signal
from unittest.mock import ANY, AsyncMock, Mock, call, patch

import pytest
from click import ClickException, UsageError
from click.testing import CliRunner

from connectors import __version__
from connectors.service_cli import _start_service, get_event_loop, main

SUCCESS_EXIT_CODE = 0
CLICK_EXCEPTION_EXIT_CODE = ClickException.exit_code
USAGE_ERROR_EXIT_CODE = UsageError.exit_code

HERE = os.path.dirname(__file__)
FIXTURES_DIR = os.path.abspath(os.path.join(HERE, "fixtures"))
CONFIG = os.path.join(FIXTURES_DIR, "config.yml")


def test_main_exits_on_sigterm(mock_responses):
    headers = {"X-Elastic-Product": "Elasticsearch"}
    host = "http://localhost:9200"

    mock_responses.get(host, headers=headers)
    mock_responses.head(f"{host}/.elastic-connectors", headers=headers)
    mock_responses.head(f"{host}/.elastic-connectors-sync-jobs", headers=headers)
    mock_responses.get(
        f"{host}/_ingest/pipeline/search-default-ingestion", headers=headers
    )

    async def kill():
        await asyncio.sleep(0.2)
        os.kill(os.getpid(), signal.SIGTERM)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(kill())

    CliRunner().invoke(main, [])
    loop.close()


@pytest.mark.parametrize("option", ["-v", "--version"])
def test_version_action(option):
    runner = CliRunner()
    result = runner.invoke(main, [option])

    assert result.exit_code == SUCCESS_EXIT_CODE
    assert __version__ in result.output


@pytest.mark.asyncio
@patch("connectors.service_cli.PreflightCheck")
@patch("connectors.service_cli.get_services")
async def test_shutdown_signal_registered(
    patch_get_services, patch_preflight_check, set_env
):
    patch_multi_service = Mock()
    patch_get_services.return_value = patch_multi_service
    patch_multi_service.run = AsyncMock()
    patch_preflight_check.return_value.run = AsyncMock(return_value=(True, False))
    loop = Mock()
    await _start_service([], {"elasticsearch": {}}, loop)
    assert loop.add_signal_handler.has_calls(
        [call(signal.SIGINT, ANY), call(signal.SIGTERM, ANY)]
    )


def test_list_action(set_env):
    runner = CliRunner()

    config_file = CONFIG
    action = "list"

    result = runner.invoke(
        main,
        ["--config-file", config_file, "--action", action],
    )

    assert result.exit_code == SUCCESS_EXIT_CODE

    output = result.output

    assert "Registered connectors:" in output
    assert "- Fakey" in output
    assert "- Large Fake" in output
    assert "Bye" in output


def test_config_with_service_type_actions(set_env):
    runner = CliRunner()

    config_file = CONFIG
    action = "config"
    service_type = "fake"

    result = runner.invoke(
        main,
        [
            "--config-file",
            config_file,
            "--action",
            action,
            "--service-type",
            service_type,
        ],
    )

    assert result.exit_code == SUCCESS_EXIT_CODE

    output = result.output

    assert "Could not find a connector for service type" not in output
    assert "Getting default configuration for service type fake" in output


def test_list_cannot_be_used_with_other_actions(set_env):
    runner = CliRunner()

    config_file = CONFIG
    first_action = "cleanup"
    second_action = "list"

    result = runner.invoke(
        main,
        [
            "--config-file",
            config_file,
            "--action",
            first_action,
            "--action",
            second_action,
        ],
    )

    assert result.exit_code == USAGE_ERROR_EXIT_CODE
    assert "Cannot use the `list` action with other actions" in result.output


def test_config_cannot_be_used_with_other_actions(set_env):
    runner = CliRunner()

    config_file = CONFIG
    first_action = "cleanup"
    second_action = "config"

    result = runner.invoke(
        main,
        [
            "--config-file",
            config_file,
            "--action",
            first_action,
            "--action",
            second_action,
        ],
    )

    assert result.exit_code == USAGE_ERROR_EXIT_CODE
    assert "Cannot use the `config` action with other actions" in result.output


@patch("connectors.service_cli.set_logger")
@patch(
    "connectors.service_cli.load_config", side_effect=Exception("something went wrong")
)
def test_main_with_invalid_configuration(load_config, set_logger):
    runner = CliRunner()

    log_level = "DEBUG"  # should be ignored!

    result = runner.invoke(main, ["--log-level", log_level, "--filebeat"])

    assert result.exit_code == CLICK_EXCEPTION_EXIT_CODE
    set_logger.assert_called_with(logging.INFO, filebeat=True)


def test_unknown_service_type(set_env):
    runner = CliRunner()

    config_file = CONFIG
    action = "config"
    unknown_service_type = "unknown"

    result = runner.invoke(
        main,
        [
            "--config-file",
            config_file,
            "--action",
            action,
            "--service-type",
            unknown_service_type,
        ],
    )

    assert result.exit_code == USAGE_ERROR_EXIT_CODE
    assert (
        f"Could not find a connector for service type {unknown_service_type}"
        in result.output
    )


@patch("connectors.service_cli._get_uvloop")
@patch("connectors.service_cli.asyncio")
def test_uvloop_success(patched_asyncio, patched_uvloop):
    get_event_loop(True)
    assert patched_asyncio.set_event_loop_policy.called_once_with(
        patched_uvloop.EventLoopPolicy()
    )


@patch("connectors.service_cli._get_uvloop", side_effect=Exception("import fails"))
@patch("connectors.service_cli.asyncio")
@patch("connectors.service_cli.logger")
def test_uvloop_error(patched_logger, patched_asyncio, patched_uvloop):
    get_event_loop(True)
    patched_logger.warning.assert_any_call(
        "Unable to enable uvloop: import fails. Running with default event loop"
    )


def test_filebeat_formats_first_log_lines_as_ecs(set_env):
    """https://github.com/elastic/connectors/issues/897

    With --filebeat, every log line -- including the ones emitted before the
    config file is loaded -- must be ECS JSON, not the plain [FMWK] format.
    """
    import io
    import json

    from connectors_sdk.logger import logger as sdk_logger

    stream = io.StringIO()
    main_handler = sdk_logger.handlers[0]
    old_stream, old_formatter = main_handler.stream, main_handler.formatter
    main_handler.setStream(stream)
    try:
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["--filebeat", "--config-file", CONFIG, "--action", "list"],
        )
    finally:
        main_handler.setStream(old_stream)
        main_handler.setFormatter(old_formatter)

    assert result.exit_code == SUCCESS_EXIT_CODE

    lines = [line for line in stream.getvalue().splitlines() if line.strip()]
    assert lines, "expected log output on the logger stream"

    for line in lines:
        entry = json.loads(line)  # raises if the line is not ECS JSON
        assert "log.level" in entry
        assert "message" in entry

    assert "Running connector service version" in lines[0]
