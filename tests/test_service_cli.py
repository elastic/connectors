#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import logging
import os
import signal
from io import StringIO
from unittest import mock
from unittest.mock import patch

import pytest

from connectors import __version__
from connectors.service_cli import main, run

HERE = os.path.dirname(__file__)
FIXTURES_DIR = os.path.abspath(os.path.join(HERE, "fixtures"))
CONFIG = os.path.join(FIXTURES_DIR, "config.yml")


def test_main(catch_stdout):
    assert main(["--version"]) == 0
    catch_stdout.seek(0)
    assert catch_stdout.read().strip() == __version__


def test_main_and_kill(mock_responses):
    headers = {"X-Elastic-Product": "Elasticsearch"}
    host = "http://localhost:9200"

    mock_responses.get(host, headers=headers)
    mock_responses.head(f"{host}/.elastic-connectors", headers=headers)
    mock_responses.head(f"{host}/.elastic-connectors-sync-jobs", headers=headers)
    mock_responses.get(
        f"{host}/_ingest/pipeline/ent-search-generic-ingestion", headers=headers
    )

    async def kill():
        await asyncio.sleep(0.2)
        os.kill(os.getpid(), signal.SIGTERM)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(kill())

    main([])


def test_run(mock_responses, set_env):
    args = mock.MagicMock()
    args.log_level = "DEBUG"
    args.config_file = CONFIG
    args.action = ["list"]
    with patch("sys.stdout", new=StringIO()) as patched_stdout:
        assert run(args) == 0

        output = patched_stdout.getvalue().strip()

        assert "Registered connectors:" in output
        assert "- Fakey" in output
        assert "- Large Fake" in output
        assert "Bye" in output


def test_config_action(mock_responses, set_env):
    args = mock.MagicMock()
    args.log_level = "DEBUG"
    args.config_file = CONFIG
    args.action = ["config"]
    args.service_type = "fake"
    with patch("sys.stdout", new=StringIO()) as patched_stdout:
        result = run(args)
        output = patched_stdout.getvalue().strip()
        assert result == 0
        assert "Could not find a connector for service type" not in output
        assert "Getting default configuration for service type fake" in output


def test_run_snowflake(mock_responses, set_env):
    args = mock.MagicMock()
    args.log_level = "DEBUG"
    args.config_file = CONFIG
    args.action = ["list", "poll"]
    with patch("sys.stdout", new=StringIO()) as patched_stdout:
        assert run(args) == -1
        output = patched_stdout.getvalue().strip()
        assert "Cannot use the `list` action with other actions" in output


@patch("connectors.service_cli.set_logger")
@patch("connectors.service_cli.load_config", side_effect=Exception("something went wrong"))
def test_main_with_invalid_configuration(load_config, set_logger):
    args = mock.MagicMock()
    args.log_level = logging.DEBUG  # should be ignored!
    args.filebeat = True

    with pytest.raises(Exception):
        run(args)

    set_logger.assert_called_with(logging.INFO, filebeat=True)
