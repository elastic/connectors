#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
import signal

from connectors.cli import main
from connectors import __version__


def test_main(catch_stdout):
    assert main(["--version"]) == 0
    catch_stdout.seek(0)
    assert catch_stdout.read().strip() == __version__


def test_main_and_kill(catch_stdout, patch_logger, mock_responses):
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.get("http://localhost:9200", headers=headers)

    async def kill():
        os.kill(os.getpid(), signal.SIGTERM)

    asyncio.ensure_future(kill())
    main([])
