#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
import signal
from unittest.mock import AsyncMock, patch

from connectors.agent.cli import FAILURE_EXIT_CODE, main
from connectors.fips import FIPSModeError


@patch("connectors.agent.cli.ConnectorsAgentComponent", return_value=AsyncMock())
def test_main_responds_to_sigterm(patch_component):
    async def kill():
        await asyncio.sleep(0.2)
        os.kill(os.getpid(), signal.SIGTERM)

    loop = asyncio.new_event_loop()
    loop.create_task(kill())

    # No asserts here.
    # main() will block forever unless it's killed with a signal
    # This test succeeds if it exits, if it hangs it'll be killed by a timeout
    main()

    loop.close()


@patch("connectors.agent.cli.ConnectorsAgentComponent")
def test_main_exits_with_failure_when_component_stops_with_an_error(patch_component):
    component = AsyncMock()
    component.run.side_effect = FIPSModeError("OpenSSL is not in FIPS mode")
    patch_component.return_value = component

    assert main() == FAILURE_EXIT_CODE


@patch(
    "connectors.agent.cli.ConnectorsAgentComponent",
    side_effect=FIPSModeError("ELASTICSEARCH_CONNECTORS_FIPS_MODE is set to 'ture'"),
)
def test_main_exits_with_failure_when_fips_mode_cannot_be_read(patch_component):
    assert main() == FAILURE_EXIT_CODE


@patch("connectors.agent.cli.ConnectorsAgentComponent")
def test_main_exits_with_failure_on_any_unexpected_error(patch_component):
    component = AsyncMock()
    component.run.side_effect = RuntimeError("something went wrong")
    patch_component.return_value = component

    assert main() == FAILURE_EXIT_CODE
