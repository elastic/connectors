#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
import signal
from unittest.mock import AsyncMock, patch

from connectors_service.agent.cli import main


@patch("connectors_service.agent.cli.ConnectorsAgentComponent", return_value=AsyncMock())
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
