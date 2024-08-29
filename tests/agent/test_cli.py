import asyncio
from unittest.mock import AsyncMock, Mock, patch

import os
import signal
import pytest

from connectors.agent.cli import main

def test_main():
    async def kill():
        await asyncio.sleep(0.2)
        os.kill(os.getpid(), signal.SIGTERM)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(kill())

    loop.close()
