from unittest.mock import MagicMock, patch

import pytest
import asyncio

from connectors.agent.component import ConnectorsAgentComponent

class StubMultiService:
    def __init__(self):
        self.running_stop = asyncio.Event()
        self.has_ran = False
        self.has_shutdown = False

    async def run(self):
        self.has_ran = True
        self.running_stop.clear()
        await self.running_stop.wait()

    def shutdown(self, sig):
        self.has_shutdown = True
        self.running_stop.set()



@pytest.mark.asyncio
@patch("connectors.agent.component.MultiService", return_value=StubMultiService())
@patch("connectors.agent.component.new_v2_from_reader", return_value=MagicMock())
async def test_try_update_without_auth_data(stub_multi_service, patch_new_v2_from_reader):
    component = ConnectorsAgentComponent()

    async def stop_after_timeout():
        await asyncio.sleep(0.1)
        component.stop("SIGINT")

    await asyncio.gather(component.run(), stop_after_timeout())
    
    assert stub_multi_service.has_ran
    assert stub_multi_service.has_shutdown
