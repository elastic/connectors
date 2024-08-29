import asyncio
from unittest.mock import ANY, AsyncMock, Mock, patch

from connectors.services.base import ServiceAlreadyRunningError
import pytest
from connectors.agent.service_manager import ConnectorServiceManager


@pytest.fixture(autouse=True)
def config_mock():
    config = Mock()

    config.get.return_value = { "service": { "idling": 123, "heartbeat": 5}, "elasticsearch": {}, "sources": []}

    return config

def multi_service_mock():
    return AsyncMock()


class StubMultiService:
    def __init__(self):
        self.running = False

    async def run(self):
        self.running = True
        while (self.running):
            await asyncio.sleep(0)

    def stop(self):
        self.running = False

@pytest.mark.asyncio
@patch("connectors.agent.service_manager.get_services")
async def test_cannot_run_same_service_manager_twice(patch_get_services, config_mock):
    service_manager = ConnectorServiceManager(config_mock)
    patch_get_services.return_value = StubMultiService()

    with pytest.raises(ServiceAlreadyRunningError):
        tasks = [asyncio.create_task(service_manager.run()) for _ in range(2)]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        # Cancel pending to clean up the tasks
        for task in pending:
            task.cancel()

        # Execute task results to cause an exception to be raised if any
        for task in done:
            task.result()
 
