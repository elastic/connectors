#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from unittest.mock import Mock, patch

import pytest

from connectors.agent.service_manager import ConnectorServiceManager
from connectors.services.base import ServiceAlreadyRunningError


@pytest.fixture(autouse=True)
def config_mock() -> Mock:
    config = Mock()

    config.get.return_value = {
        "service": {"idling": 123, "heartbeat": 5},
        "elasticsearch": {},
        "sources": [],
    }

    return config


class StubMultiService:
    def __init__(self) -> None:
        self.running_stop = asyncio.Event()
        self.has_ran = False
        self.has_shutdown = False

    async def run(self) -> None:
        self.has_ran = True
        self.running_stop.clear()
        await self.running_stop.wait()

    def shutdown(self, sig) -> None:
        self.has_shutdown = True
        self.running_stop.set()


@pytest.mark.asyncio
@patch("connectors.agent.service_manager.get_services", return_value=StubMultiService())
async def test_run_and_stop_work_as_intended(patch_get_services, config_mock) -> None:
    service_manager = ConnectorServiceManager(config_mock)

    async def stop_service_after_timeout():
        await asyncio.sleep(0.1)
        service_manager.stop()

    await asyncio.gather(service_manager.run(), stop_service_after_timeout())

    assert patch_get_services.return_value.has_ran
    assert patch_get_services.return_value.has_shutdown


@pytest.mark.asyncio
@patch("connectors.agent.service_manager.get_services", return_value=StubMultiService())
async def test_restart_starts_another_multiservice(
    patch_get_services, config_mock
) -> None:
    service_manager = ConnectorServiceManager(config_mock)

    async def stop_service_after_timeout():
        await asyncio.sleep(0.1)
        service_manager.restart()
        await asyncio.sleep(0.1)
        service_manager.stop()

    await asyncio.gather(service_manager.run(), stop_service_after_timeout())

    assert patch_get_services.called
    assert patch_get_services.call_count == 2


@pytest.mark.asyncio
@patch("connectors.agent.service_manager.get_services", return_value=StubMultiService())
async def test_cannot_run_same_service_manager_twice(
    patch_get_services, config_mock
) -> None:
    service_manager = ConnectorServiceManager(config_mock)

    with pytest.raises(ServiceAlreadyRunningError):
        tasks = [asyncio.create_task(service_manager.run()) for _ in range(2)]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        # Cancel pending to clean up the tasks
        for task in pending:
            task.cancel()

        # Execute task results to cause an exception to be raised if any
        for task in done:
            task.result()
