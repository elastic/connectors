#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import functools
from collections import defaultdict

import pytest

from connectors.services.base import BaseService, MultiService, get_services


class StubService:
    def __init__(self):
        self.running = False
        self.cancelled = False
        self.exploding = False
        self.run_sleep_delay = 0
        self.stopped = False
        self.handle_cancellation = True

    async def run(self):
        if self.handle_cancellation:
            try:
                await self._run()
            except asyncio.CancelledError:
                self.running = False
                self.cancelled = True
        else:
            await self._run()

    async def _run(self):
        self.running = True
        while self.running:
            if self.exploding:
                raise Exception("Something went wrong")
            await asyncio.sleep(self.run_sleep_delay)

    def stop(self):
        self.running = False
        self.stopped = True

    def explode(self):
        self.exploding = True


@pytest.mark.asyncio
async def test_multiservice_run_stops_all_services_when_one_raises_exception():
    service_1 = StubService()
    service_2 = StubService()
    service_3 = StubService()

    multiservice = MultiService(service_1, service_2, service_3)

    asyncio.get_event_loop().call_later(0.1, service_1.explode)

    await multiservice.run()

    assert not service_1.cancelled
    assert service_2.cancelled
    assert service_3.cancelled


@pytest.mark.asyncio
async def test_multiservice_run_stops_all_services_when_shutdown_happens():
    service_1 = StubService()
    service_2 = StubService()
    service_3 = StubService()

    multiservice = MultiService(service_1, service_2, service_3)

    asyncio.get_event_loop().call_later(
        0.1, functools.partial(multiservice.shutdown, "SIGTERM")
    )

    await multiservice.run()

    assert service_1.stopped
    assert service_2.stopped
    assert service_3.stopped


@pytest.mark.asyncio
async def test_registry():
    ran = []

    # creating a class using the BaseService as its base
    # will register the class using its name
    class TestService(BaseService):
        async def run(self):
            nonlocal ran
            ran.append(self.name)

    class SomeService(TestService):
        name = "kool"

    class SomeOtherService(TestService):
        name = "beans"

    # and make it available when we call get_services()
    multiservice = get_services(["kool", "beans"], config=defaultdict(dict))

    await multiservice.run()
    assert ran == ["kool", "beans"]


@pytest.mark.asyncio
async def test_multiservice_stop_gracefully_stops_service_that_takes_too_long_to_run():
    service_1 = StubService()

    service_2 = StubService()
    service_2.run_sleep_delay = 0.3  # large enough

    multiservice = MultiService(service_1, service_2)

    asyncio.get_event_loop().call_later(
        0.1, functools.partial(multiservice.shutdown, "SIGTERM")
    )

    await multiservice.run()

    assert service_1.stopped
    assert service_2.stopped
    assert (
        not service_2.cancelled
    )  # we're not supposed to cancel it as it's already stopping
