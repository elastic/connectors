#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import functools

import pytest

from connectors.services.base import MultiService


class StubService:
    def __init__(self):
        self.running = False
        self.cancelled = False
        self.exploding = False
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
            await asyncio.sleep(0)

    def stop(self):
        self.running = False
        self.stopped = True

    def explode(self):
        self.exploding = True


@pytest.mark.asyncio
async def test_multiservice_run_stops_all_services_when_one_stops():
    service_1 = StubService()
    service_2 = StubService()
    service_3 = StubService()

    multiservice = MultiService(service_1, service_2, service_3)

    asyncio.get_event_loop().call_later(0.1, service_1.stop)

    await multiservice.run()

    assert not service_1.cancelled
    assert service_2.cancelled
    assert service_3.cancelled


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
async def test_multiservice_run_stops_all_services_when_shutdown_happens_and_some_dont_handle_cancellation():
    service_1 = StubService()
    service_2 = StubService()
    service_3 = StubService()

    service_3.handle_cancellation = False

    multiservice = MultiService(service_1, service_2, service_3)

    asyncio.get_event_loop().call_later(0.1, service_1.stop)

    await multiservice.run()

    assert service_1.stopped
    assert service_2.cancelled
    # We assert False for service_3 - it does not handle cancellation gracefully
    assert not service_3.cancelled
