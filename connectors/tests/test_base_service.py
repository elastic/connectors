#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""connectors.services.base"""
import asyncio
import functools
from collections import defaultdict

import pytest

from connectors.services.base import BaseService, MultiService, get_services


class TestMultiservice:
    """
    Multiservice
    """

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
    async def test_run_with_service_exception(self):
        """
        run() when one of services raises an error then all services are forcefully stopped
        """

        service_1 = TestMultiservice.StubService()
        service_2 = TestMultiservice.StubService()
        service_3 = TestMultiservice.StubService()

        multiservice = MultiService(service_1, service_2, service_3)

        asyncio.get_event_loop().call_later(0.1, service_1.explode)

        await multiservice.run()

        assert not service_1.cancelled
        assert service_2.cancelled
        assert service_3.cancelled

    @pytest.mark.asyncio
    async def test_run_with_graceful_shutdown(self):
        """
        run() when multiservice is shutdown then all services are gracefully stopped
        """

        service_1 = TestMultiservice.StubService()
        service_2 = TestMultiservice.StubService()
        service_3 = TestMultiservice.StubService()

        multiservice = MultiService(service_1, service_2, service_3)

        asyncio.get_event_loop().call_later(
            0.1, functools.partial(multiservice.shutdown, "SIGTERM")
        )

        await multiservice.run()

        assert service_1.stopped
        assert service_2.stopped
        assert service_3.stopped

    @pytest.mark.asyncio
    async def test_run_when_service_stops_too_slowly(
        self,
    ):
        """
        run() when service takes too long to stop then all services are gracefully stopped anyway
        """

        service_1 = TestMultiservice.StubService()

        service_2 = TestMultiservice.StubService()
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


@pytest.mark.asyncio
async def test_get_service_with_invalid_names():
    """
    get_services() when one of service names is invalid then raises an error
    """

    class SomeService(BaseService):
        name = "existing"

    with pytest.raises(KeyError) as e:
        _ = get_services(["existing", "nonexisting"], config=defaultdict(dict))

    assert e.match("nonexisting")


@pytest.mark.asyncio
async def test_get_service_with_valid_names():
    """
    get_services() when called with valid service names then runs these services
    """
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
