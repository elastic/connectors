#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import functools
import os
from collections import defaultdict
from copy import deepcopy
from unittest.mock import Mock

import pytest

from connectors.config import load_config
from connectors.services.base import BaseService, MultiService, get_services

HERE = os.path.dirname(__file__)
FIXTURES_DIR = os.path.abspath(os.path.join(HERE, "..", "fixtures"))
CONFIG_FILE = os.path.join(FIXTURES_DIR, "config.yml")


def create_service(service_klass, config=None, config_file=None, idling=None):
    if config is None:
        config = load_config(config_file) if config_file else {}
    service = service_klass(config)
    service.idling = 0

    return service


async def run_service_with_stop_after(service, stop_after=0):
    def _stop_running_service_without_cancelling():
        service.running = False

    async def _terminate():
        if stop_after == 0:
            # so we actually want the service
            # to run current loop without interruption
            asyncio.get_event_loop().call_soon(_stop_running_service_without_cancelling)
        else:
            # but if stop_after is provided we want to
            # interrupt the service after the timeout
            await asyncio.sleep(stop_after)
            service.stop()

        await asyncio.sleep(0)

    await asyncio.gather(service.run(), _terminate())


async def create_and_run_service(
    service_klass, config=None, config_file=CONFIG_FILE, stop_after=0
):
    service = create_service(service_klass, config=config, config_file=config_file)
    await run_service_with_stop_after(service, stop_after)


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


config = {
    "elasticsearch": {},
    "service": {},
}


def test_parse_connectors_with_no_connectors():
    local_config = deepcopy(config)
    service = BaseService(local_config)
    assert not service.connectors


def test_parse_connectors():
    local_config = deepcopy(config)
    local_config["connectors"] = [
        {"connector_id": "foo", "service_type": "bar"},
        {"connector_id": "baz", "service_type": "qux"},
    ]

    service = BaseService(local_config)
    assert service.connectors["foo"]["connector_id"] == "foo"
    assert service.connectors["foo"]["service_type"] == "bar"
    assert service.connectors["baz"]["connector_id"] == "baz"
    assert service.connectors["baz"]["service_type"] == "qux"


def test_parse_connectors_with_duplicate_connectors():
    local_config = deepcopy(config)
    local_config["connectors"] = [
        {"connector_id": "foo", "service_type": "bar"},
        {"connector_id": "foo", "service_type": "baz"},
    ]

    service = BaseService(local_config)
    assert len(service.connectors) == 1
    assert service.connectors["foo"]["connector_id"] == "foo"
    assert service.connectors["foo"]["service_type"] == "baz"


def test_parse_connectors_with_incomplete_connector():
    local_config = deepcopy(config)
    local_config["connectors"] = [
        {"connector_id": "foo", "service_type": "bar"},
        {"service_type": "qux"},
    ]

    service = BaseService(local_config)
    assert len(service.connectors) == 1
    assert service.connectors["foo"]["connector_id"] == "foo"
    assert service.connectors["foo"]["service_type"] == "bar"


def test_parse_connectors_with_deprecated_config_and_new_config():
    local_config = deepcopy(config)
    local_config["connectors"] = [{"connector_id": "foo", "service_type": "bar"}]
    local_config["connector_id"] = "deprecated"
    local_config["service_type"] = "deprecated"

    service = BaseService(local_config)
    assert len(service.connectors) == 1
    assert service.connectors["foo"]["connector_id"] == "foo"
    assert service.connectors["foo"]["service_type"] == "bar"
    assert "deprecated" not in service.connectors


def test_parse_connectors_with_deprecated_config():
    local_config = deepcopy(config)
    local_config["connector_id"] = "deprecated"
    local_config["service_type"] = "deprecated"

    service = BaseService(local_config)
    assert len(service.connectors) == 1
    assert service.connectors["deprecated"]["connector_id"] == "deprecated"
    assert service.connectors["deprecated"]["service_type"] == "deprecated"


def test_override_es_config():
    connector_api_key = "connector_api_key"
    config = {
        "elasticsearch": {
            "username": "username",
            "password": "password",
            "api_key": "global_api_key",
        },
        "service": {"idling": 30},
        "sources": [],
        "connectors": [
            {
                "connector_id": "foo",
                "service_type": "bar",
                "api_key": connector_api_key,
            }
        ],
    }

    service = BaseService(config)
    connector = Mock()
    connector.id = "foo"
    override_config = service._override_es_config(connector)
    assert "username" not in override_config
    assert "password" not in override_config
    assert override_config["api_key"] == connector_api_key
