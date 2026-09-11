#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from unittest.mock import Mock, patch

import pytest

from connectors.agent.service_manager import ConnectorServiceManager
from connectors.fips import FIPSConfig, FIPSModeError
from connectors.services.base import ServiceAlreadyRunningError


@pytest.fixture(autouse=True)
def reset_fips_config():
    """Reset FIPS config state before and after each test."""
    FIPSConfig.reset()
    yield
    FIPSConfig.reset()


@pytest.fixture(autouse=True)
def config_mock():
    config = Mock()

    config.get.return_value = {
        "service": {"idling": 123, "heartbeat": 5},
        "elasticsearch": {},
        "sources": [],
    }

    return config


def _fips_config(fips_mode):
    return {
        "service": {"fips_mode": fips_mode},
        "sources": {
            "network_drive": "connectors.sources.network_drive:NetworkDriveDataSource",
            "sharepoint_server": "connectors.sources.sharepoint.sharepoint_server:SharepointServerDataSource",
            "slack": "connectors.sources.slack:SlackDataSource",
        },
    }


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
@patch("connectors.agent.service_manager.get_services", return_value=StubMultiService())
async def test_run_and_stop_work_as_intended(patch_get_services, config_mock):
    service_manager = ConnectorServiceManager(config_mock)

    async def stop_service_after_timeout():
        await asyncio.sleep(0.1)
        service_manager.stop()

    await asyncio.gather(service_manager.run(), stop_service_after_timeout())

    assert patch_get_services.return_value.has_ran
    assert patch_get_services.return_value.has_shutdown


@pytest.mark.asyncio
@patch("connectors.agent.service_manager.get_services", return_value=StubMultiService())
async def test_restart_starts_another_multiservice(patch_get_services, config_mock):
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
async def test_cannot_run_same_service_manager_twice(patch_get_services, config_mock):
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


def test_apply_fips_mode_off_keeps_all_connectors(config_mock):
    service_manager = ConnectorServiceManager(config_mock)

    config = service_manager._apply_fips_mode(_fips_config(False))

    assert FIPSConfig.is_fips_mode_enabled() is False
    assert set(config["sources"]) == {"network_drive", "sharepoint_server", "slack"}


@patch("connectors.fips.is_openssl_fips_mode", return_value=True)
def test_apply_fips_mode_on_drops_non_fips_connectors(patch_openssl, config_mock):
    service_manager = ConnectorServiceManager(config_mock)

    config = service_manager._apply_fips_mode(_fips_config(True))

    assert FIPSConfig.is_fips_mode_enabled() is True
    assert set(config["sources"]) == {"slack"}


@patch("connectors.fips.is_openssl_fips_mode", return_value=False)
def test_apply_fips_mode_on_raises_when_system_is_not_fips_ready(
    patch_openssl, config_mock
):
    service_manager = ConnectorServiceManager(config_mock)

    with pytest.raises(FIPSModeError):
        service_manager._apply_fips_mode(_fips_config(True))


def test_apply_fips_mode_defaults_to_off_when_not_configured(config_mock):
    service_manager = ConnectorServiceManager(config_mock)

    config = service_manager._apply_fips_mode({"service": {}, "sources": {}})

    assert FIPSConfig.is_fips_mode_enabled() is False
    assert config["sources"] == {}


@patch("connectors.fips.is_openssl_fips_mode", return_value=True)
def test_apply_fips_mode_does_not_mutate_the_given_config(patch_openssl, config_mock):
    service_manager = ConnectorServiceManager(config_mock)
    original = _fips_config(True)

    service_manager._apply_fips_mode(original)

    assert set(original["sources"]) == {"network_drive", "sharepoint_server", "slack"}


@pytest.mark.asyncio
@patch("connectors.fips.is_openssl_fips_mode", return_value=False)
@patch("connectors.agent.service_manager.get_services", return_value=StubMultiService())
async def test_run_aborts_and_records_fatal_error_when_system_is_not_fips_ready(
    patch_get_services, patch_openssl, config_mock
):
    config_mock.get.return_value = _fips_config(True)
    service_manager = ConnectorServiceManager(config_mock)

    with pytest.raises(FIPSModeError):
        await service_manager.run()

    assert isinstance(service_manager.fatal_error, FIPSModeError)
    assert not patch_get_services.called


@pytest.mark.asyncio
@patch("connectors.agent.service_manager.get_services", return_value=StubMultiService())
async def test_clean_shutdown_records_no_fatal_error(patch_get_services, config_mock):
    service_manager = ConnectorServiceManager(config_mock)

    async def stop_service_after_timeout():
        await asyncio.sleep(0.1)
        service_manager.stop()

    await asyncio.gather(service_manager.run(), stop_service_after_timeout())

    assert service_manager.fatal_error is None
