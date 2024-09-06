#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import Mock

import pytest
from elastic_agent_client.generated import elastic_agent_client_pb2 as proto

from connectors.agent.protocol import ConnectorActionHandler, ConnectorCheckinHandler


class TestConnectorActionHandler:
    @pytest.mark.asyncio
    async def test_handle_action(self):
        action_handler = ConnectorActionHandler()

        with pytest.raises(NotImplementedError):
            await action_handler.handle_action(None)


class TestConnectorCheckingHandler:
    @pytest.mark.asyncio
    async def test_apply_from_client_when_no_units_received(self):
        client_mock = Mock()
        config_wrapper_mock = Mock()
        service_manager_mock = Mock()

        client_mock.units = []

        checkin_handler = ConnectorCheckinHandler(
            client_mock, config_wrapper_mock, service_manager_mock
        )

        await checkin_handler.apply_from_client()

        assert not config_wrapper_mock.try_update.called
        assert not service_manager_mock.restart.called

    @pytest.mark.asyncio
    async def test_apply_from_client_when_units_with_no_output(self):
        client_mock = Mock()
        config_wrapper_mock = Mock()
        service_manager_mock = Mock()
        unit_mock = Mock()
        unit_mock.unit_type = "Something else"

        client_mock.units = [unit_mock]

        checkin_handler = ConnectorCheckinHandler(
            client_mock, config_wrapper_mock, service_manager_mock
        )

        await checkin_handler.apply_from_client()

        assert not config_wrapper_mock.try_update.called
        assert not service_manager_mock.restart.called

    @pytest.mark.asyncio
    async def test_apply_from_client_when_units_with_output_and_non_updating_config(
        self,
    ):
        client_mock = Mock()
        config_wrapper_mock = Mock()

        config_wrapper_mock.try_update.return_value = False

        service_manager_mock = Mock()
        unit_mock = Mock()
        unit_mock.unit_type = proto.UnitType.OUTPUT
        unit_mock.config.source = {"elasticsearch": {"api_key": 123}}

        client_mock.units = [unit_mock]

        checkin_handler = ConnectorCheckinHandler(
            client_mock, config_wrapper_mock, service_manager_mock
        )

        await checkin_handler.apply_from_client()

        assert config_wrapper_mock.try_update.called_once_with(unit_mock.config.source)
        assert not service_manager_mock.restart.called

    @pytest.mark.asyncio
    async def test_apply_from_client_when_units_with_output_and_updating_config(self):
        client_mock = Mock()
        config_wrapper_mock = Mock()

        config_wrapper_mock.try_update.return_value = True

        service_manager_mock = Mock()
        unit_mock = Mock()
        unit_mock.unit_type = proto.UnitType.OUTPUT
        unit_mock.config.source = {"elasticsearch": {"api_key": 123}}

        client_mock.units = [unit_mock]

        checkin_handler = ConnectorCheckinHandler(
            client_mock, config_wrapper_mock, service_manager_mock
        )

        await checkin_handler.apply_from_client()

        assert config_wrapper_mock.try_update.called_once_with(unit_mock.config.source)
        assert service_manager_mock.restart.called
