#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import AsyncMock, Mock

import pytest
from elastic_agent_client.client import Unit
from elastic_agent_client.generated import elastic_agent_client_pb2 as proto
from google.protobuf.struct_pb2 import Struct

from connectors.agent.config import ConnectorsAgentConfigurationWrapper
from connectors.agent.protocol import ConnectorActionHandler, ConnectorCheckinHandler


@pytest.fixture(autouse=True)
def input_mock():
    unit_mock = Mock()
    unit_mock.unit_type = proto.UnitType.INPUT

    def _string_config_field_mock(value):
        field = Mock()
        field.string_value = value
        return field

    unit_mock.config.source.fields = {
        "service_type": _string_config_field_mock("test-service"),
        "connector_name": _string_config_field_mock("test-connector"),
        "id": _string_config_field_mock("test-id"),
    }
    unit_mock.config.type = "connectors-py"
    return unit_mock


@pytest.fixture(autouse=True)
def connector_record_manager_mock():
    connector_record_manager_mock = Mock()
    connector_record_manager_mock.ensure_connector_records_exist = AsyncMock(
        return_value=True
    )
    return connector_record_manager_mock


class TestConnectorActionHandler:
    @pytest.mark.asyncio
    async def test_handle_action(self):
        action_handler = ConnectorActionHandler()

        with pytest.raises(NotImplementedError):
            await action_handler.handle_action(None)


class TestConnectorCheckingHandler:
    @pytest.mark.asyncio
    async def test_apply_from_client_when_no_units_received(
        self, connector_record_manager_mock, input_mock
    ):
        client_mock = Mock()
        config_wrapper_mock = Mock()
        service_manager_mock = Mock()

        client_mock.units = []

        checkin_handler = ConnectorCheckinHandler(
            client_mock,
            config_wrapper_mock,
            service_manager_mock,
        )
        checkin_handler.connector_record_manager = connector_record_manager_mock

        await checkin_handler.apply_from_client()

        assert not config_wrapper_mock.try_update.called
        assert not service_manager_mock.restart.called

    @pytest.mark.asyncio
    async def test_apply_from_client_when_units_with_no_output(
        self, connector_record_manager_mock, input_mock
    ):
        client_mock = Mock()
        config_wrapper_mock = Mock()
        service_manager_mock = Mock()
        unit_mock = Mock()
        unit_mock.unit_type = "Something else"

        client_mock.units = [unit_mock, input_mock]

        checkin_handler = ConnectorCheckinHandler(
            client_mock,
            config_wrapper_mock,
            service_manager_mock,
        )
        checkin_handler.connector_record_manager = connector_record_manager_mock

        await checkin_handler.apply_from_client()

        assert not config_wrapper_mock.try_update.called
        assert not service_manager_mock.restart.called

    @pytest.mark.asyncio
    async def test_apply_from_client_when_units_with_output_and_non_updating_config(
        self, connector_record_manager_mock, input_mock
    ):
        client_mock = Mock()
        config_wrapper_mock = Mock()

        config_wrapper_mock.try_update.return_value = False

        service_manager_mock = Mock()
        unit_mock = Mock()
        unit_mock.unit_type = proto.UnitType.OUTPUT
        unit_mock.config.source = {"elasticsearch": {"api_key": 123}}

        client_mock.units = [unit_mock, input_mock]

        checkin_handler = ConnectorCheckinHandler(
            client_mock,
            config_wrapper_mock,
            service_manager_mock,
        )
        checkin_handler.connector_record_manager = connector_record_manager_mock

        await checkin_handler.apply_from_client()

        assert config_wrapper_mock.try_update.called_once_with(unit_mock.config.source)
        assert not service_manager_mock.restart.called

    @pytest.mark.asyncio
    async def test_apply_from_client_when_units_with_output_and_updating_config(
        self, connector_record_manager_mock, input_mock
    ):
        client_mock = Mock()
        config_wrapper_mock = Mock()

        config_wrapper_mock.try_update.return_value = True

        service_manager_mock = Mock()
        unit_mock = Mock()
        unit_mock.unit_type = proto.UnitType.OUTPUT
        unit_mock.config.source = {"elasticsearch": {"api_key": 123}}
        unit_mock.config.type = "elasticsearch"

        client_mock.units = [unit_mock, input_mock]

        checkin_handler = ConnectorCheckinHandler(
            client_mock,
            config_wrapper_mock,
            service_manager_mock,
        )
        checkin_handler.connector_record_manager = connector_record_manager_mock

        await checkin_handler.apply_from_client()

        assert config_wrapper_mock.try_update.called_once_with(unit_mock)
        assert service_manager_mock.restart.called

    @pytest.mark.asyncio
    async def test_apply_from_client_when_units_with_multiple_outputs_and_updating_config(
        self, connector_record_manager_mock, input_mock
    ):
        client_mock = Mock()
        config_wrapper_mock = Mock()

        config_wrapper_mock.try_update.return_value = True

        service_manager_mock = Mock()

        unit_es = Mock()
        unit_es.unit_type = proto.UnitType.OUTPUT
        unit_es.config.type = "elasticsearch"
        unit_es.config.id = "config-es"

        unit_kafka = Mock()
        unit_kafka.unit_type = proto.UnitType.OUTPUT
        unit_kafka.config.type = "kafka"
        unit_kafka.config.id = "config-kafka"

        client_mock.units = [unit_kafka, unit_es, input_mock]

        checkin_handler = ConnectorCheckinHandler(
            client_mock,
            config_wrapper_mock,
            service_manager_mock,
        )
        checkin_handler.connector_record_manager = connector_record_manager_mock

        await checkin_handler.apply_from_client()

        # Only ES output from the policy should be used by connectors component
        assert config_wrapper_mock.try_update.called_once()
        _, called_kwargs = config_wrapper_mock.try_update.call_args
        called_output_unit = called_kwargs.get("output_unit")
        assert called_output_unit.config.id == "config-es"

        assert service_manager_mock.restart.called

    @pytest.mark.asyncio
    async def test_apply_from_client_when_units_with_multiple_mixed_outputs_and_updating_config(
        self, connector_record_manager_mock, input_mock
    ):
        client_mock = Mock()
        config_wrapper_mock = Mock()

        config_wrapper_mock.try_update.return_value = True

        service_manager_mock = Mock()

        unit_es_1 = Mock()
        unit_es_1.unit_type = proto.UnitType.OUTPUT
        unit_es_1.config.type = "elasticsearch"
        unit_es_1.config.id = "config-es-1"

        unit_es_2 = Mock()
        unit_es_2.unit_type = proto.UnitType.OUTPUT
        unit_es_2.config.type = "elasticsearch"
        unit_es_2.config.id = "config-es-2"

        unit_kafka = Mock()
        unit_kafka.unit_type = proto.UnitType.OUTPUT
        unit_kafka.config.type = "kafka"
        unit_kafka.config.id = "config-kafka"

        client_mock.units = [unit_kafka, unit_es_2, unit_es_1, input_mock]

        checkin_handler = ConnectorCheckinHandler(
            client_mock,
            config_wrapper_mock,
            service_manager_mock,
        )
        checkin_handler.connector_record_manager = connector_record_manager_mock

        await checkin_handler.apply_from_client()

        # First ES output from the policy should be used by connectors component
        assert config_wrapper_mock.try_update.called_once()
        _, called_kwargs = config_wrapper_mock.try_update.call_args
        called_output_unit = called_kwargs.get("output_unit")
        assert called_output_unit.config.id == "config-es-2"

        assert service_manager_mock.restart.called

    @pytest.mark.asyncio
    async def test_apply_from_client_when_units_with_output_and_updating_log_level(
        self, connector_record_manager_mock, input_mock
    ):
        client_mock = Mock()
        config_wrapper = ConnectorsAgentConfigurationWrapper()

        service_manager_mock = Mock()

        unit = Unit(
            unit_id="unit-1",
            unit_type=proto.UnitType.OUTPUT,
            state=proto.State.HEALTHY,
            config_idx=1,
            config=proto.UnitExpectedConfig(
                source=Struct(),
                id="config-1",
                type="elasticsearch",
                name="test-config-name",
                revision=None,
                meta=None,
                data_stream=None,
                streams=None,
            ),
            log_level=proto.UnitLogLevel.DEBUG,
        )

        client_mock.units = [unit, input_mock]

        checkin_handler = ConnectorCheckinHandler(
            client_mock,
            config_wrapper,
            service_manager_mock,
        )
        checkin_handler.connector_record_manager = connector_record_manager_mock

        await checkin_handler.apply_from_client()
        assert service_manager_mock.restart.called
        assert config_wrapper.get()["service"]["log_level"] == proto.UnitLogLevel.DEBUG
