#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import AsyncMock, patch

import pytest

from connectors.agent.connector_record_manager import (
    ConnectorRecordManager,
)
from connectors.protocol import ConnectorIndex


@pytest.fixture
def mock_connector_index():
    return AsyncMock(ConnectorIndex)


@pytest.fixture
def mock_agent_config():
    return {
        "elasticsearch": {"host": "http://localhost:9200", "api_key": "dummy_key"},
        "connectors_service": [{"connector_id": "1", "service_type": "service1"}],
    }


@pytest.fixture
def connector_record_manager(mock_connector_index):
    manager = ConnectorRecordManager()
    manager.connector_index = mock_connector_index
    return manager


@pytest.mark.asyncio
async def test_ensure_connector_records_exist_creates_connectors_if_not_exist(
    connector_record_manager, mock_agent_config
):
    random_connector_name_id = "1234"

    with patch(
        "connectors_service.agent.connector_record_manager.generate_random_id",
        return_value=random_connector_name_id,
    ):
        connector_record_manager.connector_index.connector_exists = AsyncMock(
            return_value=False
        )
        connector_record_manager.connector_index.connector_put = AsyncMock()

        await connector_record_manager.ensure_connector_records_exist(mock_agent_config)
        assert connector_record_manager.connector_index.connector_put.call_count == 1
        connector_record_manager.connector_index.connector_put.assert_any_await(
            connector_id="1",
            service_type="service1",
            connector_name=f"[Elastic-managed] service1 connector {random_connector_name_id}",
            is_native=True,
        )


@pytest.mark.asyncio
async def test_ensure_connector_records_exist_connector_already_exists(
    connector_record_manager, mock_agent_config
):
    connector_record_manager.connector_index.connector_exists = AsyncMock(
        return_value=True
    )
    await connector_record_manager.ensure_connector_records_exist(mock_agent_config)
    assert connector_record_manager.connector_index.connector_put.call_count == 0


@pytest.mark.asyncio
async def test_ensure_connector_records_raises_on_non_404_error(
    connector_record_manager, mock_agent_config
):
    connector_record_manager.connector_index.connector_exists = AsyncMock(
        side_effect=Exception("Unexpected error")
    )
    connector_record_manager.connector_index.connector_put = AsyncMock()

    with pytest.raises(Exception, match="Unexpected error"):
        await connector_record_manager.ensure_connector_records_exist(mock_agent_config)

    assert connector_record_manager.connector_index.connector_put.call_count == 0


@pytest.mark.asyncio
async def test_ensure_connector_records_exist_agent_config_not_ready(
    connector_record_manager,
):
    invalid_config = {"connectors_service": []}
    await connector_record_manager.ensure_connector_records_exist(invalid_config)
    assert connector_record_manager.connector_index.connector_put.call_count == 0


@pytest.mark.asyncio
async def test_ensure_connector_records_exist_exception_on_create(
    connector_record_manager, mock_agent_config
):
    connector_record_manager.connector_index.connector_exists = AsyncMock(
        return_value=False
    )
    connector_record_manager.connector_index.connector_put = AsyncMock(
        side_effect=Exception("Failed to create")
    )
    with pytest.raises(Exception, match="Failed to create"):
        await connector_record_manager.ensure_connector_records_exist(mock_agent_config)


def test_agent_config_ready_with_valid_config(
    connector_record_manager, mock_agent_config
):
    ready, _ = connector_record_manager._check_agent_config_ready(mock_agent_config)
    assert ready is True


def test_agent_config_ready_with_invalid_config_missing_connectors(
    connector_record_manager,
):
    invalid_config = {
        "elasticsearch": {"host": "http://localhost:9200", "api_key": "dummy_key"}
    }
    ready, _ = connector_record_manager._check_agent_config_ready(invalid_config)
    assert ready is False


def test_agent_config_ready_with_invalid_config_missing_elasticsearch(
    connector_record_manager,
):
    invalid_config = {"connectors_service": [{"connector_id": "1", "service_type": "service1"}]}
    ready, _ = connector_record_manager._check_agent_config_ready(invalid_config)
    assert ready is False
