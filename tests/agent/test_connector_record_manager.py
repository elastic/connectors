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
from connectors.es.index import DocumentNotFoundError
from connectors.protocol import ConnectorIndex


@pytest.fixture
def mock_connector_index():
    return AsyncMock(ConnectorIndex)


@pytest.fixture
def mock_agent_config():
    return {
        "elasticsearch": {"host": "http://localhost:9200", "api_key": "dummy_key"},
        "connectors": [{"connector_id": "1", "service_type": "service1"}],
    }


@pytest.fixture
def connector_record_manager(mock_connector_index):
    manager = ConnectorRecordManager()
    manager.connector_index = mock_connector_index
    return manager


@pytest.mark.asyncio
@patch("connectors.protocol.ConnectorIndex", new_callable=AsyncMock)
async def test_ensure_connector_records_exist_creates_connectors_if_not_exist(
    mock_connector_index, mock_agent_config
):
    manager = ConnectorRecordManager()
    manager.connector_index = mock_connector_index
    mock_connector_index.fetch_by_id.side_effect = DocumentNotFoundError
    mock_connector_index.connector_put = AsyncMock()

    await manager.ensure_connector_records_exist(mock_agent_config)
    assert mock_connector_index.connector_put.call_count == 1
    mock_connector_index.connector_put.assert_any_await(
        connector_id="1",
        service_type="service1",
        connector_name="[Agentless] service1 connector",
    )


@pytest.mark.asyncio
async def test_ensure_connector_records_exist_connector_already_exists(
    connector_record_manager, mock_agent_config
):
    connector_record_manager._connector_exists = AsyncMock(return_value=True)
    await connector_record_manager.ensure_connector_records_exist(mock_agent_config)
    assert connector_record_manager.connector_index.connector_put.call_count == 0


@pytest.mark.asyncio
async def test_ensure_connector_records_exist_agent_config_not_ready(
    connector_record_manager,
):
    invalid_config = {"connectors": []}
    await connector_record_manager.ensure_connector_records_exist(invalid_config)
    assert connector_record_manager.connector_index.connector_put.call_count == 0


@pytest.mark.asyncio
async def test_ensure_connector_records_exist_exception_on_create(
    connector_record_manager, mock_agent_config
):
    connector_record_manager._connector_exists = AsyncMock(return_value=False)
    connector_record_manager.connector_index.connector_put = AsyncMock(
        side_effect=Exception("Failed to create")
    )
    with pytest.raises(Exception, match="Failed to create"):
        await connector_record_manager.ensure_connector_records_exist(mock_agent_config)


@pytest.mark.asyncio
async def test_connector_exists_returns_true_when_found(connector_record_manager):
    connector_record_manager.connector_index.fetch_by_id = AsyncMock(
        return_value={"id": "1"}
    )
    exists = await connector_record_manager._connector_exists("1")
    assert exists is True


@pytest.mark.asyncio
async def test_connector_exists_returns_false_when_not_found(connector_record_manager):
    connector_record_manager.connector_index.fetch_by_id = AsyncMock(
        side_effect=DocumentNotFoundError
    )
    exists = await connector_record_manager._connector_exists("1")
    assert exists is False


@pytest.mark.asyncio
async def test_connector_exists_handles_exception(connector_record_manager):
    connector_record_manager.connector_index.fetch_by_id = AsyncMock(
        side_effect=Exception("Fetch error")
    )
    exists = await connector_record_manager._connector_exists("1")
    assert exists is False


def test_agent_config_ready_with_valid_config(
    connector_record_manager, mock_agent_config
):
    ready = connector_record_manager._agent_config_ready(mock_agent_config)
    assert ready is True


def test_agent_config_ready_with_invalid_config_missing_connectors(
    connector_record_manager,
):
    invalid_config = {
        "elasticsearch": {"host": "http://localhost:9200", "api_key": "dummy_key"}
    }
    ready = connector_record_manager._agent_config_ready(invalid_config)
    assert ready is False


def test_agent_config_ready_with_invalid_config_missing_elasticsearch(
    connector_record_manager,
):
    invalid_config = {"connectors": [{"connector_id": "1", "service_type": "service1"}]}
    ready = connector_record_manager._agent_config_ready(invalid_config)
    assert ready is False
