from contextlib import asynccontextmanager
from unittest.mock import patch, AsyncMock, MagicMock
from aiomock import AIOMock
import pytest

from connectors.source import DataSourceConfiguration
from connectors.sources.redis import RedisDataSource, _get_key_patterns


@pytest.mark.asyncio
@pytest.mark.parametrize("config, field", [
    ({"host": "", "port": 6379, "password": "password", "db": 0}, "host"),
    ({"host": "localhost", "port": 6379, "password": "", "db": 0}, "password"),
])
async def test_validate_config_empty_fields(config, field):
    rds = RedisDataSource(configuration=DataSourceConfiguration(config))

    with pytest.raises(ValueError) as exc_info:
        await rds.validate_config()

    assert str(exc_info.value) == f"{field} field must not be empty."


@pytest.mark.asyncio
async def test_validate_config_port_type():
    rds = RedisDataSource(
        configuration=DataSourceConfiguration({"host": "host", "port": 6379, "password": "password", "db": 0}))
    rds.configuration.set_field(name="port", value="abc")

    with pytest.raises(ValueError) as exc_info:
        await rds.validate_config()

    assert str(exc_info.value) == "port must be a positive integer"


@pytest.mark.asyncio
async def test_validate_config_db_type():
    rds = RedisDataSource(
        configuration=DataSourceConfiguration({"host": "host", "port": 6379, "password": "password", "db": 0}))
    rds.configuration.set_field(name="db", value="abc")

    with pytest.raises(ValueError) as exc_info:
        await rds.validate_config()

    assert str(exc_info.value) == "db must be a non-negative integer"


@pytest.mark.asyncio
@patch("connectors.sources.redis._redis_client")
async def test_ping_positive(redis_mock):
    redis_mock.ping = AsyncMock(True)
    rds = RedisDataSource(configuration={"host": "localhost", "port": 6379, "password": "", "db": 0})

    result = await rds.ping()

    assert result is True


@pytest.mark.asyncio
async def test_ping_positive():
    @asynccontextmanager
    async def pingable_client():
        redis_mock = AIOMock()
        redis_mock.ping = AsyncMock(return_value=True)

        yield redis_mock

    rds = RedisDataSource(
        configuration=DataSourceConfiguration({"host": "localhost", "port": 6379, "password": "", "db": 0}))
    rds._redis_client = pingable_client

    result = await rds.ping()

    assert result is True


@pytest.mark.asyncio
async def test_ping_negative():
    @asynccontextmanager
    async def non_pingable_client():
        redis_mock = AIOMock()
        redis_mock.ping = AsyncMock(return_value=False)

        yield redis_mock

    rds = RedisDataSource(
        configuration=DataSourceConfiguration({"host": "localhost", "port": 6379, "password": "", "db": 0}))
    rds._redis_client = non_pingable_client

    result = await rds.ping()

    assert result is False


@pytest.mark.asyncio
async def test_get_docs():
    @asynccontextmanager
    async def redis_client_mock():
        redis_mock = AIOMock()

        redis_mock.keys = AsyncMock(return_value=["key1", "key2", "key3"])
        redis_mock.get = AsyncMock(return_value="value")

        yield redis_mock

    config = {
        "host": "localhost",
        "port": 6379,
        "db": 0,
    }

    instance = RedisDataSource(DataSourceConfiguration(config))
    instance._redis_client = redis_client_mock

    result = []

    async for doc, _ in instance.get_docs():
        result.append((doc, None))

    assert result == [
        ({"_id": "key1", "value": "value"}, None),
        ({"_id": "key2", "value": "value"}, None),
        ({"_id": "key3", "value": "value"}, None),
    ]


@pytest.mark.asyncio
async def test_get_key_patterns_with_advanced_rules():
    mock_filtering = MagicMock()
    mock_filtering.has_advanced_rules.return_value = True
    mock_filtering.get_advanced_rules.return_value = ["key1", "key2", "another_key"]

    key_patterns = await _get_key_patterns(mock_filtering)

    assert key_patterns == ["key1", "key2", "another_key"]
    mock_filtering.has_advanced_rules.assert_called_once()
    mock_filtering.get_advanced_rules.assert_called_once()


@pytest.mark.asyncio
async def test_get_key_patterns_without_advanced_rules():
    mock_filtering = MagicMock()
    mock_filtering.has_advanced_rules.return_value = False

    key_patterns = await _get_key_patterns(mock_filtering)

    assert key_patterns == ["*"]
    mock_filtering.has_advanced_rules.assert_called_once()
    mock_filtering.get_advanced_rules.assert_not_called()