#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import json
from contextlib import asynccontextmanager
from unittest import mock
from unittest.mock import ANY, AsyncMock, Mock

import pytest
import redis
from freezegun import freeze_time

from connectors.filtering.validation import SyncRuleValidationResult
from connectors.protocol import Filter
from connectors.source import ConfigurableFieldValueError
from connectors.sources.redis import (
    RedisAdvancedRulesValidator,
    RedisDataSource,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source

ADVANCED_SNIPPET = "advanced_snippet"

DOCUMENT = [
    {
        "_id": "aa00c4c0f44c5cb7cad68df40e8f8877",
        "key": "0",
        "value": "this is value",
        "size_in_bytes": 10,
        "database": 0,
        "key_type": "string",
        "_timestamp": "2023-01-24T04:07:19+00:00",
    }
]


class RedisClientMock:
    async def execute_command(self, SELECT="JSON.GET", key="json_key"):
        return json.dumps({"1": "1", "2": "2"})

    async def zrange(self, key, start, skip, withscores=True):
        return {1, 2, 3}

    async def smembers(self, key):
        return {1, 2, 3}

    async def get(self, key):
        return "this is value"

    async def hgetall(self, key):
        return "hash"

    async def xread(self, key):
        return "stream"

    async def lrange(self, key, start, skip):
        return [1, 2, 3]

    async def config_get(self, databases):
        return {"databases": "1"}

    async def ping(self):
        return False

    async def aclose(self):
        return True

    async def type(self, key):  # NOQA
        return "string"

    async def memory_usage(self, key):
        return 10

    async def scan_iter(self, match, count, _type):
        yield "0"

    async def validate_database(self, db=0):
        await self.execute_command()
        await self.ping()


@asynccontextmanager
async def create_redis_source():
    async with create_source(
        RedisDataSource,
        host="localhost",
        port=6379,
        database="0",
        username="username",
        password="password",
    ) as source:
        yield source


@pytest.mark.asyncio
async def test_ping_positive():
    async with create_redis_source() as source:
        source.client.ping = AsyncMock()
        await source.ping()


@pytest.mark.asyncio
async def test_ping_negative():
    async with create_redis_source() as source:
        mocked_client = Mock()
        with mock.patch("redis.from_url", return_value=mocked_client):
            mocked_client.ping = AsyncMock(
                side_effect=redis.exceptions.AuthenticationError
            )
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
async def test_validate_config_when_database_type():
    async with create_redis_source() as source:
        source.client.database = ["1", "db123", "123"]
        mocked_client = Mock()
        mocked_client.validate_database = AsyncMock(return_value=True)
        with mock.patch("redis.from_url", return_value=mocked_client):
            with pytest.raises(ConfigurableFieldValueError):
                await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_when_database_is_invalid():
    async with create_redis_source() as source:
        source.client.database = ["123"]
        mocked_client = Mock()
        mocked_client.validate_database = AsyncMock(return_value=True)
        with mock.patch("redis.from_url", return_value=mocked_client):
            with pytest.raises(ConfigurableFieldValueError):
                await source.validate_config()


@pytest.mark.asyncio
@freeze_time("2023-01-24T04:07:19+00:00")
async def test_get_docs():
    async with create_redis_source() as source:
        source.client.database = [1]

        with mock.patch(
            "redis.from_url",
            return_value=AsyncMock(),
        ):
            source.get_db_records = AsyncIterator(items=DOCUMENT)
            async for (doc, _) in source.get_docs():
                assert doc in DOCUMENT


@pytest.mark.asyncio
async def test_get_databases_for_multiple_db():
    async with create_redis_source() as source:
        source.client.database = [1, 2]
        async for database in source.client.get_databases():
            assert database in [1, 2]


@pytest.mark.asyncio
async def test_get_databases_with_asterisk():
    async with create_redis_source() as source:
        source.client.database = ["*"]
        source.client._client = RedisClientMock()
        async for database in source.client.get_databases():
            assert database == 0


@pytest.mark.asyncio
async def test_get_databases_expect_no_databases_on_auth_error():
    async with create_redis_source() as source:
        source.client.database = ["*"]
        mocked_client = Mock()
        with mock.patch("redis.from_url", return_value=mocked_client):
            mocked_client.ping = AsyncMock(
                side_effect=redis.exceptions.AuthenticationError
            )
            async for database in source.client.get_databases():
                assert database == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key, key_type, expected_response",
    [
        ("json_key", "ReJSON-RL", {"1": "1", "2": "2"}),
        ("string_key", "string", "this is value"),
        ("list_key", "list", [1, 2, 3]),
        ("set_key", "set", {1, 2, 3}),
        ("sorted_set_key", "zset", {1, 2, 3}),
        ("hash_key", "hash", "hash"),
        ("stream_key", "stream", "stream"),
    ],
)
async def test_get_key_value(key, key_type, expected_response):
    async with create_redis_source() as source:
        source.client.database = ["*"]
        source.client._client = RedisClientMock()
        value = await source.client.get_key_value(key=key, key_type=key_type)
        assert value == expected_response


@pytest.mark.asyncio
@freeze_time("2023-01-24T04:07:19+00:00")
async def test_get_key_metadata():
    async with create_redis_source() as source:
        source.client._client = RedisClientMock()
        key_type, value, size = await source.client.get_key_metadata(key="0")
        assert key_type == "string"
        assert value == "this is value"
        assert size == 10


@pytest.mark.asyncio
@freeze_time("2023-01-24T04:07:19+00:00")
async def test_get_db_records():
    async with create_redis_source() as source:
        source.client._client = RedisClientMock()
        source.client.get_paginated_key = AsyncIterator(["0"])
        async for record in source.get_db_records(db=0):
            assert record == DOCUMENT[0]


@pytest.mark.parametrize(
    "filtering",
    [
        Filter(
            {
                ADVANCED_SNIPPET: {
                    "value": [
                        {"database": 0, "key_pattern": "0*", "type": "string"},
                    ]
                }
            }
        ),
    ],
)
@pytest.mark.asyncio
@freeze_time("2023-01-24T04:07:19+00:00")
async def test_get_docs_with_sync_rules(filtering):
    async with create_redis_source() as source:
        source.client.database = ["*"]
        source.client._client = Mock()
        source.client._client.scan_iter = AsyncIterator(["0"])
        source.client._client.execute_command = AsyncMock(return_value=True)
        source.client._client.type = AsyncMock(return_value="string")
        source.client._client.get = AsyncMock(return_value="this is value")
        source.client._client.memory_usage = AsyncMock(return_value=10)
        async for (doc, _) in source.get_docs(filtering):
            assert doc in DOCUMENT
        source.client._client.scan_iter.assert_called_once_with(
            match="0*", count=1000, _type="string"
        )


@pytest.mark.parametrize(
    "advanced_rules, expected_validation_result",
    [
        (
            # valid: empty array should be valid
            [],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: empty object should also be valid -> default value in Kibana
            {},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: one custom pattern
            [{"database": 0, "key_pattern": "*"}],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: two custom patterns
            [
                {"database": 0, "key_pattern": "test*"},
                {"database": 1, "type": "string"},
            ],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # invalid: database number
            [{"database": -1}],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: array of arrays -> wrong type
            {"database": ["a/b/c", ""]},
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid database name
            {"database": 0, "key_pattern": "abc*"},
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: key_pattern or type is missing
            {"database": 0},
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_advanced_rules_validation(advanced_rules, expected_validation_result):
    async with create_redis_source() as source:
        source.client._client = RedisClientMock()
        validation_result = await RedisAdvancedRulesValidator(source).validate(
            advanced_rules
        )
        assert validation_result == expected_validation_result


@pytest.mark.asyncio
async def test_client_when_mutual_ssl_enabled():
    async with create_redis_source() as source:
        source.client.database = ["*"]
        source.client.ssl_enabled = True
        source.client.mutual_tls_enabled = True
        source.client.tls_certfile = (
            "-----BEGIN CERTIFICATE----- Invalid-Certificate -----END CERTIFICATE-----"
        )
        source.client.tls_keyfile = "-----BEGIN RSA PRIVATE KEY----- Invalid-Certificate -----END RSA PRIVATE KEY-----"
        mocked_client = AsyncMock()
        with mock.patch(
            "redis.from_url",
            return_value=mocked_client,
        ):
            redis_client = source.client._client
            assert hasattr(redis_client, "ping")


@pytest.mark.asyncio
async def test_client_when_ssl_enabled():
    async with create_redis_source() as source:
        source.client.database = ["*"]
        source.client.ssl_enabled = True
        mocked_client = Mock()
        with mock.patch(
            "redis.from_url",
            return_value=mocked_client,
        ):
            redis_client = source.client._client
            assert hasattr(redis_client, "ping")
