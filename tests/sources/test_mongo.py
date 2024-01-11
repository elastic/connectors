#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from unittest import mock
from unittest.mock import AsyncMock, Mock

import pytest
from bson import DBRef, ObjectId
from bson.decimal128 import Decimal128
from pymongo.errors import OperationFailure

from connectors.protocol import Filter
from connectors.source import ConfigurableFieldValueError
from connectors.sources.mongo import MongoAdvancedRulesValidator, MongoDataSource
from tests.commons import AsyncIterator
from tests.sources.support import create_source


@asynccontextmanager
async def create_mongo_source(
    host="mongodb://127.0.0.1:27021",
    database="db",
    collection="col",
    ssl_enabled=False,
    ssl_ca="",
    tls_insecure=False,
):
    async with create_source(
        MongoDataSource,
        host=host,
        user="foo",
        password="bar",
        direct_connection=True,
        database=database,
        collection=collection,
        ssl_enabled=ssl_enabled,
        ssl_ca=ssl_ca,
        tls_insecure=tls_insecure,
    ) as source:
        yield source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "advanced_rules, is_valid",
    [
        (
            # empty advanced rules
            {},
            True,
        ),
        (
            # valid aggregate
            {
                "aggregate": {
                    "allowDiskUse": True,
                    "maxTimeMS": 1,
                    "batchSize": 1,
                    "let": {"key": "value"},
                    "pipeline": [{"$project": {"name": {"$toUpper": "$name"}}}],
                }
            },
            True,
        ),
        (
            # unknown field in aggregate
            {"aggregate": {"unknown": True}},
            False,
        ),
        (
            # empty pipeline
            {"aggregate": {"pipeline": []}},
            False,
        ),
        (
            # wrong type
            {"aggregate": {"batchSize": 1.5}},
            False,
        ),
        (
            # valid find
            {
                "find": {
                    "filter": {"key": "value"},
                    "projection": ["field"],
                    "skip": 1,
                    "limit": 10,
                    "no_cursor_timeout": True,
                    "allow_partial_results": True,
                    "batch_size": 5,
                    "return_key": True,
                    "show_record_id": False,
                    "max_time_ms": 10,
                    "allow_disk_use": True,
                }
            },
            True,
        ),
        (
            # projection as object
            {"find": {"projection": {"key": "value"}}},
            True,
        ),
        (
            # empty projection
            {"find": {"projection": []}},
            False,
        ),
        (
            # wrong type
            {"find": {"skip": 1.5}},
            False,
        ),
        (
            # unknown field in find
            {"find": {"unknown": 42}},
            False,
        ),
        (
            # unknown top level field
            {"filter": {}},
            False,
        ),
        (
            # aggregate and find present
            {"aggregate": {}, "find": {}},
            False,
        ),
    ],
)
async def test_advanced_rules_validator(advanced_rules, is_valid):
    validation_result = await MongoAdvancedRulesValidator().validate(advanced_rules)
    assert validation_result.is_valid == is_valid


def build_resp():
    doc1 = {"id": "one", "tuple": (1, 2, 3), "date": datetime.now()}
    doc2 = {"id": "two", "dict": {"a": "b"}, "decimal": Decimal128("0.0005")}

    class Docs(dict):
        def __init__(self):
            self["cursor"] = self
            self["id"] = "1234"
            self["firstBatch"] = [doc1, doc2]
            self["nextBatch"] = []

    resp = mock.MagicMock()
    resp.docs = [Docs()]
    return resp


@pytest.mark.asyncio
@mock.patch(
    "pymongo.topology.Topology._select_servers_loop", lambda *x: [mock.MagicMock()]
)
@mock.patch(
    "pymongo.mongo_client.MongoClient._run_operation", lambda *xi, **kw: build_resp()
)
async def test_get_docs(*args):
    async with create_mongo_source() as source:
        num = 0
        async for (doc, _) in source.get_docs():
            assert doc["id"] in ("one", "two")
            num += 1

        assert num == 2


@pytest.mark.asyncio
@mock.patch(
    "pymongo.topology.Topology._select_servers_loop", lambda *x: [mock.MagicMock()]
)
@mock.patch(
    "pymongo.mongo_client.MongoClient._run_operation", lambda *xi, **kw: build_resp()
)
async def test_ping_when_called_then_does_not_raise(*args):
    admin_mock = Mock()
    command_mock = AsyncMock()
    admin_mock.command = command_mock

    class MockedClient:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return

        @property
        def admin(self):
            return admin_mock

    async with create_mongo_source() as source:
        with mock.patch.object(source, "get_client", return_value=MockedClient()):
            await source.ping()


@pytest.mark.asyncio
async def test_mongo_data_source_get_docs_when_advanced_rules_find_present():
    async with create_mongo_source() as source:
        collection_mock = Mock()
        collection_mock.find = AsyncIterator(items=[{"_id": 1}])
        source.collection = collection_mock

        filtering = Filter(
            {
                "advanced_snippet": {
                    "value": {
                        "find": {
                            "filter": {"key": "value"},
                            "projection": ["field"],
                            "skip": 1,
                            "limit": 10,
                            "no_cursor_timeout": True,
                            "allow_partial_results": True,
                            "batch_size": 5,
                            "return_key": True,
                            "show_record_id": False,
                            "max_time_ms": 10,
                            "allow_disk_use": True,
                        }
                    }
                }
            }
        )

        with mock.patch.object(source, "get_client") as mock_client:
            mock_client.__enter__.return_value = Mock()

            async for _ in source.get_docs(filtering):
                pass

            find_call_kwargs = collection_mock.find.call_kwargs
            assert find_call_kwargs[0] == filtering.get_advanced_rules().get("find")


@pytest.mark.asyncio
async def test_mongo_data_source_get_docs_when_advanced_rules_aggregate_present():
    async with create_mongo_source() as source:
        collection_mock = Mock()
        collection_mock.aggregate = AsyncIterator(items=[{"_id": 1}])
        source.collection = collection_mock

        filtering = Filter(
            {
                "advanced_snippet": {
                    "value": {
                        "aggregate": {
                            "allowDiskUse": True,
                            "maxTimeMS": 10,
                            "batchSize": 1,
                            "let": {"key": "value"},
                            "pipeline": [
                                {"$match": {"field1": "value1"}},
                                {"$group": {"_id": "$field2", "count": {"$sum": 1}}},
                            ],
                        }
                    }
                }
            }
        )

        with mock.patch.object(source, "get_client") as mock_client:
            mock_client.__enter__.return_value = Mock()
            async for _ in source.get_docs(filtering):
                pass

            aggregate_call_kwargs = collection_mock.aggregate.call_kwargs
            assert aggregate_call_kwargs[0] == filtering.get_advanced_rules().get(
                "aggregate"
            )


def future_with_result(result):
    future = asyncio.Future()
    future.set_result(result)

    return future


@pytest.mark.asyncio
@mock.patch(
    "motor.motor_asyncio.AsyncIOMotorDatabase.validate_collection",
    side_effect=OperationFailure("Unauthorized"),
)
async def test_validate_config_when_database_name_invalid_then_raises_exception(
    patch_validate_collection,
):
    server_database_names = ["hello", "world"]
    configured_database_name = "something"

    with mock.patch(
        "motor.motor_asyncio.AsyncIOMotorClient.list_database_names",
        return_value=future_with_result(server_database_names),
    ):
        async with create_mongo_source(
            database=configured_database_name,
            collection="something",
        ) as source:
            with pytest.raises(ConfigurableFieldValueError) as e:
                await source.validate_config()
            # assert that message contains database name from config
            assert e.match(configured_database_name)
            # assert that message contains database names from the server too
            for database_name in server_database_names:
                assert e.match(database_name)


@pytest.mark.asyncio
@mock.patch(
    "motor.motor_asyncio.AsyncIOMotorDatabase.validate_collection",
    side_effect=OperationFailure("Unauthorized"),
)
async def test_validate_config_when_collection_name_invalid_then_raises_exception(
    patch_validate_collection,
):
    server_database_names = ["hello"]
    server_collection_names = ["first", "second"]
    configured_database_name = "hello"
    configured_collection_name = "third"

    with mock.patch(
        "motor.motor_asyncio.AsyncIOMotorClient.list_database_names",
        return_value=future_with_result(server_database_names),
    ), mock.patch(
        "motor.motor_asyncio.AsyncIOMotorDatabase.list_collection_names",
        return_value=future_with_result(server_collection_names),
    ):
        async with create_mongo_source(
            database=configured_database_name,
            collection=configured_collection_name,
        ) as source:
            with pytest.raises(ConfigurableFieldValueError) as e:
                await source.validate_config()
            # assert that message contains database name from config
            assert e.match(configured_collection_name)
            # assert that message contains database names from the server too
            for collection_name in server_collection_names:
                assert e.match(collection_name)


@pytest.mark.asyncio
@mock.patch(
    "motor.motor_asyncio.AsyncIOMotorDatabase.validate_collection",
    side_effect=OperationFailure("Unauthorized"),
)
@mock.patch(
    "motor.motor_asyncio.AsyncIOMotorClient.list_database_names",
    return_value=future_with_result(["hello"]),
)
@mock.patch(
    "motor.motor_asyncio.AsyncIOMotorDatabase.list_collection_names",
    return_value=future_with_result(["first"]),
)
async def test_validate_config_when_collection_access_unauthorized(
    patch_validate_collection, patch_list_database_names, patch_list_collection_names
):
    configured_database_name = "hello"
    configured_collection_name = "second"

    with pytest.raises(ConfigurableFieldValueError) as e:
        async with create_mongo_source(
            database=configured_database_name,
            collection=configured_collection_name,
        ) as source:
            await source.validate_config()

    assert e is not None


@pytest.mark.asyncio
@mock.patch(
    "motor.motor_asyncio.AsyncIOMotorDatabase.validate_collection",
    side_effect=OperationFailure("Unauthorized"),
)
@mock.patch(
    "motor.motor_asyncio.AsyncIOMotorClient.list_database_names",
    side_effect=OperationFailure("Unauthorized"),
)
async def test_validate_config_when_collection_access_unauthorized_and_no_admin_access(
    patch_validate_collection, patch_list_database_names
):
    configured_database_name = "hello"
    configured_collection_name = "second"

    with pytest.raises(ConfigurableFieldValueError) as e:
        async with create_mongo_source(
            database=configured_database_name,
            collection=configured_collection_name,
        ) as source:
            await source.validate_config()

    assert e is not None


@pytest.mark.asyncio
@mock.patch(
    "motor.motor_asyncio.AsyncIOMotorDatabase.validate_collection",
    return_value=future_with_result(None),
)
async def test_validate_config_when_configuration_valid_then_does_not_raise(
    patch_validate_connection,
):
    configured_database_name = "hello"
    configured_collection_name = "second"

    async with create_mongo_source(
        database=configured_database_name,
        collection=configured_collection_name,
    ) as source:
        await source.validate_config()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw, output",
    [
        ({"ref": DBRef("foo", "bar")}, {"ref": {"$ref": "foo", "$id": "bar"}}),
        ({"dec": Decimal128("1.25")}, {"dec": 1.25}),
        (
            {"id": ObjectId("507f1f77bcf86cd799439011")},
            {"id": "507f1f77bcf86cd799439011"},
        ),
    ],
)
async def test_serialize(raw, output):
    async with create_mongo_source() as source:
        assert source.serialize(raw) == output


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "certificate_value, tls_insecure",
    [
        (
            # Connection Successful: Certificate: Valid, tls_insecure: False
            "-----BEGIN CERTIFICATE----- Valid-Certificate -----END CERTIFICATE-----",
            False,
        ),
        (
            # Insecure Connection Successful: Certificate: Invalid, tls_insecure: True
            "-----BEGIN CERTIFICATE----- Invalid-Certificate -----END CERTIFICATE-----",
            True,
        ),
    ],
)
@mock.patch("ssl.SSLContext.load_verify_locations")
@mock.patch(
    "motor.motor_asyncio.AsyncIOMotorDatabase.validate_collection",
    side_effect=OperationFailure("Unauthorized"),
)
async def test_ssl_successful_connection(
    patch_validate_collection, mock_ssl, certificate_value, tls_insecure
):
    mock_ssl.return_value = True
    async with create_mongo_source(
        ssl_enabled=True, ssl_ca=certificate_value, tls_insecure=tls_insecure
    ) as source:
        with mock.patch(
            "motor.motor_asyncio.AsyncIOMotorClient.list_database_names",
            return_value=future_with_result(["db"]),
        ), mock.patch(
            "motor.motor_asyncio.AsyncIOMotorDatabase.list_collection_names",
            return_value=future_with_result(["col"]),
        ):
            await source.validate_config()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "host_value, ssl_value",
    [
        ("mongodb://127.0.0.1:27021/?ssl=true", False),
        ("mongodb://127.0.0.1:27021/?ssl=false", True),
    ],
)
async def test_get_client_when_pass_conflicting_values(host_value, ssl_value):
    async with create_mongo_source(host=host_value, ssl_enabled=ssl_value) as source:
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()
