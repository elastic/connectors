#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from datetime import datetime
from unittest import mock
from unittest.mock import Mock

import pytest
from bson.decimal128 import Decimal128

from connectors.protocol import Filter
from connectors.source import ConfigurableFieldValueError
from connectors.sources.mongo import MongoAdvancedRulesValidator, MongoDataSource
from tests.commons import AsyncIterator
from tests.sources.support import create_source


def create_connector():
    return create_source(
        MongoDataSource,
        host="mongodb://127.0.0.1:27021",
        database="db",
        collection="col",
        direct_connection=True,
        user="foo",
        password="password",
    )


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
@mock.patch("pymongo.mongo_client.MongoClient._get_socket")
@mock.patch(
    "pymongo.mongo_client.MongoClient._run_operation", lambda *xi, **kw: build_resp()
)
async def test_get_docs(*args):
    source = create_connector()
    num = 0
    async for (doc, _) in source.get_docs():
        assert doc["id"] in ("one", "two")
        num += 1

    assert num == 2


@pytest.mark.asyncio
@mock.patch(
    "pymongo.topology.Topology._select_servers_loop", lambda *x: [mock.MagicMock()]
)
@mock.patch("pymongo.mongo_client.MongoClient._get_socket")
@mock.patch(
    "pymongo.mongo_client.MongoClient._run_operation", lambda *xi, **kw: build_resp()
)
@pytest.mark.asyncio
async def test_ping_when_called_then_does_not_raise(*args):
    source = create_connector()

    await source.ping()


@pytest.mark.asyncio
async def test_mongo_data_source_get_docs_when_advanced_rules_find_present():
    source = create_connector()
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

    async for _ in source.get_docs(filtering):
        pass

    find_call_kwargs = collection_mock.find.call_kwargs
    assert find_call_kwargs[0] == filtering.get_advanced_rules().get("find")


@pytest.mark.asyncio
async def test_mongo_data_source_get_docs_when_advanced_rules_aggregate_present():
    source = create_connector()

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

    async for _ in source.get_docs(filtering):
        pass

    aggregate_call_kwargs = collection_mock.aggregate.call_kwargs
    assert aggregate_call_kwargs[0] == filtering.get_advanced_rules().get("aggregate")


def future_with_result(result):
    future = asyncio.Future()
    future.set_result(result)

    return future


@pytest.mark.asyncio
async def test_validate_config_when_database_name_invalid_then_raises_exception():
    server_database_names = ["hello", "world"]
    configured_database_name = "something"

    with mock.patch(
        "motor.motor_asyncio.AsyncIOMotorClient.list_database_names",
        return_value=future_with_result(server_database_names),
    ):
        source = create_source(
            MongoDataSource,
            host="mongodb://127.0.0.1:27021",
            user="foo",
            password="password",
            database=configured_database_name,
            collection="something",
        )
        with pytest.raises(ConfigurableFieldValueError) as e:
            await source.validate_config()
        # assert that message contains database name from config
        assert e.match(configured_database_name)
        # assert that message contains database names from the server too
        for database_name in server_database_names:
            assert e.match(database_name)


@pytest.mark.asyncio
async def test_validate_config_when_collection_name_invalid_then_raises_exception():
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
        source = create_source(
            MongoDataSource,
            host="mongodb://127.0.0.1:27021",
            user="foo",
            password="bar",
            database=configured_database_name,
            collection=configured_collection_name,
        )
        with pytest.raises(ConfigurableFieldValueError) as e:
            await source.validate_config()
        # assert that message contains database name from config
        assert e.match(configured_collection_name)
        # assert that message contains database names from the server too
        for collection_name in server_collection_names:
            assert e.match(collection_name)


@pytest.mark.asyncio
async def test_validate_config_when_configuration_valid_then_does_not_raise():
    server_database_names = ["hello"]
    server_collection_names = ["first", "second"]
    configured_database_name = "hello"
    configured_collection_name = "second"

    with mock.patch(
        "motor.motor_asyncio.AsyncIOMotorClient.list_database_names",
        return_value=future_with_result(server_database_names),
    ), mock.patch(
        "motor.motor_asyncio.AsyncIOMotorDatabase.list_collection_names",
        return_value=future_with_result(server_collection_names),
    ):
        source = create_source(
            MongoDataSource,
            host="mongodb://127.0.0.1:27021",
            user="foo",
            password="bar",
            database=configured_database_name,
            collection=configured_collection_name,
        )

        await source.validate_config()
