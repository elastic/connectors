#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime
from unittest import mock
from unittest.mock import Mock

import pytest
from bson.decimal128 import Decimal128

from connectors.byoc import Filter
from connectors.sources.mongo import MongoDataSource
from connectors.sources.tests.support import assert_basics, create_source
from connectors.tests.commons import AsyncIterator


@pytest.mark.asyncio
@mock.patch(
    "pymongo.topology.Topology._select_servers_loop", lambda *x: [mock.MagicMock()]
)
@mock.patch("pymongo.mongo_client.MongoClient._get_socket")
async def test_basics(*args):
    await assert_basics(MongoDataSource, "host", "mongodb://127.0.0.1:27021")


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
async def test_get_docs(patch_logger, *args):
    source = create_source(MongoDataSource)
    num = 0
    async for (doc, dl) in source.get_docs():
        assert doc["id"] in ("one", "two")
        num += 1

    assert num == 2


@pytest.mark.asyncio
async def test_mongo_data_source_get_docs_when_advanced_rules_find_present():
    source = create_source(MongoDataSource)

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
