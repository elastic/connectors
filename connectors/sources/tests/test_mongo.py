#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime
from unittest import mock

import pytest
from bson.decimal128 import Decimal128

from connectors.sources.mongo import MongoDataSource
from connectors.source import DataSourceConfiguration
from connectors.sources.tests.support import create_source


def test_get_configuration():
    klass = MongoDataSource
    # make sure the config can be read
    config = DataSourceConfiguration(klass.get_default_configuration())
    assert config["host"] == "mongodb://127.0.0.1:27021"


@pytest.mark.asyncio
async def test_ping():
    with (
        mock.patch(
            "pymongo.topology.Topology._select_servers_loop",
            lambda *x: [mock.MagicMock()],
        ),
        mock.patch("pymongo.mongo_client.MongoClient._get_socket"),
    ):
        source = create_source(MongoDataSource)
        await source.ping()


@pytest.mark.asyncio
async def test_get_docs(patch_logger):

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

    with (
        mock.patch(
            "pymongo.topology.Topology._select_servers_loop",
            lambda *x: [mock.MagicMock()],
        ),
        mock.patch("pymongo.mongo_client.MongoClient._get_socket"),
        mock.patch("connectors.sources.mongo.MongoDataSource.watch"),
        mock.patch(
            "pymongo.mongo_client.MongoClient._run_operation", lambda *xi, **kw: resp
        ),
    ):
        source = create_source(MongoDataSource)
        num = 0
        async for (doc, dl) in source.get_docs():
            assert doc["id"] in ("one", "two")
            num += 1

        assert num == 2
