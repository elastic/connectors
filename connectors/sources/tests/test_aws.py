#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime
from unittest import mock

import pytest

from connectors.sources.aws import S3DataSource
from connectors.sources.tests.support import create_source, assert_basics


@pytest.mark.asyncio
async def test_basics():
    await assert_basics(S3DataSource, "bucket", "ent-search-ingest-dev")


class Summary:
    def __init__(self, key):
        self.key = key

    @property
    async def size(self):
        return 12

    @property
    async def last_modified(self):
        return datetime.now()


class AIOResourceCollection:
    def __init__(self, *args, **kw):
        pass

    async def __anext__(self):
        yield Summary("1.txt")
        yield Summary("2.md")

    def __aiter__(self):
        return self.__anext__()


class S3Object(dict):
    def __init__(self, *args, **kw):
        self.meta = mock.MagicMock()
        self["Body"] = self
        self.called = False

    async def read(self, *args):
        if self.called:
            return b""
        self.called = True
        return b"xxxxx"

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def _make_api_call(self, *args, **kw):
        return self


@pytest.mark.asyncio
async def test_get_docs(patch_logger, mock_aws):
    source = create_source(S3DataSource)
    with mock.patch(
        "aioboto3.resources.collection.AIOResourceCollection", AIOResourceCollection
    ), mock.patch("aiobotocore.client.AioBaseClient", S3Object):

        num = 0
        async for (doc, dl) in source.get_docs():
            assert doc["_id"] in (
                "dd7ec931179c4dcb6a8ffb8b8786d20b",
                "f5776ce002083f17f7a8f8f37568a092",
            )
            data = await dl(doit=True, timestamp="xx")
            assert data["text"] == "xxxxx"
            num += 1
