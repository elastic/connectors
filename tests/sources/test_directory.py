#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import pytest

from connectors.sources.directory import DEFAULT_DIR, DirectoryDataSource
from tests.sources.support import assert_basics, create_source


@pytest.mark.asyncio
async def test_basics():
    await assert_basics(DirectoryDataSource, "directory", DEFAULT_DIR)


@pytest.mark.asyncio
async def test_get_docs(catch_stdout):
    async with create_source(DirectoryDataSource) as source:
        num = 0
        async for (doc, dl) in source.get_docs():
            num += 1
            if doc["path"].endswith("__init__.py"):
                continue
            data = await dl(doit=True, timestamp="xx")
            if data is not None:
                assert len(data["_attachment"]) > 0
            if num > 100:
                break

        assert num > 3
