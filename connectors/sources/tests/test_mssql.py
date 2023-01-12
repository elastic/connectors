#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the microsoft sql database source class methods"""
from unittest.mock import Mock

import pytest

from connectors.sources.mssql import MSSQLDataSource
from connectors.sources.tests.support import create_source


class AsyncIter:
    """This Class is use to return async generator"""

    def __init__(self, items):
        """Setup list of dictionary"""
        self.items = items

    async def __aiter__(self):
        """This Method is used to return async generator"""
        for item in self.items:
            yield item

    async def __anext__(self):
        """This Method is used to return one document"""
        return self.items[0]


@pytest.mark.asyncio
async def test_ping(patch_logger):
    """Test ping method of MSSQLDataSource class"""
    # Setup
    source = create_source(MSSQLDataSource)
    source.execute_query = Mock(return_value=AsyncIter(["table1", "table2"]))

    # Execute
    await source.ping()


@pytest.mark.asyncio
async def test_ping_negative(patch_logger):
    """Test ping method of MSSQLDataSource class with negative case"""
    # Setup
    source = create_source(MSSQLDataSource)

    # Execute
    with pytest.raises(Exception):
        await source.ping()
