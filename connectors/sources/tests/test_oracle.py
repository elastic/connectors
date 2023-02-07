#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Oracle Database source class methods"""
from unittest.mock import Mock, patch

import pytest

from connectors.sources.oracle import OracleDataSource
from connectors.sources.tests.support import create_source
from connectors.tests.commons import AsyncIterator


@pytest.mark.asyncio
async def test_ping(patch_logger):
    """Test ping method of OracleDataSource class"""
    # Setup
    source = create_source(OracleDataSource)
    source.execute_query = Mock(return_value=AsyncIterator(["table1", "table2"]))

    # Execute
    await source.ping()


@pytest.mark.asyncio
async def test_ping_negative(patch_logger):
    """Test ping method of OracleDataSource class with negative case"""
    # Setup
    source = create_source(OracleDataSource)

    with patch.object(
        OracleDataSource, "execute_query", side_effect=Exception("Something went wrong")
    ):
        # Execute
        with pytest.raises(Exception):
            await source.ping()
