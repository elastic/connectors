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

DSN = "oracle+oracledb://admin:Password_123@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=9090))(CONNECT_DATA=(SID=xe)))"


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


@pytest.mark.asyncio
@patch("connectors.sources.oracle.create_engine")
async def test_create_engine_in_thick_mode(mock_fun):
    """Test create_engine method of OracleDataSource class in thick mode"""
    # Setup
    source = create_source(OracleDataSource)
    config_file_path = {"lib_dir": "/home/devuser/lib", "config_dir": ""}
    source.oracle_home = "/home/devuser"
    mock_fun.return_value = "Mock Response"

    # Execute
    source._create_engine()

    # Assert
    mock_fun.assert_called_with(DSN, thick_mode=config_file_path)


@pytest.mark.asyncio
@patch("connectors.sources.oracle.create_engine")
async def test_create_engine_in_thin_mode(mock_fun):
    """Test create_engine method of OracleDataSource class in thin mode"""
    # Setup
    source = create_source(OracleDataSource)

    # Execute
    source._create_engine()

    # Assert
    mock_fun.assert_called_with(DSN)
