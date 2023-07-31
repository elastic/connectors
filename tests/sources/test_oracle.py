#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Oracle Database source class methods"""
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from connectors.sources.oracle import OracleDataSource, OracleQueries
from tests.sources.support import create_source
from tests.sources.test_generic_database import ConnectionSync

DSN = "oracle+oracledb://admin:Password_123@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=9090))(CONNECT_DATA=(SID=xe)))"


@pytest.mark.asyncio
@patch("connectors.sources.oracle.create_engine")
async def test_create_engine_in_thick_mode(mock_fun):
    """Test create_engine method of OracleDataSource class in thick mode"""
    # Setup
    async with create_source(OracleDataSource) as source:
        config_file_path = {"lib_dir": "/home/devuser/lib", "config_dir": ""}
        source.oracle_home = "/home/devuser"
        mock_fun.return_value = "Mock Response"

        # Execute
        source._create_engine()

        # Assert
        mock_fun.assert_called_with(DSN, thick_mode=config_file_path)


@pytest.mark.asyncio
async def test_ping():
    async with create_source(OracleDataSource) as source:
        with patch.object(Engine, "connect", return_value=ConnectionSync(OracleQueries())):
            await source.ping()


@pytest.mark.asyncio
@patch("connectors.sources.oracle.create_engine")
async def test_create_engine_in_thin_mode(mock_fun):
    """Test create_engine method of OracleDataSource class in thin mode"""
    # Setup
    async with create_source(OracleDataSource) as source:
        # Execute
        source._create_engine()

        # Assert
        mock_fun.assert_called_with(DSN)


@pytest.mark.asyncio
async def test_get_docs():
    # Setup
    async with create_source(OracleDataSource) as source:
        with patch.object(
            Engine, "connect", return_value=ConnectionSync(OracleQueries())
        ):
            source.engine = create_engine(DSN)
            actual_response = []
            expected_response = [
                {
                    "emp_table_ids": 1,
                    "emp_table_names": "abcd",
                    "_id": "xe_emp_table_1_",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "Database": "xe",
                    "Table": "emp_table",
                },
                {
                    "emp_table_ids": 2,
                    "emp_table_names": "xyz",
                    "_id": "xe_emp_table_2_",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "Database": "xe",
                    "Table": "emp_table",
                },
            ]

            # Execute
            async for doc in source.get_docs():
                actual_response.append(doc[0])

            # Assert
            assert actual_response == expected_response
