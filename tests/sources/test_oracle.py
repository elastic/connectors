#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Oracle Database source class methods"""
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from sqlalchemy.engine import Engine

from connectors.sources.oracle import OracleClient, OracleDataSource, OracleQueries
from tests.sources.support import create_source
from tests.sources.test_generic_database import ConnectionSync

DSN_SID = "oracle+oracledb://admin:Password_123@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=9090))(CONNECT_DATA=(SID=xe)))"
DSN_SERVICE_NAME = "oracle+oracledb://admin:Password_123@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=9090))(CONNECT_DATA=(service_name=xe)))"
SID = "sid"
SERVICE_NAME = "service_name"


@contextmanager
def oracle_client(**extras):
    arguments = {
        "host": "127.0.0.1",
        "port": 9090,
        "user": "admin",
        "password": "Password_123",
        "connection_source": SID,
        "sid": "xe",
        "service_name": "xe",
        "tables": "*",
        "protocol": "TCP",
        "oracle_home": "",
        "wallet_config": "",
        "logger_": None,
    } | extras

    client = OracleClient(**arguments)
    try:
        yield client
    finally:
        client.close()


@patch("connectors.sources.oracle.create_engine")
@pytest.mark.parametrize(
    "connection_source, DSN",
    [
        (SID, DSN_SID),
        (SERVICE_NAME, DSN_SERVICE_NAME),
    ],
)
def test_engine_in_thin_mode(mock_fun, connection_source, DSN):
    """Test engine method of OracleClient class in thin mode"""
    # Setup
    with oracle_client() as client:
        # Execute
        client.connection_source = connection_source
        _ = client.engine

        # Assert
        mock_fun.assert_called_with(DSN)


@patch("connectors.sources.oracle.create_engine")
@pytest.mark.parametrize(
    "connection_source, DSN",
    [
        (SID, DSN_SID),
        (SERVICE_NAME, DSN_SERVICE_NAME),
    ],
)
def test_engine_in_thick_mode(mock_fun, connection_source, DSN):
    """Test engine method of OracleClient class in thick mode"""
    oracle_home = "/home/devuser"
    config_file_path = {"lib_dir": f"{oracle_home}/lib", "config_dir": ""}

    # Setup
    with oracle_client(oracle_home="/home/devuser") as client:
        client.connection_source = connection_source
        mock_fun.return_value = "Mock Response"

        # Execute
        _ = client.engine

        # Assert
        mock_fun.assert_called_with(DSN, thick_mode=config_file_path)


@pytest.mark.asyncio
async def test_ping():
    async with create_source(OracleDataSource) as source:
        with patch.object(
            Engine, "connect", return_value=ConnectionSync(OracleQueries())
        ):
            await source.ping()


@pytest.mark.asyncio
async def test_get_docs():
    # Setup
    async with create_source(
        OracleDataSource,
        username="admin",
        password="changeme",
        data_source=SID,
        sid="xe",
        tables="*",
    ) as source:
        with patch.object(
            Engine, "connect", return_value=ConnectionSync(OracleQueries())
        ):
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
