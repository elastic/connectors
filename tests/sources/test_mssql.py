#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the microsoft sql database source class methods"""
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from connectors.sources.mssql import MSSQLDataSource, MSSQLQueries
from tests.sources.support import create_source
from tests.sources.test_generic_database import ConnectionSync

MSSQL_CONNECTION_STRING = "mssql+pytds://admin:Password_123@127.0.0.1:9090/xe"


class MockEngine:
    """This Class create mock engine for mssql dialect"""

    def connect(self):
        """Make a connection

        Returns:
            connection: Instance of ConnectionSync
        """
        return ConnectionSync(MSSQLQueries())


@freeze_time("2023-01-24T04:07:19")
@patch("connectors.sources.mssql.create_engine")
@patch("connectors.sources.mssql.URL.create")
@pytest.mark.asyncio
async def test_create_engine(mock_create_url, mock_create_engine):
    # Setup
    source = create_source(MSSQLDataSource)
    mock_create_engine.return_value = "Mock engine"
    mock_create_url.return_value = MSSQL_CONNECTION_STRING

    # Execute
    source._create_engine()

    # Assert
    mock_create_engine.assert_called_with(MSSQL_CONNECTION_STRING, connect_args={})

    # Setup
    source.ssl_enabled = True
    source.ssl_ca = "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"

    # Execute
    source._create_engine()

    # Assert
    mock_create_engine.assert_called_with(
        MSSQL_CONNECTION_STRING,
        connect_args={
            "cafile": source.certfile,
            "validate_host": False,
        },
    )

    # Cleanup
    await source.close()


@pytest.mark.asyncio
async def test_ping():
    source = create_source(MSSQLDataSource)
    source.engine = MockEngine()

    await source.ping()

    await source.close()


@pytest.mark.asyncio
async def test_get_docs():
    # Setup
    source = create_source(MSSQLDataSource)
    source.engine = MockEngine()
    actual_response = []
    expected_response = [
        {
            "dbo_emp_table_ids": 1,
            "dbo_emp_table_names": "abcd",
            "_id": "xe_dbo_emp_table_1_",
            "_timestamp": "2023-02-21T08:37:15+00:00",
            "Database": "xe",
            "Table": "emp_table",
            "schema": "dbo",
        },
        {
            "dbo_emp_table_ids": 2,
            "dbo_emp_table_names": "xyz",
            "_id": "xe_dbo_emp_table_2_",
            "_timestamp": "2023-02-21T08:37:15+00:00",
            "Database": "xe",
            "Table": "emp_table",
            "schema": "dbo",
        },
    ]

    # Execute
    async for doc in source.get_docs():
        actual_response.append(doc[0])

    # Assert
    assert actual_response == expected_response

    await source.close()


@pytest.mark.asyncio
async def test_close():
    source = create_source(MSSQLDataSource)
    source.create_pem_file()
    await source.close()
