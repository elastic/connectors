#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the microsoft sql database source class methods"""
from unittest.mock import patch

import pytest
from sqlalchemy.engine import Engine

from connectors.sources.mssql import MSSQLDataSource, MSSQLQueries
from tests.sources.support import create_source
from tests.sources.test_generic_database import ConnectionSync


class MockEngine:
    """This Class create mock engine for mssql dialect"""

    def connect(self):
        """Make a connection

        Returns:
            connection: Instance of ConnectionSync
        """
        return ConnectionSync(MSSQLQueries())


@pytest.mark.asyncio
async def test_ping():
    async with create_source(MSSQLDataSource) as source:
        source.engine = MockEngine()
        with patch.object(
            Engine, "connect", return_value=ConnectionSync(MSSQLQueries())
        ):
            await source.ping()


@pytest.mark.asyncio
async def test_get_docs():
    # Setup
    async with create_source(
        MSSQLDataSource, database="xe", tables="*", schema="dbo"
    ) as source:
        with patch.object(
            Engine, "connect", return_value=ConnectionSync(MSSQLQueries())
        ):
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
