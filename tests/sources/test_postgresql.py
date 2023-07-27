#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the PostgreSQL database source class methods"""
import ssl
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.engine import AsyncEngine

from connectors.sources.postgresql import PostgreSQLDataSource, PostgreSQLQueries
from tests.sources.support import create_source

POSTGRESQL_CONNECTION_STRING = (
    "postgresql+asyncpg://admin:changme@127.0.0.1:5432/testdb"
)
SCHEMA = "public"
TABLE = "emp_table"


class MockSsl:
    """This class contains methods which returns dummy ssl context"""

    def load_verify_locations(self, cadata):
        """This method verify locations"""
        pass


class ConnectionAsync:
    """This class creates dummy connection with database and return dummy cursor"""

    async def __aenter__(self):
        """Make a dummy database connection and return it"""
        return self

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass

    async def execute(self, query):
        """This method returns dummy cursor"""
        return CursorAsync(query=query)


class CursorAsync:
    """This class contains methods which returns dummy response"""

    async def __aenter__(self):
        """Make a dummy database connection and return it"""
        return self

    def __init__(self, *args, **kw):
        """Setup dummy cursor"""
        self.query = kw["query"]
        self.first_call = True

    def keys(self):
        """Return Columns of table

        Returns:
            list: List of columns
        """
        return ["ids", "names"]

    def fetchmany(self, size):
        """This method returns response of fetchmany

        Args:
            size (int): Number of rows

        Returns:
            list: List of rows
        """
        if self.first_call:
            self.first_call = False
            return [
                (
                    1,
                    "abcd",
                ),
                (
                    1,
                    "xyz",
                ),
            ]
        return []

    def fetchall(self):
        """This method returns results of query

        Returns:
            list: List of rows
        """
        self.query = str(self.query)
        query_object = PostgreSQLQueries()
        if self.query == query_object.all_schemas():
            return [(SCHEMA,)]
        elif self.query == query_object.all_tables(database="xe", schema=SCHEMA):
            return [(TABLE,)]
        elif self.query == query_object.table_data_count(schema=SCHEMA, table=TABLE):
            return [(10,)]
        elif self.query == query_object.table_primary_key(schema=SCHEMA, table=TABLE):
            return [("ids",)]
        elif self.query == query_object.table_last_update_time(
            schema=SCHEMA, table=TABLE
        ):
            return [("2023-02-21T08:37:15+00:00",)]

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass


def test_get_connect_args():
    """This function test get_connect_args with dummy certificate"""
    # Setup
    source = create_source(PostgreSQLDataSource)
    source.ssl_ca = "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"

    # Execute
    with patch.object(ssl, "create_default_context", return_value=MockSsl()):
        source.get_connect_args()


@pytest.mark.asyncio
async def test_get_docs_postgresql():
    # Setup
    source = create_source(PostgreSQLDataSource)
    with patch.object(AsyncEngine, "connect", return_value=ConnectionAsync()):
        source.engine = create_async_engine(POSTGRESQL_CONNECTION_STRING)
        actual_response = []
        expected_response = [
            {
                "public_emp_table_ids": 1,
                "public_emp_table_names": "abcd",
                "_id": "xe_public_emp_table_1_",
                "_timestamp": "2023-02-21T08:37:15+00:00",
                "Database": "xe",
                "Table": "emp_table",
                "schema": "public",
            },
            {
                "public_emp_table_ids": 1,
                "public_emp_table_names": "xyz",
                "_id": "xe_public_emp_table_1_",
                "_timestamp": "2023-02-21T08:37:15+00:00",
                "Database": "xe",
                "Table": "emp_table",
                "schema": "public",
            },
        ]

        # Execute
        async for doc in source.get_docs():
            actual_response.append(doc[0])

        # Assert
        assert actual_response == expected_response
