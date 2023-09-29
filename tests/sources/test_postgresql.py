#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the PostgreSQL database source class methods"""
import ssl
from contextlib import asynccontextmanager
from unittest.mock import ANY, Mock, patch

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.engine import AsyncEngine

from connectors.filtering.validation import SyncRuleValidationResult
from connectors.protocol import Filter
from connectors.sources.postgresql import (
    PostgreSQLAdvancedRulesValidator,
    PostgreSQLClient,
    PostgreSQLDataSource,
    PostgreSQLQueries,
)
from tests.sources.support import create_source

ADVANCED_SNIPPET = "advanced_snippet"
POSTGRESQL_CONNECTION_STRING = (
    "postgresql+asyncpg://admin:changme@127.0.0.1:5432/testdb"
)
SCHEMA = "public"
TABLE = "emp_table"
CUSTOMER_TABLE = "customer"


@asynccontextmanager
async def create_postgresql_source():
    async with create_source(
        PostgreSQLDataSource,
        host="127.0.0.1",
        port="9090",
        database="xe",
        tables="*",
        schema=SCHEMA,
    ) as source:
        yield source


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

            self.query = str(self.query)
            query_object = PostgreSQLQueries()
            if self.query == query_object.all_tables(database="xe", schema=SCHEMA):
                return [(TABLE,)]
            elif self.query == query_object.table_data_count(
                schema=SCHEMA, table=TABLE
            ):
                return [(10,)]
            elif self.query == query_object.table_primary_key(
                schema=SCHEMA, table=TABLE
            ):
                return [("ids",)]
            elif self.query == query_object.table_primary_key(
                schema=SCHEMA, table=CUSTOMER_TABLE
            ):
                return [("ids",)]
            elif self.query == query_object.table_last_update_time(
                schema=SCHEMA, table=TABLE
            ):
                return [("2023-02-21T08:37:15+00:00",)]
            elif self.query == query_object.table_last_update_time(
                schema=SCHEMA, table=CUSTOMER_TABLE
            ):
                return [("2023-02-21T08:37:15+00:00",)]
            elif self.query.lower() == "select * from customer":
                return [(1, "customer_1"), (2, "customer_2")]
            elif self.query == query_object.ping():
                return [(2,)]
            else:
                return [
                    (
                        1,
                        "abcd",
                    ),
                    (
                        2,
                        "xyz",
                    ),
                ]
        return []

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass


def test_get_connect_args():
    """This function test _get_connect_args with dummy certificate"""
    # Setup
    client = PostgreSQLClient(
        host="",
        port="",
        user="",
        password="",
        database="",
        schema="",
        tables="*",
        ssl_enabled=True,
        ssl_ca="-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----",
        logger_=None,
    )

    # Execute
    with patch.object(ssl, "create_default_context", return_value=MockSsl()):
        client._get_connect_args()


@pytest.mark.asyncio
async def test_postgresql_ping():
    # Setup
    async with create_postgresql_source() as source:
        with patch.object(AsyncEngine, "connect", return_value=ConnectionAsync()):
            await source.ping()

        await source.close()


@pytest.mark.asyncio
async def test_ping():
    async with create_postgresql_source() as source:
        with patch.object(AsyncEngine, "connect", return_value=ConnectionAsync()):
            await source.ping()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_ping_negative():
    with pytest.raises(Exception):
        async with create_source(PostgreSQLDataSource, port=5432) as source:
            with patch.object(AsyncEngine, "connect", side_effect=Exception()):
                await source.ping()


@pytest.mark.parametrize(
    "advanced_rules, expected_validation_result",
    [
        (
            # valid: empty array should be valid
            [],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: empty object should also be valid -> default value in Kibana
            {},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: valid queries
            [
                {
                    "tables": ["emp_table"],
                    "query": "select * from emp_table",
                }
            ],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # invalid: tables not present in database
            [
                {
                    "tables": ["table_name"],
                    "query": "select * from table_name",
                }
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: tables key missing
            [{"query": "select * from table_name"}],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: invalid key
            [
                {
                    "tables": "table_name",
                    "query": "select * from table_name",
                }
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: tables can be empty
            [
                {
                    "tables": [],
                    "query": "select * from table_name",
                }
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_advanced_rules_validation(advanced_rules, expected_validation_result):
    async with create_source(
        PostgreSQLDataSource, database="xe", tables="*", schema="public", port=5432
    ) as source:
        with patch.object(AsyncEngine, "connect", return_value=ConnectionAsync()):
            validation_result = await PostgreSQLAdvancedRulesValidator(source).validate(
                advanced_rules
            )

            assert validation_result == expected_validation_result


@pytest.mark.asyncio
async def test_get_docs():
    # Setup
    async with create_postgresql_source() as source:
        with patch.object(AsyncEngine, "connect", return_value=ConnectionAsync()):
            source.engine = create_async_engine(POSTGRESQL_CONNECTION_STRING)
            actual_response = []
            expected_response = [
                {
                    "public_emp_table_ids": 1,
                    "public_emp_table_names": "abcd",
                    "_id": "xe_public_emp_table_1",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": "emp_table",
                    "schema": "public",
                },
                {
                    "public_emp_table_ids": 2,
                    "public_emp_table_names": "xyz",
                    "_id": "xe_public_emp_table_2",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": "emp_table",
                    "schema": "public",
                },
            ]

            # Execute
            async for doc in source.get_docs():
                actual_response.append(doc[0])

            # Assert
            assert actual_response == expected_response


@pytest.mark.parametrize(
    "filtering, expected_response",
    [
        # Configured valid query
        (
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": [
                            {
                                "tables": ["emp_table"],
                                "query": "select * from emp_table",
                            },
                        ]
                    }
                }
            ),
            [
                {
                    "public_emp_table_ids": 1,
                    "public_emp_table_names": "abcd",
                    "_id": "xe_public_emp_table_1",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": ["emp_table"],
                    "schema": "public",
                },
                {
                    "public_emp_table_ids": 2,
                    "public_emp_table_names": "xyz",
                    "_id": "xe_public_emp_table_2",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": ["emp_table"],
                    "schema": "public",
                },
            ],
        ),
        (
            # Configured multiple rules
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": [
                            {
                                "tables": ["emp_table"],
                                "query": "select * from emp_table",
                            },
                            {"tables": ["customer"], "query": "select * from customer"},
                        ]
                    }
                }
            ),
            [
                {
                    "public_emp_table_ids": 1,
                    "public_emp_table_names": "abcd",
                    "_id": "xe_public_emp_table_1",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": ["emp_table"],
                    "schema": "public",
                },
                {
                    "public_emp_table_ids": 2,
                    "public_emp_table_names": "xyz",
                    "_id": "xe_public_emp_table_2",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": ["emp_table"],
                    "schema": "public",
                },
                {
                    "public_customer_ids": 1,
                    "public_customer_names": "customer_1",
                    "_id": "xe_public_customer_1",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": ["customer"],
                    "schema": "public",
                },
                {
                    "public_customer_ids": 2,
                    "public_customer_names": "customer_2",
                    "_id": "xe_public_customer_2",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": ["customer"],
                    "schema": "public",
                },
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules(filtering, expected_response):
    async with create_source(
        PostgreSQLDataSource,
        database="xe",
        tables="*",
        schema="public",
        port=5432,
    ) as source:
        with patch.object(AsyncEngine, "connect", return_value=ConnectionAsync()):
            actual_response = []

            async for doc in source.get_docs(filtering=filtering):
                actual_response.append(doc[0])

            assert actual_response == expected_response
