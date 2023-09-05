#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the microsoft sql database source class methods"""
import os
from unittest.mock import ANY, AsyncMock, Mock, patch

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError

from connectors.filtering.validation import SyncRuleValidationResult
from connectors.protocol import Filter
from connectors.sources.mssql import (
    MSSQLAdvancedRulesValidator,
    MSSQLDataSource,
    MSSQLQueries,
)
from tests.sources.support import create_source
from tests.sources.test_generic_database import ConnectionSync

ADVANCED_SNIPPET = "advanced_snippet"


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
@patch("connectors.utils.apply_retry_strategy")
async def test_ping_negative(mock_apply_retry_strategy):
    mock_apply_retry_strategy.return_value = Mock()
    with pytest.raises(Exception):
        async with create_source(MSSQLDataSource) as source:
            with patch.object(Engine, "connect", side_effect=Exception()):
                await source.ping()


@pytest.mark.asyncio
@patch("connectors.utils.apply_retry_strategy")
async def test_fetch_documents_from_table_negative(mock_apply_retry_strategy):
    mock_apply_retry_strategy.return_value = Mock()
    async with create_source(MSSQLDataSource) as source:
        with patch.object(
            source.mssql_client,
            "get_table_row_count",
            side_effect=ProgrammingError(statement=None, params=None, orig=None),
        ):
            async for _ in source.fetch_documents_from_table("table"):
                pass


@pytest.mark.asyncio
@patch("connectors.utils.apply_retry_strategy")
async def test_fetch_documents_from_query_negative(mock_apply_retry_strategy):
    mock_apply_retry_strategy.return_value = Mock()
    async with create_source(MSSQLDataSource) as source:
        with patch.object(
            source,
            "get_primary_key",
            side_effect=ProgrammingError(statement=None, params=None, orig=None),
        ):
            async for _ in source.fetch_documents_from_query(
                ["table"], "select * from table"
            ):
                pass


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
                    "_id": "xe_dbo_emp_table_1",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": "emp_table",
                    "schema": "dbo",
                },
                {
                    "dbo_emp_table_ids": 2,
                    "dbo_emp_table_names": "xyz",
                    "_id": "xe_dbo_emp_table_2",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": "emp_table",
                    "schema": "dbo",
                },
            ]

            # Execute
            async for doc in source.get_docs():
                actual_response.append(doc[0])

            # Assert
            assert actual_response == expected_response


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
        MSSQLDataSource, database="xe", tables="*", schema="dbo"
    ) as source:
        with patch.object(
            Engine, "connect", return_value=ConnectionSync(MSSQLQueries())
        ):
            validation_result = await MSSQLAdvancedRulesValidator(source).validate(
                advanced_rules
            )

            assert validation_result == expected_validation_result


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
                    "dbo_emp_table_ids": 1,
                    "dbo_emp_table_names": "abcd",
                    "_id": "xe_dbo_emp_table_1",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": ["emp_table"],
                    "schema": "dbo",
                },
                {
                    "dbo_emp_table_ids": 2,
                    "dbo_emp_table_names": "xyz",
                    "_id": "xe_dbo_emp_table_2",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": ["emp_table"],
                    "schema": "dbo",
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
                    "dbo_emp_table_ids": 1,
                    "dbo_emp_table_names": "abcd",
                    "_id": "xe_dbo_emp_table_1",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": ["emp_table"],
                    "schema": "dbo",
                },
                {
                    "dbo_emp_table_ids": 2,
                    "dbo_emp_table_names": "xyz",
                    "_id": "xe_dbo_emp_table_2",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": ["emp_table"],
                    "schema": "dbo",
                },
                {
                    "dbo_customer_ids": 1,
                    "dbo_customer_names": "customer_1",
                    "_id": "xe_dbo_customer_1",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": ["customer"],
                    "schema": "dbo",
                },
                {
                    "dbo_customer_ids": 2,
                    "dbo_customer_names": "customer_2",
                    "_id": "xe_dbo_customer_2",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "database": "xe",
                    "table": ["customer"],
                    "schema": "dbo",
                },
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules(filtering, expected_response):
    async with create_source(
        MSSQLDataSource, database="xe", tables="*", schema="dbo"
    ) as source:
        with patch.object(
            Engine, "connect", return_value=ConnectionSync(MSSQLQueries())
        ):
            actual_response = []

            async for doc in source.get_docs(filtering=filtering):
                actual_response.append(doc[0])

            assert actual_response == expected_response


@pytest.mark.asyncio
async def test_create_pem_file():
    async with create_source(MSSQLDataSource) as source:
        source.mssql_client.create_pem_file()
        assert ".pem" in source.mssql_client.certfile
        try:
            os.remove(source.mssql_client.certfile)
        except Exception:
            pass


@pytest.mark.asyncio
async def test_get_tables_to_fetch():
    actual_response = []
    expected_response = ["table1", "table2"]
    async with create_source(MSSQLDataSource) as source:
        source.mssql_client.tables = expected_response
        async for table in source.mssql_client.get_tables_to_fetch():
            actual_response.append(table)
        assert expected_response == actual_response


@pytest.mark.asyncio
async def test_yield_docs_custom_query():
    async with create_source(MSSQLDataSource) as source:
        source.mssql_client.get_table_primary_key = AsyncMock(return_value=[])
        async for _ in source._yield_docs_custom_query(
            ["tables"], "select * form tables"
        ):
            pass
