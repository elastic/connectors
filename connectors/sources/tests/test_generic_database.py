#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Generic Database source class methods"""
from unittest.mock import patch

import pytest
from asyncpg.exceptions._base import InternalClientError
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.engine import AsyncEngine

from connectors.source import DataSourceConfiguration
from connectors.sources.generic_database import (
    GenericBaseDataSource,
    configured_tables,
    is_wildcard,
)
from connectors.sources.oracle import OracleDataSource
from connectors.sources.postgresql import PostgreSQLDataSource
from connectors.sources.tests.support import create_source
from connectors.tests.commons import AsyncIterator

POSTGRESQL_CONNECTION_STRING = (
    "postgresql+asyncpg://admin:changme@127.0.0.1:5432/testdb"
)
ORACLE_CONNECTION_STRING = "oracle+oracledb://admin:changme@127.0.0.1:1521/testdb"


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
        return CursorAsync()


class CursorAsync:
    """This class contains methods which returns dummy response"""

    async def __aenter__(self):
        """Make a dummy database connection and return it"""
        return self

    def __init__(self, *args, **kw):
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
                    2,
                    "xyz",
                ),
            ]
        return []

    def fetchall(self):
        """This method returns object of Return class


        Returns:
            list: List of rows
        """
        return [(10,)]

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass


class ConnectionSync:
    """This Class create dummy connection with database and return dummy cursor"""

    def __enter__(self):
        """Make a dummy database connection and return it"""
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass

    def execute(self, statement):
        """This method returns dummy cursor"""
        return CursorSync()


class CursorSync:
    """This class contains methods which returns dummy response"""

    def __enter__(self):
        """Make a dummy database connection and return it"""
        return self

    def __init__(self, *args, **kw):
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
                    2,
                    "xyz",
                ),
            ]
        return []

    def fetchall(self):
        """This method returns object of Return class"""
        return [(10,)]


def test_get_configuration(patch_logger):
    """Test get_configuration method of GenericBaseDataSource class"""

    # Setup
    klass = GenericBaseDataSource

    # Execute
    config = DataSourceConfiguration(klass.get_default_configuration())

    # Assert
    assert config["host"] == "127.0.0.1"


def test_validate_configuration_missing_fields(patch_logger):
    """Test _validate_configuration method check missing fields"""
    # Setup
    source = create_source(GenericBaseDataSource)
    with pytest.raises(Exception):
        source.configuration.set_field(name="host", value="")

        # Execute
        source._validate_configuration()


def test_validate_configuration_port(patch_logger):
    """Test _validate_configuration method check port"""
    # Setup
    source = create_source(GenericBaseDataSource)
    with pytest.raises(Exception):
        source.configuration.set_field(name="port", value="abcd")

        # Execute
        source._validate_configuration()


def test_validate_configuration_ssl(patch_logger):
    """Test _validate_configuration method check port"""
    # Setup
    source = create_source(PostgreSQLDataSource)
    source.configuration.set_field(name="ssl_disabled", value=False)

    with pytest.raises(Exception):
        # Execute
        source._validate_configuration()


@pytest.mark.asyncio
async def test_get_docs_postgresql(patch_logger):
    """Test get_docs method"""
    # Setup
    source = create_source(PostgreSQLDataSource)
    with patch.object(AsyncEngine, "connect", return_value=ConnectionAsync()):
        source.engine = create_async_engine(POSTGRESQL_CONNECTION_STRING)
        actual_response = []
        expected_response = [
            {
                "10_10_ids": 1,
                "10_10_names": "abcd",
                "_id": "xe_10_10_",
                "_timestamp": "",
                "Database": "xe",
                "Table": 10,
                "schema": 10,
            },
            {
                "10_10_ids": 2,
                "10_10_names": "xyz",
                "_id": "xe_10_10_",
                "_timestamp": "",
                "Database": "xe",
                "Table": 10,
                "schema": 10,
            },
        ]

        # Execute
        async for i in source.get_docs():
            i[0]["_timestamp"] = ""
            actual_response.append(i[0])

        # Assert
        assert actual_response == expected_response


@pytest.mark.asyncio
async def test_get_docs_oracle(patch_logger):
    """Test get_docs method"""
    # Setup
    source = create_source(OracleDataSource)
    with patch.object(Engine, "connect", return_value=ConnectionSync()):
        source.engine = create_engine(ORACLE_CONNECTION_STRING)
        actual_response = []
        expected_response = [
            {
                "10_ids": 1,
                "10_names": "abcd",
                "_id": "xe_10_",
                "_timestamp": "",
                "Database": "xe",
                "Table": 10,
            },
            {
                "10_ids": 2,
                "10_names": "xyz",
                "_id": "xe_10_",
                "_timestamp": "",
                "Database": "xe",
                "Table": 10,
            },
        ]

        # Execute
        async for i in source.get_docs():
            i[0]["_timestamp"] = ""
            actual_response.append(i[0])

        # Assert
        assert actual_response == expected_response


@pytest.mark.asyncio
async def test_close(patch_logger):
    """Test close method"""
    source = create_source(GenericBaseDataSource)
    await source.close()


@pytest.mark.asyncio
async def test_async_connect_negative(patch_logger):
    """Test _async_connect method with negative case"""
    source = create_source(GenericBaseDataSource)
    with patch.object(
        AsyncEngine, "connect", side_effect=InternalClientError("Something went wrong")
    ):
        source.engine = create_async_engine(POSTGRESQL_CONNECTION_STRING)

        # Execute
        with pytest.raises(InternalClientError):
            await source._async_connect("table1")


@pytest.mark.asyncio
async def test_sync_connect_negative(patch_logger):
    """Test _sync_connect method with negative case"""
    source = create_source(GenericBaseDataSource)
    with patch.object(
        Engine, "connect", side_effect=InternalClientError("Something went wrong")
    ):
        source.engine = create_engine(ORACLE_CONNECTION_STRING)

        # Execute
        with pytest.raises(InternalClientError):
            await source._sync_connect("table1")


@pytest.mark.asyncio
async def test_execute_query_negative_for_internal_client_error(patch_logger):
    """Test _execute_query method with negative case"""
    source = create_source(PostgreSQLDataSource)
    with patch.object(
        AsyncEngine, "connect", side_effect=InternalClientError("Something went wrong")
    ):
        source.engine = source.engine = create_async_engine(
            POSTGRESQL_CONNECTION_STRING
        )
        source.is_async = True

        # Execute
        with pytest.raises(InternalClientError):
            await anext(source.execute_query("PING"))


@pytest.mark.asyncio
async def test_fetch_documents_negative(patch_logger):
    """Test fetch_documents method with negative case"""
    source = create_source(GenericBaseDataSource)
    with patch.object(
        GenericBaseDataSource,
        "execute_query",
        side_effect=InternalClientError("Something went wrong"),
    ):
        source.engine = create_engine(ORACLE_CONNECTION_STRING)

        # Execute
        with pytest.raises(Exception):
            await anext(source.fetch_documents("table1"))


@pytest.mark.asyncio
async def test_execute_query_negative():
    """Test execute_query method with negative case"""
    source = create_source(OracleDataSource)
    with patch.object(
        OracleDataSource,
        "_sync_connect",
        side_effect=Exception("Something went wrong"),
    ):
        source.retry_count = 1

        # Execute
        with pytest.raises(Exception):
            await anext(source.execute_query("PING"))


@pytest.mark.parametrize(
    "tables, expected_tables",
    [
        ("", []),
        ("table", ["table"]),
        ("table_1, table_2", ["table_1", "table_2"]),
        (["table_1", "table_2"], ["table_1", "table_2"]),
        (["table_1", "table_2", ""], ["table_1", "table_2"]),
    ],
)
def test_configured_tables(tables, expected_tables):
    actual_tables = configured_tables(tables)

    assert actual_tables == expected_tables


@pytest.mark.parametrize("tables", ["*", ["*"]])
@pytest.mark.asyncio
async def test_get_tables_to_fetch_remote_tables(tables):
    source = create_source(GenericBaseDataSource)
    source.execute_query = AsyncIterator(["table"])

    await source.get_tables_to_fetch("schema")

    assert "ALL_TABLE" == source.execute_query.call_kwargs[0]["query_name"]


@pytest.mark.asyncio
async def test_get_tables_to_fetch_configured_tables():
    source = create_source(GenericBaseDataSource)
    tables = ["table_1", "table_2"]
    source.tables = tables

    assert tables == await source.get_tables_to_fetch("schema")


@pytest.mark.parametrize("tables", ["*", ["*"]])
def test_is_wildcard(tables):
    assert is_wildcard(tables)
