#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Generic Database source class methods"""
from unittest.mock import Mock, patch

import pytest
from asyncpg.exceptions._base import InternalClientError
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.engine import AsyncEngine

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.generic_database import (
    GenericBaseDataSource,
    configured_tables,
    is_wildcard,
)
from connectors.sources.postgresql import PostgreSQLDataSource
from tests.commons import AsyncIterator
from tests.sources.support import create_source

POSTGRESQL_CONNECTION_STRING = (
    "postgresql+asyncpg://admin:changme@127.0.0.1:5432/testdb"
)
ORACLE_CONNECTION_STRING = "oracle+oracledb://admin:changme@127.0.0.1:1521/testdb"
SCHEMA = "dbo"
TABLE = "emp_table"
USER = "ADMIN"


class ConnectionSync:
    """This Class create dummy connection with database and return dummy cursor"""

    def __init__(self, query_object):
        """Setup dummy connection"""
        self.query_object = query_object

    def __enter__(self):
        """Make a dummy database connection and return it"""
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass

    def execute(self, statement):
        """This method returns dummy cursor"""
        return CursorSync(query_object=self.query_object, statement=statement)

    def close(self):
        pass


class CursorSync:
    """This class contains methods which returns dummy response"""

    def __enter__(self):
        """Make a dummy database connection and return it"""
        return self

    def __init__(self, query_object, *args, **kwargs):
        """Setup dummy cursor"""
        self.first_call = True
        self.query = kwargs["statement"]
        self.query_object = query_object

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
        """This method returns results of query"""
        self.query = str(self.query)
        if self.query == self.query_object.all_schemas():
            return [(SCHEMA,)]
        elif self.query == self.query_object.all_tables(schema=SCHEMA, user=USER):
            return [(TABLE,)]
        elif self.query == self.query_object.table_data_count(
            schema=SCHEMA, table=TABLE
        ):
            return [(10,)]
        elif self.query == self.query_object.table_primary_key(
            schema=SCHEMA, table=TABLE, user=USER
        ):
            return [("ids",)]
        elif self.query == self.query_object.table_last_update_time(
            schema=SCHEMA, table=TABLE
        ):
            return [("2023-02-21T08:37:15+00:00",)]


def test_get_configuration():
    """Test get_configuration method of GenericBaseDataSource class"""

    # Setup
    klass = GenericBaseDataSource

    # Execute
    config = DataSourceConfiguration(klass.get_default_configuration())

    # Assert
    assert config["host"] == "127.0.0.1"


@pytest.mark.asyncio
async def test_validate_config_valid_fields():
    # Setup
    source = create_source(PostgreSQLDataSource)

    # Execute
    try:
        await source.validate_config()
    except ConfigurableFieldValueError as e:
        raise AssertionError("Method raised an exception") from e


@pytest.mark.parametrize(
    "field", ["host", "port", "username", "password", "database", "tables"]
)
@pytest.mark.asyncio
async def test_validate_config_missing_fields(field):
    # Setup
    source = create_source(GenericBaseDataSource)
    with pytest.raises(ConfigurableFieldValueError):
        source.configuration.set_field(name=field, value="")

        # Execute
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_ssl():
    """Test validate_config method check ssl"""
    # Setup
    source = create_source(PostgreSQLDataSource)
    source.configuration.set_field(name="ssl_enabled", value=True)

    with pytest.raises(ConfigurableFieldValueError):
        # Execute
        await source.validate_config()


@pytest.mark.asyncio
async def test_close():
    source = create_source(GenericBaseDataSource)

    await source.close()


@pytest.mark.asyncio
async def test_async_connect_negative():
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
async def test_sync_connect_negative():
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
async def test_execute_query_negative_for_internal_client_error():
    """Test _execute_query method with negative case"""
    source = create_source(GenericBaseDataSource)
    with patch.object(
        AsyncEngine, "connect", side_effect=InternalClientError("Something went wrong")
    ):
        source.engine = source.engine = create_async_engine(
            POSTGRESQL_CONNECTION_STRING
        )
        source.is_async = True

        # Execute
        with pytest.raises(InternalClientError):
            await anext(source.execute_query("select 1+1"))


@pytest.mark.asyncio
async def test_fetch_documents_negative():
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


@pytest.fixture
def patch_default_wait_multiplier():
    """Patch wait multiplier to 0"""
    with patch("connectors.sources.generic_database.DEFAULT_WAIT_MULTIPLIER", 0):
        yield


@pytest.mark.asyncio
async def test_execute_query_negative(patch_default_wait_multiplier):
    """Test execute_query method when server is unavailable"""
    source = create_source(GenericBaseDataSource)
    with patch.object(
        GenericBaseDataSource,
        "_sync_connect",
        side_effect=Exception("Something went wrong"),
    ):
        # Execute
        with pytest.raises(Exception, match="Something went wrong"):
            await anext(source.execute_query("select 1+1 from dual"))


@pytest.mark.asyncio
async def test_ping():
    # Setup
    source = create_source(GenericBaseDataSource)
    source._create_engine = Mock()
    source.queries = Mock()
    source.execute_query = Mock(return_value=AsyncIterator(["table1", "table2"]))

    # Execute
    await source.ping()


@pytest.mark.asyncio
async def test_ping_negative():
    """Test ping method of GenericBaseDataSource class when connection is not established"""
    # Setup
    source = create_source(GenericBaseDataSource)

    with patch.object(
        GenericBaseDataSource,
        "execute_query",
        side_effect=Exception("Something went wrong"),
    ):
        source.dialect = "Oracle"
        # Execute
        with pytest.raises(Exception, match="Can't connect to Oracle on 127.0.0.1"):
            await source.ping()


@pytest.mark.asyncio
async def test_fetch_rows_with_zero_table():
    """Test fetch_rows method when no tables found in schema/database"""
    # Setup
    source = create_source(GenericBaseDataSource)
    source._create_engine = Mock()
    source.queries = Mock()
    source.execute_query = Mock(return_value=AsyncIterator([[]]))

    # Execute
    async for doc in source.fetch_rows():
        assert doc == []

    source.execute_query = Mock(return_value=AsyncIterator([[]]))
    # Execute
    async for doc in source.fetch_rows(schema="public"):
        assert doc == []
        with pytest.raises(Exception):
            await anext(source.execute_query("select 1+1"))


@pytest.mark.asyncio
async def test_fetch_rows_with_msdb():
    # Setup
    source = create_source(GenericBaseDataSource)
    source._create_engine = Mock()
    source.queries = Mock()
    source.execute_query = Mock(return_value=AsyncIterator([[]]))
    source.database = "msdb"

    # Execute and Assert
    async for doc in source.fetch_rows():
        assert doc == []


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
    source.queries = Mock()

    await source.get_tables_to_fetch("schema")

    assert (
        source.queries.all_tables(schema="schema")
        == source.execute_query.call_kwargs[0]["query"]
    )


@pytest.mark.asyncio
async def test_get_tables_to_fetch_configured_tables():
    source = create_source(GenericBaseDataSource)
    tables = ["table_1", "table_2"]
    source.queries = Mock()
    source.tables = tables

    assert tables == await source.get_tables_to_fetch("schema")


@pytest.mark.parametrize("tables", ["*", ["*"]])
def test_is_wildcard(tables):
    assert is_wildcard(tables)
