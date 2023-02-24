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

from connectors.source import DataSourceConfiguration
from connectors.sources.generic_database import GenericBaseDataSource
from connectors.sources.mssql import MSSQLQueries
from connectors.sources.oracle import OracleDataSource
from connectors.sources.postgresql import PostgreSQLDataSource
from connectors.sources.tests.support import create_source
from connectors.tests.commons import AsyncIterator

POSTGRESQL_CONNECTION_STRING = (
    "postgresql+asyncpg://admin:changme@127.0.0.1:5432/testdb"
)
ORACLE_CONNECTION_STRING = "oracle+oracledb://admin:changme@127.0.0.1:1521/testdb"


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
        return CursorSync(statement=statement)

    def close(self):
        pass


class CursorSync:
    """This class contains methods which returns dummy response"""

    def __enter__(self):
        """Make a dummy database connection and return it"""
        return self

    def __init__(self, *args, **kw):
        self.first_call = True
        self.query = kw["statement"]

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
        self.query = str(self.query)
        if (
            self.query
            == "SELECT s.name from sys.schemas s inner join sys.sysusers u on u.uid = s.principal_id"
        ):
            return [("public",)]
        elif self.query in [
            "SELECT table_name FROM information_schema.tables WHERE TABLE_SCHEMA = 'public'",
            "SELECT TABLE_NAME FROM all_tables where OWNER = 'ADMIN'",
        ]:
            return [("emp_table",)]
        elif self.query in [
            'SELECT COUNT(*) FROM public."emp_table"',
            "SELECT COUNT(*) FROM emp_table",
        ]:
            return [(10,)]
        elif self.query in [
            "SELECT C.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME WHERE C.TABLE_NAME='emp_table' and C.TABLE_SCHEMA='public' and T.CONSTRAINT_TYPE='PRIMARY KEY'",
            "SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = 'emp_table' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = 'ADMIN' AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position",
        ]:
            return [("ids",)]
        elif self.query in [
            "SELECT SCN_TO_TIMESTAMP(MAX(ora_rowscn)) from emp_table",
            "SELECT last_user_update FROM sys.dm_db_index_usage_stats WHERE object_id=object_id('public.emp_table')",
        ]:
            return [("2023-02-21T08:37:15+00:00",)]


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
async def test_execute_query_negative_for_internalclienterror(patch_logger):
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


@pytest.fixture
def patch_default_wait_multiplier():
    """Patch retry interval to 0"""
    with patch("connectors.sources.generic_database.DEFAULT_WAIT_MULTIPLIER", 0):
        yield


@pytest.mark.asyncio
async def test_execute_query_negative(patch_default_wait_multiplier):
    """Test execute_query method with negative case"""
    source = create_source(OracleDataSource)
    with patch.object(
        OracleDataSource,
        "_sync_connect",
        side_effect=Exception("Something went wrong"),
    ):
        # Execute
        with pytest.raises(Exception):
            await anext(source.execute_query("PING"))


@pytest.mark.asyncio
async def test_ping(patch_logger):
    """Test ping method of MSSQLDataSource class"""
    # Setup
    source = create_source(GenericBaseDataSource)
    source._create_engine = Mock()
    source.queries = MSSQLQueries()
    source.execute_query = Mock(return_value=AsyncIterator(["table1", "table2"]))

    # Execute
    await source.ping()


@pytest.mark.asyncio
async def test_ping_negative(patch_logger):
    """Test ping method of MSSQLDataSource class with negative case"""
    # Setup
    source = create_source(GenericBaseDataSource)

    with patch.object(
        GenericBaseDataSource,
        "execute_query",
        side_effect=Exception("Something went wrong"),
    ):
        # Execute
        with pytest.raises(Exception):
            await source.ping()


@pytest.mark.asyncio
async def test_fetch_rows_with_zero_table():
    # Setup
    source = create_source(GenericBaseDataSource)
    source._create_engine = Mock()
    source.queries = MSSQLQueries()
    source.execute_query = Mock(return_value=AsyncIterator([[]]))

    # Execute
    async for doc in source.fetch_rows():
        assert doc == []

    source.execute_query = Mock(return_value=AsyncIterator([[]]))
    # Execute
    async for doc in source.fetch_rows(schema="public"):
        assert doc == []
