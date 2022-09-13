#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the MySQL source class methods"""
import asyncio
import datetime
import decimal
import random
from decimal import Decimal
from unittest import mock

import aiomysql
import pytest
from bson import Decimal128

from connectors.source import DataSourceConfiguration
from connectors.sources.mysql import MySqlDataSource
from connectors.sources.tests.support import create_source


def test_get_configuration():
    """Test get_configuration method of MySQL"""
    # Setup
    klass = MySqlDataSource

    # Execute
    config = DataSourceConfiguration(klass.get_default_configuration())

    # Assert
    assert config["host"] == "127.0.0.1"
    assert config["port"] == 3306


class Result:
    """This class contains method which returns dummy response"""

    def result(self):
        """Result method which returns dummy result"""
        return [["table1"], ["table2"]]


class Cursor:
    """This class contains methods which returns dummy response"""

    async def __aenter__(self):
        """Make a dummy database connection and return it"""
        return self

    def __init__(self, *args, **kw):
        self.description = [["Database"]]

    def fetchall(self):
        """This method returns object of Return class"""
        return Result()

    def execute(self, query):
        """This method returns future object"""
        futures_object = asyncio.Future()
        futures_object.set_result(mock.MagicMock())
        return futures_object

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass


class Connection:
    """This class contains methods which returns dummy connection response"""

    async def __aenter__(self):
        """Make a dummy database connection and return it"""
        return self

    async def ping(self):
        """This method returns object of Result class"""
        return True

    async def cursor(self):
        """This method returns object of Result class"""
        return Cursor

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass


async def mock_mysql_response():
    """Creates mock response

    Returns:
        Mock Object: Mock response
    """
    mock_response = asyncio.Future()
    mock_response.set_result(mock.MagicMock())

    return mock_response


@pytest.mark.asyncio
async def test_ping():
    """Test ping method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)

    mock_response = asyncio.Future()
    mock_response.set_result(mock.MagicMock())

    source.connection_pool = await mock_response
    source.connection_pool.acquire = Connection

    with mock.patch.object(aiomysql, "create_pool", return_value=mock_response):
        # Execute
        await source.ping()


@pytest.mark.asyncio
async def test_ping_negative():
    """Test ping method of MySqlDataSource class with negative case"""
    # Setup
    source = create_source(MySqlDataSource)

    with pytest.raises(Exception):
        # Execute
        await source.ping()


@pytest.mark.asyncio
async def test__execute_query():
    """Test _execute_query method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor

    # Execute
    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        response = await source._execute_query(query="select * from table")

        # Assert
        assert response == (["Database"], [["table1"], ["table2"]])


@pytest.mark.asyncio
async def test__execute_query_negative():
    """Test _execute_query method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor

    # Execute
    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        with pytest.raises(Exception):
            await source._execute_query()


@pytest.mark.asyncio
async def test_serialize():
    """This function test serialize method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)

    # Execute
    response = source.serialize(
        {
            "key1": "value",
            "key2": [],
            "key3": {"Key": "value"},
            "key4": datetime.datetime.now(),
            "key5": decimal.Decimal(str(random.random())),
            "key6": Decimal128(Decimal("0.0005")),
            "key7": bytes("value", "utf-8"),
        }
    )

    # Assert
    assert list(response.keys()) == [
        "key1",
        "key2",
        "key3",
        "key4",
        "key5",
        "key6",
        "key7",
    ]


@pytest.mark.asyncio
async def test_create_document():
    """This function test create_document method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor

    query = "select * from table"

    # Execute
    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        response = await source._execute_query(query)

        mock.patch("source._execute_query", return_value=response)

    response = source.create_document(
        database="database_name", table="table_name", query=query
    )

    # Assert
    async for document in response:
        assert document == {
            "Database": "database_name",
            "Table": "table_name",
            "_id": "database_name_table_name_",
            "timestamp": "table1",
        }


@pytest.mark.asyncio
async def test_fetch_rows():
    """This function test fetch_rows method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor

    query = "select * from table"

    # Execute
    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        response = await source._execute_query(query)

        mock.patch("source._execute_query", return_value=response)

    # Assert
    async for row in source.fetch_rows(database="database_name"):
        assert row.get("_id")


@pytest.mark.asyncio
async def test_fetch_all_databases():
    """This function test fetch_all_databases method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor

    query = "SHOW DATABASES"

    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        response = await source._execute_query(query)

        mock.patch("source._execute_query", return_value=response)

    # Execute
    response = await source._fetch_all_databases()

    # Assert
    assert response == ["table1", "table2"]


class AsyncIter:
    """This Class is use to return async generator"""

    def __init__(self, items):
        """Setup list of dictionary"""
        self.items = items

    async def __aiter__(self):
        """This Method is used to return async generator"""
        for item in self.items:
            yield item


@pytest.mark.asyncio
async def test_get_docs():
    # Setup
    source = create_source(MySqlDataSource)
    source.configuration.set_field(name="database", value=["database_1"])

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor

    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        source.fetch_rows = mock.MagicMock(return_value=AsyncIter([{"a": 1, "b": 2}]))

    # Execute
    async for doc, _ in source.get_docs():
        assert doc == {"a": 1, "b": 2}
