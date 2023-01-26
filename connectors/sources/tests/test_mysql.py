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
import ssl
from decimal import Decimal
from unittest import mock

import aiomysql
import pytest
from bson import Decimal128

from connectors.source import DataSourceConfiguration
from connectors.sources.mysql import MySqlDataSource
from connectors.sources.tests.support import create_source


@pytest.fixture
def patch_default_wait_multiplier():
    with mock.patch("connectors.sources.mysql.DEFAULT_WAIT_MULTIPLIER", 0):
        yield


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
        self.first_call = True
        self.description = [["Database"]]

    def fetchall(self):
        """This method returns object of Return class"""
        futures_object = asyncio.Future()
        futures_object.set_result([["table1"], ["table2"]])
        return futures_object

    async def fetchmany(self, size=1):
        """This method returns response of fetchmany"""
        if self.first_call:
            self.first_call = False
            return [["table1"], ["table2"]]
        if self.is_connection_lost:
            raise Exception("Incomplete Read Error")
        return []

    async def scroll(self, *args, **kw):
        raise Exception("Incomplete Read Error")

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


class ConnectionPool:
    def close(self):
        pass

    async def wait_closed(self):
        pass


class mock_ssl:
    """This class contains methods which returns dummy ssl context"""

    def load_verify_locations(self, cadata):
        """This method verify locations"""
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
async def test_close_without_connection_pool():
    """Test close method of MySql without connection pool"""
    # Setup
    source = create_source(MySqlDataSource)

    source.connection_pool = None

    # Execute
    await source.close()


@pytest.mark.asyncio
async def test_close_with_connection_pool():
    """Test close method of MySql with connection pool"""
    # Setup
    source = create_source(MySqlDataSource)
    source.connection_pool = ConnectionPool()
    source.connection_pool.acquire = Connection

    # Execute
    await source.close()


@pytest.mark.asyncio
async def test_ping(patch_logger):
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
async def test_ping_negative(patch_logger):
    """Test ping method of MySqlDataSource class with negative case"""
    # Setup
    source = create_source(MySqlDataSource)

    mock_response = asyncio.Future()
    mock_response.set_result(mock.Mock())

    source.connection_pool = await mock_response

    with mock.patch.object(aiomysql, "create_pool", return_value=mock_response):
        with pytest.raises(Exception):
            # Execute
            await source.ping()


@pytest.mark.asyncio
async def test_connect_with_retry(patch_logger, patch_default_wait_multiplier):
    """Test _connect method of MySQL with retry"""
    # Setup
    source = create_source(MySqlDataSource)

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor
    source.connection_pool.acquire.cursor.is_connection_lost = True

    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        # Execute
        streamer = source._connect(
            query_name="TABLE_DATA", fetch_many=True, database="db", table="table"
        )

        with pytest.raises(Exception):
            async for response in streamer:
                response


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
async def test_fetch_documents():
    """This function test fetch_documents method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor
    source.connection_pool.acquire.cursor.is_connection_lost = False

    query_name = "select * from table"

    # Execute
    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        response = source._connect(query_name)

        mock.patch("source._connect", return_value=response)

    response = source.fetch_documents(database="database_name", table="table_name")

    # Assert
    document_list = []
    async for document in response:
        document_list.append(document)

    assert {
        "Database": "database_name",
        "Table": "table_name",
        "_id": "database_name_table_name_",
        "_timestamp": "table1",
        "database_name_table_name_Database": "table1",
    } in document_list


@pytest.mark.asyncio
async def test_fetch_rows():
    """This function test fetch_rows method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor
    source.connection_pool.acquire.cursor.is_connection_lost = False

    query = "select * from table"

    # Execute
    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        response = source._connect(query)

        mock.patch("source._connect", return_value=response)

    # Assert
    async for row in source.fetch_rows(database="database_name"):
        assert "_id" in row


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
async def test_get_docs_with_list():
    """Test get docs method of MySql with input as list for database field"""
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

    with mock.patch.object(MySqlDataSource, "_validate_databases", return_value=([])):
        # Execute
        async for doc, _ in source.get_docs():
            assert doc == {"a": 1, "b": 2}


@pytest.mark.asyncio
async def test_get_docs_with_str():
    """Test get docs method of MySql with input as str for database field"""
    # Setup
    source = create_source(MySqlDataSource)
    source.configuration.set_field(name="database", value="database_1")

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor

    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        source.fetch_rows = mock.MagicMock(return_value=AsyncIter([{"a": 1, "b": 2}]))

    with mock.patch.object(MySqlDataSource, "_validate_databases", return_value=([])):
        # Execute
        async for doc, _ in source.get_docs():
            assert doc == {"a": 1, "b": 2}


@pytest.mark.asyncio
async def test_get_docs_with_empty():
    """Test get docs method of MySql with input as empty for database field"""
    # Setup
    source = create_source(MySqlDataSource)
    source.configuration.set_field(name="database", value="")

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor

    # Execute
    with pytest.raises(Exception):
        async for doc, _ in source.get_docs():
            doc


@pytest.mark.asyncio
async def test_get_docs():
    """Test get docs method of Mysql"""
    # Setup
    source = create_source(MySqlDataSource)

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor

    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        source.fetch_rows = mock.MagicMock(return_value=AsyncIter([{"a": 1, "b": 2}]))

    with mock.patch.object(MySqlDataSource, "_validate_databases", return_value=([])):
        # Execute
        async for doc, _ in source.get_docs():
            assert doc == {"a": 1, "b": 2}


@pytest.mark.asyncio
async def test_validate_databases():
    """This function test _validate_databases method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)

    source.connection_pool = await mock_mysql_response()
    source.connection_pool.acquire = Connection
    source.connection_pool.acquire.cursor = Cursor

    query = "SHOW DATABASES"

    with mock.patch.object(
        aiomysql, "create_pool", return_value=(await mock_mysql_response())
    ):
        response = source._connect(query)

        mock.patch("source._connect", return_value=response)

    # Execute
    response = await source._validate_databases([])

    # Assert
    assert response == []


def test_validate_configuration():
    """This function test _validate_configuration method of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)
    source.configuration.set_field(name="host", value="")

    # Execute
    with pytest.raises(Exception):
        source._validate_configuration()


def test_validate_configuration_with_port():
    """This function test _validate_configuration method with port str input of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)
    source.configuration.set_field(name="port", value="port")

    # Execute
    with pytest.raises(Exception):
        source._validate_configuration()


def test_ssl_context():
    """This function test _ssl_context with dummy certificate"""
    # Setup
    certificate = "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"
    source = create_source(MySqlDataSource)

    # Execute
    with mock.patch.object(ssl, "create_default_context", return_value=mock_ssl()):
        source._ssl_context(certificate=certificate)
