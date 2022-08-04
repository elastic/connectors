#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the MySQL source class methods"""
import aiomysql
import asyncio
import pytest
from unittest import mock

from connectors.source import DataSourceConfiguration
from connectors.sources.mysql import MySqlDataSource
from connectors.sources.tests.support import create_source


class Result:
    """This class contains method which returns dummy response"""

    def result(self):
        """Result method which returns dummy result"""
        return [["table1"], ["table2"]]


class Cursor:
    """This class contains methods which returns dummy response"""

    def __init__(self, *args, **kw):
        self.description = [["Database"]]

    def fetchall(self):
        """This method returns object of Result class"""
        return Result()

    def execute(self, query):
        """This method returns future object"""
        futures_object = asyncio.Future()
        futures_object.set_result(mock.MagicMock())
        return futures_object

    def close(self):
        """This method returns future object"""
        futures_object = asyncio.Future()
        futures_object.set_result(True)
        return futures_object


class Connection:
    """This class contains methods which returns dummy connection"""

    def __init__(self, *args, **kw):
        self.connection = True

    def ping(self):
        """This method returns dummy ping connection"""
        futures_object = asyncio.Future()
        futures_object.set_result(mock.MagicMock())
        return futures_object

    def cursor(self):
        """This method returns dummy cursor future object"""
        futures_object = asyncio.Future()
        futures_object.set_result(Cursor())
        return futures_object

    def close(self):
        """This method returns dummy response"""
        return True


class ConnectionNegative:
    """This class contains methods which returns negative dummy connection"""

    def __init__(self, *args, **kw):
        self.connection = True

    def ping(self):
        """This method returns dummy ping connection"""
        raise Exception

    def cursor(self):
        """This method returns dummy cursor future object"""
        futures_object = asyncio.Future()
        futures_object.set_result(Cursor())
        return futures_object

    def close(self):
        """This method returns dummy response"""
        return True


class MysqlDatabaseConnectionManager(object):
    """This is the context manager which handles dummy MySQL server connection"""

    def __init__(self, *args, **kw):
        self.cursor = Cursor()
        self.connection = Connection()

    async def __aenter__(self):
        """make a dummy database connection and return it"""
        return self

    async def __aexit__(self, exception_type, exception_value, traceback):
        """make sure the dummy database connection gets closed"""
        pass


def test_get_configuration():
    """Test get_configuration method of MySQL"""
    # Setup
    klass = MySqlDataSource

    # Execute
    config = DataSourceConfiguration(klass.get_default_configuration())

    # Assert
    assert config["host"] == "localhost"
    assert config["port"] == 3306


@pytest.mark.asyncio
async def test_ping():
    """Test ping method of MySqlDataSource class"""

    # Setup
    mock_response = asyncio.Future()
    mock_response.set_result(Connection())

    with mock.patch.object(
            aiomysql, "connect", return_value=mock_response
    ):
        source = create_source(MySqlDataSource)

        # Execute
        await source.ping()


@pytest.mark.asyncio
async def test_ping_negative():
    """Test ping method of MySqlDataSource class with negative case"""

    # Setup
    mock_response = asyncio.Future()
    mock_response.set_result(ConnectionNegative())

    with mock.patch.object(
            aiomysql, "connect", return_value=mock_response
    ):
        source = create_source(MySqlDataSource)

        # Execute
        with pytest.raises(Exception):
            await source.ping()


@pytest.mark.asyncio
async def test_database_connection_error():
    """Test DatabaseConnectionError of MySQL"""
    # Setup
    source = create_source(MySqlDataSource)
    with pytest.raises(Exception):
        # Execute
        await source.ping()
