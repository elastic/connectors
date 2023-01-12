#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Generic Database source class methods"""
import datetime
import decimal
import random
from decimal import Decimal
from unittest.mock import patch

import pytest
from bson import Decimal128
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.engine import AsyncEngine

from connectors.source import DataSourceConfiguration
from connectors.sources.generic_database import GenericBaseDataSource
from connectors.sources.mssql import MSSQLDataSource
from connectors.sources.oracle import OracleDataSource
from connectors.sources.postgresql import PostgreSQLDataSource
from connectors.sources.tests.support import create_source


class connection_async:
    """This Class create dummy connection with database and return dummy cursor"""

    async def __aenter__(self):
        """Make a dummy database connection and return it"""
        return self

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass

    async def execute(self, query):
        """This method returns dummy cursor"""
        return cursor_async()


class cursor_async:
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
        return [("testdb",)]

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass


class connection_sync:
    """This Class create dummy connection with database and return dummy cursor"""

    def __enter__(self):
        """Make a dummy database connection and return it"""
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """Make sure the dummy database connection gets closed"""
        pass

    def execute(self, statement):
        """This method returns dummy cursor"""
        return cursor_sync()


class cursor_sync:
    """This class contains methods which returns dummy response"""

    async def __enter__(self):
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
        return [("ORCLCDB",)]


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
    source = create_source(GenericBaseDataSource)
    source.configuration.set_field(name="ssl_disabled", value=False)

    with pytest.raises(Exception):

        # Execute
        source._validate_configuration()


@pytest.mark.asyncio
async def test_serialize(patch_logger):
    """This function test serialize method of GDC"""
    # Setup
    source = create_source(GenericBaseDataSource)

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
async def test_get_docs_postgresql(patch_logger):
    """Test get_docs method"""
    # Setup
    source = create_source(PostgreSQLDataSource)
    with patch.object(AsyncEngine, "connect", return_value=connection_async()):
        source.engine = create_async_engine(
            "postgresql+asyncpg://admin:changme@127.0.0.1:5432/testdb"
        )
        actual_response = []
        expected_response = [
            {
                "testdb_testdb_ids": 1,
                "testdb_testdb_names": "abcd",
                "_id": "xe_testdb_testdb_",
                "_timestamp": "",
                "Database": "xe",
                "Table": "testdb",
                "schema": "testdb",
            },
            {
                "testdb_testdb_ids": 2,
                "testdb_testdb_names": "xyz",
                "_id": "xe_testdb_testdb_",
                "_timestamp": "",
                "Database": "xe",
                "Table": "testdb",
                "schema": "testdb",
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
    with patch.object(Engine, "connect", return_value=connection_sync()):
        source.engine = create_engine("oracle://admin:changme@127.0.0.1:1521/testdb")
        actual_response = []
        expected_response = [
            {
                "orclcdb_ids": 1,
                "orclcdb_names": "abcd",
                "_id": "xe_ORCLCDB_",
                "_timestamp": "",
                "Database": "xe",
                "Table": "ORCLCDB",
            },
            {
                "orclcdb_ids": 2,
                "orclcdb_names": "xyz",
                "_id": "xe_ORCLCDB_",
                "_timestamp": "",
                "Database": "xe",
                "Table": "ORCLCDB",
            },
        ]

        # Execute
        async for i in source.get_docs():
            i[0]["_timestamp"] = ""
            actual_response.append(i[0])

        # Assert
        assert actual_response == expected_response


@pytest.mark.asyncio
async def test_get_docs_mssql(patch_logger):
    """Test get_docs method"""
    # Setup
    source = create_source(MSSQLDataSource)
    with patch.object(Engine, "connect", return_value=connection_sync()):
        source.engine = create_engine(
            "mssql+pymssql://admin:changme@127.0.0.1:1433/testdb"
        )
        actual_response = []
        expected_response = [
            {
                "orclcdb_orclcdb_ids": 1,
                "orclcdb_orclcdb_names": "abcd",
                "_id": "xe_ORCLCDB_ORCLCDB_",
                "_timestamp": "",
                "Database": "xe",
                "Table": "ORCLCDB",
                "schema": "ORCLCDB",
            },
            {
                "orclcdb_orclcdb_ids": 2,
                "orclcdb_orclcdb_names": "xyz",
                "_id": "xe_ORCLCDB_ORCLCDB_",
                "_timestamp": "",
                "Database": "xe",
                "Table": "ORCLCDB",
                "schema": "ORCLCDB",
            },
        ]

        # Execute
        async for i in source.get_docs():
            i[0]["_timestamp"] = ""
            actual_response.append(i[0])

        # Assert
        assert actual_response == expected_response
