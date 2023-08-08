#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Generic Database source class methods"""
import pytest

from connectors.sources.generic_database import configured_tables, is_wildcard

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
def test_is_wildcard(tables):
    assert is_wildcard(tables)
