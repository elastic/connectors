#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from abc import ABC, abstractmethod

WILDCARD = "*"

DEFAULT_FETCH_SIZE = 50
DEFAULT_RETRY_COUNT = 3
DEFAULT_WAIT_MULTIPLIER = 2


def configured_tables(tables):
    """Split a string containing a comma-seperated list of tables by comma and strip the table names.

    Filter out `None` and zero-length values from the tables.
    If `tables` is a list return the list also without `None` and zero-length values.

    Arguments:
    - `tables`: string containing a comma-seperated list of tables or a list of tables
    """

    def table_filter(table):
        return table is not None and len(table) > 0

    return (
        list(
            filter(
                lambda table: table_filter(table),
                map(lambda table: table.strip(), tables.split(",")),
            )
        )
        if isinstance(tables, str)
        else list(filter(lambda table: table_filter(table), tables))
    )


def is_wildcard(tables):
    return tables in (WILDCARD, [WILDCARD])


class Queries(ABC):
    """Class contains abstract methods for queries"""

    @abstractmethod
    def ping(self):
        """Query to ping source"""
        pass

    @abstractmethod
    def all_tables(self, **kwargs):
        """Query to get all tables"""
        pass

    @abstractmethod
    def table_primary_key(self, **kwargs):
        """Query to get the primary key"""
        pass

    @abstractmethod
    def table_data(self, **kwargs):
        """Query to get the table data"""
        pass

    @abstractmethod
    def table_last_update_time(self, **kwargs):
        """Query to get the last update time of the table"""
        pass

    @abstractmethod
    def table_data_count(self, **kwargs):
        """Query to get the number of rows in the table"""
        pass

    @abstractmethod
    def all_schemas(self):
        """Query to get all schemas of database"""
        pass
