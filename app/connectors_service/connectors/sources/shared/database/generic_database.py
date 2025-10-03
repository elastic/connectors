#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from abc import ABC, abstractmethod

from asyncpg.exceptions._base import InternalClientError
from sqlalchemy.exc import ProgrammingError

from connectors.utils import RetryStrategy, retryable

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
                (table.strip() for table in tables.split(",")),
            )
        )
        if isinstance(tables, str)
        else list(filter(lambda table: table_filter(table), tables))
    )


def is_wildcard(tables):
    return tables in (WILDCARD, [WILDCARD])


def map_column_names(column_names, schema=None, tables=None):
    prefix = ""
    if schema and len(schema.strip()) > 0:
        prefix += schema.strip() + "_"
    if tables and len(tables) > 0:
        prefix += f"{'_'.join(sorted(tables))}_"
    return [f"{prefix}{column}".lower() for column in column_names]


def hash_id(tables, row, primary_key_columns):
    """Generates an id using table names as prefix in sorted order and primary key values.

    Example:
        tables: table1, table2
        primary key values: 1, 42
        table1_table2_1_42
    """

    if not isinstance(tables, list):
        tables = [tables]

    return (
        f"{'_'.join(sorted(tables))}_"
        f"{'_'.join([str(pk_value) for pk in primary_key_columns if (pk_value := row.get(pk)) is not None])}"
    )


async def fetch(
    cursor_func,
    fetch_columns=False,
    fetch_size=DEFAULT_FETCH_SIZE,
    retry_count=DEFAULT_RETRY_COUNT,
):
    @retryable(
        retries=retry_count,
        interval=DEFAULT_WAIT_MULTIPLIER,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[InternalClientError, ProgrammingError],
    )
    async def _execute():
        cursor = await cursor_func()
        # sending back column names if required
        if fetch_columns:
            yield cursor.keys()

        while True:
            rows = cursor.fetchmany(size=fetch_size)  # pyright: ignore
            rows_length = len(rows)

            if not rows_length:
                break

            for row in rows:
                yield row

            if rows_length < fetch_size:
                break

            await asyncio.sleep(0)

    async for result in _execute():
        yield result


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
