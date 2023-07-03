#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from abc import ABC, abstractmethod
from functools import partial

from asyncpg.exceptions._base import InternalClientError
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from connectors.source import BaseDataSource
from connectors.utils import iso_utc

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


class GenericBaseDataSource(BaseDataSource):
    """Class contains common functionalities for Generic Database connector"""

    def __init__(self, configuration):
        """Setup connection to the database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)

        # Connector configurations
        self.retry_count = self.configuration["retry_count"]

        # Connection related configurations
        self.user = self.configuration["username"]
        self.password = self.configuration["password"]
        self.host = self.configuration["host"]
        self.port = self.configuration["port"]
        self.database = self.configuration["database"]
        self.tables = self.configuration["tables"]
        self.is_async = False
        self.engine = None
        self.dialect = ""
        self.connection = None
        self.queries = None
        self.certfile = ""
        self.tables_to_skip = {}

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for database-server configured by user

        Returns:
            dictionary: Default configuration
        """
        return {
            "host": {
                "label": "Host",
                "order": 1,
                "type": "str",
                "value": "127.0.0.1",
            },
            "port": {
                "display": "numeric",
                "label": "Port",
                "order": 2,
                "type": "int",
                "value": 9090,
            },
            "username": {
                "label": "Username",
                "order": 3,
                "type": "str",
                "value": "admin",
            },
            "password": {
                "label": "Password",
                "order": 4,
                "sensitive": True,
                "type": "str",
                "value": "Password_123",
            },
            "database": {
                "label": "Database",
                "order": 5,
                "type": "str",
                "value": "xe",
            },
            "tables": {
                "display": "textarea",
                "label": "Comma-separated list of tables",
                "options": [],
                "order": 6,
                "type": "list",
                "value": WILDCARD,
            },
            "fetch_size": {
                "default_value": DEFAULT_FETCH_SIZE,
                "display": "numeric",
                "label": "Rows fetched per request",
                "order": 7,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": DEFAULT_FETCH_SIZE,
            },
            "retry_count": {
                "default_value": DEFAULT_RETRY_COUNT,
                "display": "numeric",
                "label": "Retries per request",
                "order": 8,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": DEFAULT_RETRY_COUNT,
            },
        }

    async def execute_query(self, query, fetch_many=False, **kwargs):
        """Executes a query and yield rows

        Args:
            query (str): Query.
            fetch_many (bool): Flag to use fetchmany method. Defaults to False.

        Raises:
            exception: Raise an exception after retrieving

        Yields:
            list: Column names and query response
        """
        size = self.configuration["fetch_size"]

        retry = 1
        yield_once = True

        rows_fetched = 0

        while retry <= self.retry_count:
            try:
                if self.is_async:
                    cursor = await self._async_connect(query=query)
                else:
                    cursor = await self._sync_connect(query=query)
                if fetch_many:
                    # sending back column names only once
                    if yield_once:
                        if kwargs["schema"] is not None:
                            yield [
                                f"{kwargs['schema']}_{kwargs['table']}_{column}".lower()
                                for column in cursor.keys()  # pyright: ignore
                            ]
                        else:
                            yield [
                                f"{kwargs['table']}_{column}".lower()
                                for column in cursor.keys()  # pyright: ignore
                            ]
                        yield_once = False

                    while True:
                        rows = cursor.fetchmany(size=size)  # pyright: ignore
                        rows_length = len(rows)

                        if not rows_length:
                            break

                        for row in rows:
                            yield row

                        rows_fetched += rows_length
                        await asyncio.sleep(0)
                else:
                    yield cursor.fetchall()  # pyright: ignore
                break
            except (InternalClientError, ProgrammingError):
                raise
            except Exception as exception:
                self._logger.warning(
                    f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}"
                )
                if retry == self.retry_count:
                    raise exception
                await asyncio.sleep(DEFAULT_WAIT_MULTIPLIER**retry)
                retry += 1

    async def _async_connect(self, query):
        """Execute the passed query on the Async supported Database server and return cursor.

        Args:
            query (str): Database query to be executed.

        Returns:
            cursor: Asynchronous cursor
        """
        try:
            if self.engine is None:
                self._create_engine()
            async with self.engine.connect() as connection:  # pyright: ignore
                cursor = await connection.execute(text(query))
                return cursor
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while executing query. Exception: {exception}"
            )
            raise

    async def close(self):
        """Close the connection to the database server."""
        if self.connection is None:
            return
        self.connection.close()

    async def _sync_connect(self, query):
        """Executes the passed query on the Non-Async supported Database server and return cursor.

        Args:
            query (str): Database query to be executed.

        Returns:
            cursor: Synchronous cursor
        """
        try:
            if self.engine is None:
                self._create_engine()
            loop = asyncio.get_running_loop()
            if self.connection is None:
                self.connection = await loop.run_in_executor(
                    executor=None, func=self.engine.connect  # pyright: ignore
                )
            cursor = await loop.run_in_executor(
                executor=None,
                func=partial(self.connection.execute, statement=text(query)),
            )
            return cursor
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while executing query. Exception: {exception}"
            )
            raise

    def _create_engine(self):
        """Create engine based on dialects"""
        raise NotImplementedError

    async def ping(self):
        """Verify the connection with the database-server configured by user"""
        self._logger.info("Validating the Connector Configuration...")
        try:
            if self.queries is None:
                raise NotImplementedError

            await anext(
                self.execute_query(
                    query=self.queries.ping(),
                )
            )
            self._logger.info(f"Successfully connected to {self.dialect}.")
        except Exception as e:
            raise Exception(f"Can't connect to {self.dialect} on {self.host}") from e

    async def fetch_documents(self, table, schema=None):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            table (str): Name of table
            schema (str): Name of schema. Defaults to None.

        Yields:
            Dict: Document to be indexed
        """
        try:
            if self.queries is None:
                raise NotImplementedError

            [[row_count]] = await anext(
                self.execute_query(
                    query=self.queries.table_data_count(
                        schema=schema,
                        table=table,
                    ),
                )
            )
            if row_count > 0:
                # Query to get the table's primary key
                columns = await anext(
                    self.execute_query(
                        query=self.queries.table_primary_key(
                            user=self.user.upper(),
                            database=self.database,
                            schema=schema,
                            table=table,
                        ),
                    )
                )
                if schema:
                    keys = [
                        f"{schema}_{table}_{column_name}"
                        for [column_name] in columns
                        if column_name
                    ]
                else:
                    keys = [
                        f"{table}_{column_name}"
                        for [column_name] in columns
                        if column_name
                    ]
                if keys:
                    try:
                        last_update_time = await anext(
                            self.execute_query(
                                query=self.queries.table_last_update_time(
                                    database=self.database,
                                    schema=schema,
                                    table=table,
                                ),
                            )
                        )
                        last_update_time = last_update_time[0][0]
                    except Exception:
                        self._logger.warning(
                            f"Unable to fetch last_updated_time for {table}"
                        )
                        last_update_time = None
                    streamer = self.execute_query(
                        query=self.queries.table_data(
                            schema=schema,
                            table=table,
                        ),
                        fetch_many=True,
                        schema=schema,
                        table=table,
                    )
                    column_names = await anext(streamer)
                    async for row in streamer:
                        row = dict(zip(column_names, row, strict=True))
                        keys_value = ""
                        for key in keys:
                            keys_value += (
                                f"{row.get(key.lower())}_"
                                if row.get(key.lower())
                                else ""
                            )
                        row.update(
                            {
                                "_id": f"{self.database}_{schema}_{table}_{keys_value}"
                                if schema
                                else f"{self.database}_{table}_{keys_value}",
                                "_timestamp": last_update_time or iso_utc(),
                                "Database": self.database,
                                "Table": table,
                            }
                        )
                        if schema:
                            row["schema"] = schema
                        yield self.serialize(doc=row)
                else:
                    self._logger.warning(
                        f"Skipping {table} table from database {self.database} since no primary key is associated with it. Assign primary key to the table to index it in the next sync interval."
                    )
            else:
                self._logger.warning(f"No rows found for {table}.")
        except (InternalClientError, ProgrammingError) as exception:
            self._logger.warning(
                f"Something went wrong while fetching document for table {table}. Error: {exception}"
            )

    async def fetch_rows(self, schema=None):
        """Fetches all the rows from all the tables of the database.

        Args:
            schema (str): Name of schema. Defaults to None.

        Yields:
            Dict: Row document to index
        """
        tables_to_fetch = await self.get_tables_to_fetch(schema)
        if self.database in self.tables_to_skip.keys():
            tables_to_fetch = set(tables_to_fetch) - set(
                self.tables_to_skip[self.database]
            )
        tables_to_fetch = list(tables_to_fetch)
        for table in tables_to_fetch:
            self._logger.debug(f"Found table: {table} in database: {self.database}.")
            async for row in self.fetch_documents(
                table=table,
                schema=schema,
            ):
                yield row
        if len(tables_to_fetch) < 1:
            if schema:
                self._logger.warning(
                    f"Fetched 0 tables for schema: {schema} and database: {self.database}"
                )
            else:
                self._logger.warning(
                    f"Fetched 0 tables for the database: {self.database}"
                )

    async def get_tables_to_fetch(self, schema):
        tables = configured_tables(self.tables)
        if self.queries is None:
            raise NotImplementedError
        return (
            map(
                lambda table: table[0],  # type: ignore
                await anext(
                    self.execute_query(
                        query=self.queries.all_tables(
                            user=self.user.upper(),
                            database=self.database,
                            schema=schema,
                        )
                    )
                ),
            )
            if is_wildcard(tables)
            else tables
        )
