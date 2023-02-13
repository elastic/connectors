#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from functools import partial

from asyncpg.exceptions._base import InternalClientError
from sqlalchemy import text

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import iso_utc

DEFAULT_FETCH_SIZE = 50
DEFAULT_RETRY_COUNT = 3
DEFAULT_WAIT_MULTIPLIER = 2


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
        self.user = self.configuration["user"]
        self.password = self.configuration["password"]
        self.host = self.configuration["host"]
        self.port = self.configuration["port"]
        self.database = self.configuration["database"]
        self.is_async = False
        self.engine = None
        self.dialect = ""
        self.connection = None
        self.queries = None

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for database-server configured by user

        Returns:
            dictionary: Default configuration
        """
        return {
            "host": {
                "value": "127.0.0.1",
                "label": "Host",
                "type": "str",
            },
            "port": {
                "value": 9090,
                "label": "Port",
                "type": "int",
            },
            "user": {
                "value": "admin",
                "label": "Username",
                "type": "str",
            },
            "password": {
                "value": "Password_123",
                "label": "Password",
                "type": "str",
            },
            "database": {
                "value": "xe",
                "label": "Databases",
                "type": "str",
            },
            "fetch_size": {
                "value": DEFAULT_FETCH_SIZE,
                "label": "Rows fetched per request",
                "type": "int",
            },
            "retry_count": {
                "value": DEFAULT_RETRY_COUNT,
                "label": "Retries per request",
                "type": "int",
            },
        }

    def _validate_configuration(self):
        """Validates the configuration parameters

        Raises:
            Exception: Configured keys can't be empty
        """
        connection_fields = [
            "host",
            "port",
            "user",
            "password",
            "database",
        ]

        if empty_connection_fields := [
            field for field in connection_fields if self.configuration[field] == ""
        ]:
            raise Exception(
                f"Configured keys: {empty_connection_fields} can't be empty."
            )

        if (
            isinstance(self.configuration["port"], str)
            and not self.configuration["port"].isnumeric()
        ):
            raise Exception("Configured port has to be an integer.")

        if self.dialect == "Postgresql" and not (
            self.configuration["ssl_disabled"] or self.configuration["ssl_ca"]
        ):
            raise Exception("SSL certificate must be configured.")

    async def execute_query(self, query_name, fetch_many=False, **query_kwargs):
        """Executes a query and yield rows

        Args:
            query_name (str): Name of query.
            fetch_many (bool): Flag to use fetchmany method. Defaults to False.

        Raises:
            exception: Raise an exception after retrieving

        Yields:
            list: Column names and query response
        """
        query = self.queries[query_name].format(**query_kwargs)
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
                        if query_kwargs["schema"]:
                            yield [
                                f"{query_kwargs['schema']}_{query_kwargs['table']}_{column}".lower()
                                for column in cursor.keys()
                            ]
                        else:
                            yield [
                                f"{query_kwargs['table']}_{column}".lower()
                                for column in cursor.keys()
                            ]
                        yield_once = False

                    while True:
                        rows = cursor.fetchmany(size=size)
                        rows_length = len(rows)

                        if not rows_length:
                            break

                        for row in rows:
                            yield row

                        rows_fetched += rows_length
                        await asyncio.sleep(0)
                else:
                    yield cursor.fetchall()
                break
            except InternalClientError:
                raise
            except Exception as exception:
                logger.warning(
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
            async with self.engine.connect() as connection:
                cursor = await connection.execute(text(query))
                return cursor
        except Exception as exception:
            logger.warning(
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
            loop = asyncio.get_running_loop()
            self.connection = await loop.run_in_executor(
                executor=None, func=self.engine.connect
            )
            cursor = await loop.run_in_executor(
                executor=None,
                func=partial(self.connection.execute, statement=text(query)),
            )
            return cursor
        except Exception as exception:
            logger.warning(
                f"Something went wrong while executing query. Exception: {exception}"
            )
            raise

    def _create_engine(self):
        """Create engine based on dialects"""
        raise NotImplementedError

    async def ping(self):
        """Verify the connection with the database-server configured by user"""
        logger.info("Validating the Connector Configuration...")
        try:
            self._validate_configuration()
            self._create_engine()
            await anext(
                self.execute_query(
                    query_name="PING",
                )
            )
            logger.info(f"Successfully connected to {self.dialect}.")
        except Exception:
            raise Exception(f"Can't connect to {self.dialect} on {self.host}")

    async def fetch_documents(self, table, schema=None):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            table (str): Name of table
            schema (str): Name of schema. Defaults to None.

        Yields:
            Dict: Document to be indexed
        """
        try:
            [[row_count]] = await anext(
                self.execute_query(
                    query_name="TABLE_DATA_COUNT",
                    schema=schema,
                    table=table,
                )
            )
            if row_count > 0:
                # Query to get the table's primary key
                columns = await anext(
                    self.execute_query(
                        query_name="TABLE_PRIMARY_KEY",
                        user=self.user.upper(),
                        database=self.database,
                        schema=schema,
                        table=table,
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
                                query_name="TABLE_LAST_UPDATE_TIME",
                                database=self.database,
                                schema=schema,
                                table=table,
                            )
                        )
                        last_update_time = last_update_time[0][0]
                    except Exception:
                        logger.warning(f"Unable to fetch last_updated_time for {table}")
                        last_update_time = None
                    streamer = self.execute_query(
                        query_name="TABLE_DATA",
                        fetch_many=True,
                        schema=schema,
                        table=table,
                    )
                    column_names = await anext(streamer)
                    async for row in streamer:
                        row = dict(zip(column_names, row))
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
                    logger.warning(
                        f"Skipping {table} table from database {self.database} since no primary key is associated with it. Assign primary key to the table to index it in the next sync interval."
                    )
            else:
                logger.warning(f"No rows found for {table}.")
        except InternalClientError as exception:
            logger.warning(
                f"Something went wrong while fetching document for table {table}. Error: {exception}"
            )

    async def fetch_rows(self, schema=None):
        """Fetches all the rows from all the tables of the database.

        Args:
            schema (str): Name of schema. Defaults to None.

        Yields:
            Dict: Row document to index
        """
        # Query to get all table names from a database
        list_of_tables = await anext(
            self.execute_query(
                query_name="ALL_TABLE",
                user=self.user.upper(),
                database=self.database,
                schema=schema,
            )
        )
        if len(list_of_tables) > 0:
            for [table_name] in list_of_tables:
                logger.debug(f"Found table: {table_name} in database: {self.database}.")
                async for row in self.fetch_documents(
                    table=table_name,
                    schema=schema,
                ):
                    yield row
        else:
            if schema:
                logger.warning(
                    f"Fetched 0 tables for schema: {schema} and database: {self.database}"
                )
            else:
                logger.warning(f"Fetched 0 tables for the database: {self.database}")
