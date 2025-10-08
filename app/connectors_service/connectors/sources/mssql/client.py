#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#


import asyncio
import os
from functools import cached_property, partial
from tempfile import NamedTemporaryFile

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from connectors.sources.mssql.queries import MSSQLQueries
from connectors.sources.shared.database.generic_database import (
    DEFAULT_FETCH_SIZE,
    DEFAULT_RETRY_COUNT,
    configured_tables,
    fetch,
    is_wildcard,
)
from connectors.utils import get_pem_format


class MSSQLClient:
    def __init__(
        self,
        host,
        port,
        user,
        password,
        database,
        tables,
        schema,
        ssl_enabled,
        ssl_ca,
        validate_host,
        logger_,
        retry_count=DEFAULT_RETRY_COUNT,
        fetch_size=DEFAULT_FETCH_SIZE,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.tables = tables
        self.schema = schema
        self.retry_count = retry_count
        self.fetch_size = fetch_size
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.validate_host = validate_host

        self.connection = None
        self.certfile = ""
        self.queries = MSSQLQueries()
        self._logger = logger_

    def set_logger(self, logger_):
        self._logger = logger_

    def close(self):
        if os.path.exists(self.certfile):
            try:
                os.remove(self.certfile)
            except Exception as exception:
                self._logger.warning(
                    f"Something went wrong while removing temporary certificate file. Exception: {exception}"
                )
        if self.connection is not None:
            self.connection.close()

    @cached_property
    def engine(self):
        """Create sync engine for mssql"""
        connection_string = URL.create(
            "mssql+pytds",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )
        connect_args = {}
        if self.ssl_enabled:
            self.create_pem_file()
            connect_args = {
                "cafile": self.certfile,
                "validate_host": self.validate_host,
            }
        return create_engine(connection_string, connect_args=connect_args)

    def create_pem_file(self):
        """Create pem file for SSL Verification"""
        pem_certificates = get_pem_format(key=self.ssl_ca)
        with NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as cert:
            cert.write(pem_certificates)
            self.certfile = cert.name

    async def get_cursor(self, query):
        """Executes the passed query on the Non-Async supported Database server and return cursor.

        Args:
            query (str): Database query to be executed.

        Returns:
            cursor: Synchronous cursor
        """
        try:
            loop = asyncio.get_running_loop()
            if self.connection is None:
                self.connection = await loop.run_in_executor(
                    executor=None,
                    func=self.engine.connect,  # pyright: ignore
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

    async def ping(self):
        return await anext(
            fetch(
                cursor_func=partial(self.get_cursor, self.queries.ping()),
                fetch_size=1,
                retry_count=self.retry_count,
            )
        )

    async def get_tables_to_fetch(self, is_filtering=False):
        tables = configured_tables(self.tables)
        if is_wildcard(tables) or is_filtering:
            if is_filtering:
                self._logger.info("Syncing all tables: advanced sync rules are enabled")
            else:
                self._logger.info("Syncing all tables: 'tables' is set to '*'")

            async for row in fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.all_tables(
                        database=self.database,
                        schema=self.schema,
                    ),
                ),
                fetch_size=self.fetch_size,
                retry_count=self.retry_count,
            ):
                yield row[0]
        else:
            self._logger.info(f"Fetching configured tables: {tables}")
            for table in tables:
                yield table

    async def get_table_row_count(self, table):
        [row_count] = await anext(
            fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_data_count(
                        schema=self.schema,
                        table=table,
                    ),
                ),
                fetch_size=1,
                retry_count=self.retry_count,
            )
        )
        self._logger.info(f'Table "{table}" has {row_count} records')
        return row_count

    async def get_table_primary_key(self, table):
        primary_keys = [
            key
            async for [key] in fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_primary_key(
                        schema=self.schema,
                        table=table,
                    ),
                ),
                fetch_size=self.fetch_size,
                retry_count=self.retry_count,
            )
        ]

        self._logger.debug(f'Found primary keys for table "{table}": {primary_keys}')

        return primary_keys

    async def get_table_last_update_time(self, table):
        self._logger.debug(f"Fetching last updated time for table: {table}")
        async for [last_update_time] in fetch(
            cursor_func=partial(
                self.get_cursor,
                self.queries.table_last_update_time(schema=self.schema, table=table),
            ),
            fetch_size=1,
            retry_count=self.retry_count,
        ):
            return last_update_time

    async def data_streamer(self, table=None, query=None):
        """Streaming data from a table

        Args:
            table (str): Table.

        Raises:
            exception: Raise an exception after retrieving

        Yields:
            list: It will first yield the column names, then data in each row
        """
        record_count = 0
        if query is not None:
            cursor_query = query
            self._logger.debug(f"Streaming records from database using query: {query}")
        else:
            cursor_query = self.queries.table_data(
                schema=self.schema,
                table=table,
            )
            self._logger.debug(f'Streaming records from database for table "{table}"')

        async for data in fetch(
            cursor_func=partial(
                self.get_cursor,
                cursor_query,
            ),
            fetch_columns=True,
            fetch_size=self.fetch_size,
            retry_count=self.retry_count,
        ):
            record_count += 1
            yield data
