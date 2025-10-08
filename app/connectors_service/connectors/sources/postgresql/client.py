#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import ssl
from functools import cached_property, partial
from urllib.parse import quote

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from connectors.sources.postgresql.queries import PostgreSQLQueries
from connectors.sources.shared.database.generic_database import (
    DEFAULT_FETCH_SIZE,
    DEFAULT_RETRY_COUNT,
    configured_tables,
    fetch,
    is_wildcard,
)
from connectors.utils import get_pem_format

FETCH_LIMIT = 1000


class PostgreSQLClient:
    def __init__(
        self,
        host,
        port,
        user,
        password,
        database,
        schema,
        tables,
        ssl_enabled,
        ssl_ca,
        logger_,
        retry_count=DEFAULT_RETRY_COUNT,
        fetch_size=DEFAULT_FETCH_SIZE,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.tables = tables
        self.retry_count = retry_count
        self.fetch_size = fetch_size
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.queries = PostgreSQLQueries()
        self.connection_pool = None
        self.connection = None
        self._logger = logger_

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def engine(self):
        connection_string = f"postgresql+asyncpg://{self.user}:{quote(self.password)}@{self.host}:{self.port}/{self.database}"
        return create_async_engine(
            connection_string,
            connect_args=self._get_connect_args(),
        )

    async def get_cursor(self, query):
        """Execute the passed query on the Async supported Database server and return cursor.

        Args:
            query (str): Database query to be executed.

        Returns:
            cursor: Asynchronous cursor
        """
        self._logger.debug(f"Retrieving the cursor for query '{query}'")
        try:
            async with self.engine.connect() as connection:  # pyright: ignore
                cursor = await connection.execute(text(query))
                return cursor
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while getting cursor. Exception: {exception}"
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
            self._logger.info("Fetching all tables")
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
            self._logger.info(f"Fetching user configured tables: {tables}")
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

        self._logger.debug(f"Found primary keys for table '{table}': {primary_keys}")

        return primary_keys

    async def get_table_last_update_time(self, table):
        self._logger.debug(f"Fetching last updated time for table '{table}'")
        [last_update_time] = await anext(
            fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_last_update_time(
                        schema=self.schema,
                        table=table,
                    ),
                ),
                fetch_size=1,
                retry_count=self.retry_count,
            )
        )
        self._logger.debug(f"Last updated time for table '{table}': {last_update_time}")
        return last_update_time

    async def data_streamer(
        self, table=None, query=None, row_count=None, order_by_columns=None
    ):
        """Streaming data from a table

        Args:
            table (str): Table.
            query (str): Query.

        Raises:
            exception: Raise an exception after retrieving

        Yields:
            list: It will first yield the column names, then data in each row
        """
        record_count = 0
        if query is None and row_count is not None and order_by_columns is not None:
            self._logger.debug(f"Streaming records from database for table '{table}'")
            order_by_columns_list = ",".join(
                [f'"{column}"' for column in order_by_columns]
            )
            offset = 0
            fetch_columns = True
            while True:
                async for data in fetch(
                    cursor_func=partial(
                        self.get_cursor,
                        self.queries.table_data(
                            schema=self.schema,
                            table=table,
                            columns=order_by_columns_list,
                            limit=FETCH_LIMIT,
                            offset=offset,
                        )
                        if query is None
                        else query,
                    ),
                    fetch_columns=fetch_columns,
                    fetch_size=self.fetch_size,
                    retry_count=self.retry_count,
                ):
                    record_count += 1
                    yield data
                fetch_columns = False
                offset += FETCH_LIMIT

                if row_count <= offset:
                    self._logger.info(
                        f"Found {record_count} records from table '{table}'"
                    )
                    return
        else:
            self._logger.debug(f"Streaming records from database using query: {query}")
            async for data in fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_data(
                        schema=self.schema,
                        table=table,
                    )
                    if query is None
                    else query,
                ),
                fetch_columns=True,
                fetch_size=self.fetch_size,
                retry_count=self.retry_count,
            ):
                record_count += 1
                yield data

            self._logger.info(f"Found {record_count} records for '{query}' query")

    def _get_connect_args(self):
        """Convert string to pem format and create an SSL context

        Returns:
            dictionary: Connection arguments
        """
        if not self.ssl_enabled:
            return {}

        pem_format = get_pem_format(key=self.ssl_ca)
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(cadata=pem_format)
        connect_args = {"ssl": ctx}
        return connect_args
