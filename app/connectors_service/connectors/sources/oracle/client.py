#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#


import asyncio
import os
from functools import cached_property, partial
from urllib.parse import quote

from sqlalchemy import create_engine, text

from connectors.sources.oracle.queries import OracleQueries
from connectors.sources.shared.database.generic_database import (
    DEFAULT_FETCH_SIZE,
    DEFAULT_RETRY_COUNT,
    configured_tables,
    fetch,
    is_wildcard,
)

DEFAULT_PROTOCOL = "TCP"
DEFAULT_ORACLE_HOME = ""
SID = "sid"
SERVICE_NAME = "service_name"


class OracleClient:
    def __init__(
        self,
        host,
        port,
        user,
        password,
        connection_source,
        sid,
        service_name,
        tables,
        protocol,
        oracle_home,
        wallet_config,
        logger_,
        retry_count=DEFAULT_RETRY_COUNT,
        fetch_size=DEFAULT_FETCH_SIZE,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection_source = connection_source
        self.sid = sid
        self.service_name = service_name
        self.tables = tables
        self.protocol = protocol
        self.oracle_home = oracle_home
        self.wallet_config = wallet_config
        self.retry_count = retry_count
        self.fetch_size = fetch_size

        self.connection = None
        self.queries = OracleQueries()
        self._logger = logger_

    def set_logger(self, logger_):
        self._logger = logger_

    def close(self):
        if self.connection is not None:
            self.connection.close()

    @cached_property
    def engine(self):
        """Create sync engine for oracle"""
        if self.connection_source == SID:
            dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL={self.protocol})(HOST={self.host})(PORT={self.port}))(CONNECT_DATA=(SID={self.sid})))"
        else:
            dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL={self.protocol})(HOST={self.host})(PORT={self.port}))(CONNECT_DATA=(service_name={self.service_name})))"
        connection_string = (
            f"oracle+oracledb://{self.user}:{quote(self.password)}@{dsn}"
        )
        if self.oracle_home != "":
            os.environ["ORACLE_HOME"] = self.oracle_home
            return create_engine(
                connection_string,
                thick_mode={
                    "lib_dir": f"{self.oracle_home}/lib",
                    "config_dir": self.wallet_config,
                },
            )
        else:
            return create_engine(connection_string)

    async def get_cursor(self, query):
        """Executes the passed query on the Non-Async supported Database server and return cursor.

        Args:
            query (str): Database query to be executed.

        Returns:
            cursor: Synchronous cursor
        """
        self._logger.debug(f"Retrieving the cursor for query '{query}'")
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
                f"Something went wrong while getting cursor; error: {exception}"
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

    async def get_tables_to_fetch(self):
        tables = configured_tables(self.tables)
        if is_wildcard(tables):
            self._logger.info(
                "Fetching all tables as the configuration field 'tables' is set to '*'"
            )
            async for row in fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.all_tables(
                        user=self.user,
                    ),
                ),
                fetch_size=self.fetch_size,
                retry_count=self.retry_count,
            ):
                yield row[0]
        else:
            self._logger.info(f"Fetching user-configured tables '{tables}'")
            for table in tables:
                yield table

    async def get_table_row_count(self, table):
        [row_count] = await anext(
            fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_data_count(
                        table=table,
                    ),
                ),
                fetch_size=1,
                retry_count=self.retry_count,
            )
        )
        return row_count

    async def get_table_primary_key(self, table):
        self._logger.debug(f"Extracting primary keys for table '{table}'")
        primary_keys = [
            key
            async for [key] in fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_primary_key(
                        user=self.user,
                        table=table,
                    ),
                ),
                fetch_size=self.fetch_size,
                retry_count=self.retry_count,
            )
        ]
        self._logger.debug(f"Found primary keys for table '{table}'")
        return primary_keys

    async def get_table_last_update_time(self, table):
        self._logger.debug(f"Fetching last updated time for table '{table}'")
        [last_update_time] = await anext(
            fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_last_update_time(
                        table=table,
                    ),
                ),
                fetch_size=1,
                retry_count=self.retry_count,
            )
        )
        return last_update_time

    async def data_streamer(self, table):
        """Streaming data from a table

        Args:
            table (str): Table.

        Raises:
            exception: Raise an exception after retrieving

        Yields:
            list: It will first yield the column names, then data in each row
        """
        self._logger.debug(f"Streaming records from database for table '{table}'")
        record_count = 0
        async for data in fetch(
            cursor_func=partial(
                self.get_cursor,
                self.queries.table_data(
                    table=table,
                ),
            ),
            fetch_columns=True,
            fetch_size=self.fetch_size,
            retry_count=self.retry_count,
        ):
            record_count += 1
            yield data
        self._logger.info(f"Found {record_count} records for table '{table}'")
