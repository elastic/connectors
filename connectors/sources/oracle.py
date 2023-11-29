#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Oracle source module is responsible to fetch documents from Oracle."""
import asyncio
import os
from functools import cached_property, partial
from urllib.parse import quote

from asyncpg.exceptions._base import InternalClientError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

from connectors.source import BaseDataSource
from connectors.sources.generic_database import (
    DEFAULT_FETCH_SIZE,
    DEFAULT_RETRY_COUNT,
    Queries,
    configured_tables,
    fetch,
    is_wildcard,
    map_column_names,
)
from connectors.utils import iso_utc

DEFAULT_PROTOCOL = "TCP"
DEFAULT_ORACLE_HOME = ""


class OracleQueries(Queries):
    """Class contains methods which return query"""

    def ping(self):
        """Query to ping source"""
        return "SELECT 1+1 FROM DUAL"

    def all_tables(self, **kwargs):
        """Query to get all tables"""
        return (
            f"SELECT TABLE_NAME FROM all_tables where OWNER = UPPER('{kwargs['user']}')"
        )

    def table_primary_key(self, **kwargs):
        """Query to get the primary key"""
        return f"SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '{kwargs['table']}' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = UPPER('{kwargs['user']}') AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position"

    def table_data(self, **kwargs):
        """Query to get the table data"""
        return f"SELECT * FROM {kwargs['table']}"

    def table_last_update_time(self, **kwargs):
        """Query to get the last update time of the table"""
        return f"SELECT SCN_TO_TIMESTAMP(MAX(ora_rowscn)) from {kwargs['table']}"

    def table_data_count(self, **kwargs):
        """Query to get the number of rows in the table"""
        return f"SELECT COUNT(*) FROM {kwargs['table']}"

    def all_schemas(self):
        """Query to get all schemas of database"""
        pass  # Multiple schemas not supported in Oracle


class OracleClient:
    def __init__(
        self,
        host,
        port,
        user,
        password,
        database,
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
        self.database = database
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
        dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL={self.protocol})(HOST={self.host})(PORT={self.port}))(CONNECT_DATA=(SID={self.database})))"
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
        try:
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

    async def get_tables_to_fetch(self):
        tables = configured_tables(self.tables)
        if is_wildcard(tables):
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
        return primary_keys

    async def get_table_last_update_time(self, table):
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
            yield data


class OracleDataSource(BaseDataSource):
    """Oracle Database"""

    name = "Oracle Database"
    service_type = "oracle"

    def __init__(self, configuration):
        """Setup connection to the Oracle database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.database = self.configuration["database"]
        self.oracle_client = OracleClient(
            host=self.configuration["host"],
            port=self.configuration["port"],
            user=self.configuration["username"],
            password=self.configuration["password"],
            database=self.configuration["database"],
            tables=self.configuration["tables"],
            protocol=self.configuration["oracle_protocol"],
            oracle_home=self.configuration["oracle_home"],
            wallet_config=self.configuration["wallet_configuration_path"],
            retry_count=self.configuration["retry_count"],
            fetch_size=self.configuration["fetch_size"],
            logger_=self._logger,
        )

    def _set_internal_logger(self):
        self.oracle_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        return {
            "host": {
                "label": "Host",
                "order": 1,
                "type": "str",
            },
            "port": {
                "display": "numeric",
                "label": "Port",
                "order": 2,
                "type": "int",
            },
            "username": {
                "label": "Username",
                "order": 3,
                "type": "str",
            },
            "password": {
                "label": "Password",
                "order": 4,
                "sensitive": True,
                "type": "str",
            },
            "database": {
                "label": "Database",
                "order": 5,
                "type": "str",
            },
            "tables": {
                "display": "textarea",
                "label": "Comma-separated list of tables",
                "options": [],
                "order": 6,
                "type": "list",
            },
            "fetch_size": {
                "default_value": DEFAULT_FETCH_SIZE,
                "display": "numeric",
                "label": "Rows fetched per request",
                "order": 7,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "retry_count": {
                "default_value": DEFAULT_RETRY_COUNT,
                "display": "numeric",
                "label": "Retries per request",
                "order": 8,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "oracle_protocol": {
                "default_value": DEFAULT_PROTOCOL,
                "display": "dropdown",
                "label": "Oracle connection protocol",
                "options": [
                    {"label": "TCP", "value": "TCP"},
                    {"label": "TCPS", "value": "TCPS"},
                ],
                "order": 9,
                "type": "str",
                "value": DEFAULT_PROTOCOL,
                "ui_restrictions": ["advanced"],
            },
            "oracle_home": {
                "default_value": DEFAULT_ORACLE_HOME,
                "label": "Path to Oracle Home",
                "order": 10,
                "required": False,
                "type": "str",
                "value": DEFAULT_ORACLE_HOME,
                "ui_restrictions": ["advanced"],
            },
            "wallet_configuration_path": {
                "default_value": "",
                "label": "Path to SSL Wallet configuration files",
                "order": 11,
                "required": False,
                "type": "str",
                "ui_restrictions": ["advanced"],
            },
        }

    async def close(self):
        self.oracle_client.close()

    async def ping(self):
        """Verify the connection with the database-server configured by user"""
        self._logger.info("Validating the Connector Configuration...")
        try:
            await self.oracle_client.ping()
            self._logger.info("Successfully connected to Oracle.")
        except Exception as e:
            msg = f"Can't connect to Oracle on {self.oracle_client.host}"
            raise Exception(msg) from e

    async def fetch_documents(self, table):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            table (str): Name of table

        Yields:
            Dict: Document to be indexed
        """
        try:
            row_count = await self.oracle_client.get_table_row_count(table=table)
            if row_count > 0:
                # Query to get the table's primary key
                keys = await self.oracle_client.get_table_primary_key(table=table)
                keys = map_column_names(column_names=keys, tables=[table])
                if keys:
                    try:
                        last_update_time = (
                            await self.oracle_client.get_table_last_update_time(
                                table=table
                            )
                        )
                    except Exception:
                        self._logger.warning(
                            f"Unable to fetch last_updated_time for {table}"
                        )
                        last_update_time = None
                    streamer = self.oracle_client.data_streamer(table=table)
                    column_names = await anext(streamer)
                    column_names = map_column_names(
                        column_names=column_names, tables=[table]
                    )
                    async for row in streamer:
                        row = dict(zip(column_names, row, strict=True))
                        keys_value = ""
                        for key in keys:
                            keys_value += f"{row.get(key)}_" if row.get(key) else ""
                        row.update(
                            {
                                "_id": f"{self.database}_{table}_{keys_value}",
                                "_timestamp": last_update_time or iso_utc(),
                                "Database": self.database,
                                "Table": table,
                            }
                        )
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

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        table_count = 0
        async for table in self.oracle_client.get_tables_to_fetch():
            self._logger.debug(f"Found table: {table} in database: {self.database}.")
            table_count += 1
            async for row in self.fetch_documents(table=table):
                yield row, None
        if table_count < 1:
            self._logger.warning(f"Fetched 0 tables for the database: {self.database}")
