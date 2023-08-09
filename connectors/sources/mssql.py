#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft SQL source module is responsible to fetch documents from Microsoft SQL."""
import asyncio
import os
from functools import cached_property, partial
from tempfile import NamedTemporaryFile

from asyncpg.exceptions._base import InternalClientError
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import ProgrammingError

from connectors.source import BaseDataSource
from connectors.sources.generic_database import (
    DEFAULT_FETCH_SIZE,
    DEFAULT_RETRY_COUNT,
    WILDCARD,
    Queries,
    configured_tables,
    fetch,
    is_wildcard,
    map_column_names,
)
from connectors.utils import get_pem_format, iso_utc

# Connector will skip the below tables if it gets from the input
TABLES_TO_SKIP = {"msdb": ["sysutility_ucp_configuration_internal"]}


class MSSQLQueries(Queries):
    """Class contains methods which return query"""

    def ping(self):
        """Query to ping source"""
        return "SELECT 1+1"

    def all_tables(self, **kwargs):
        """Query to get all tables"""
        return f"SELECT table_name FROM information_schema.tables WHERE TABLE_SCHEMA = '{ kwargs['schema'] }'"

    def table_primary_key(self, **kwargs):
        """Query to get the primary key"""
        return f"SELECT C.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME WHERE C.TABLE_NAME='{kwargs['table']}' and C.TABLE_SCHEMA='{kwargs['schema']}' and T.CONSTRAINT_TYPE='PRIMARY KEY'"

    def table_data(self, **kwargs):
        """Query to get the table data"""
        return f'SELECT * FROM {kwargs["schema"]}."{kwargs["table"]}"'

    def table_last_update_time(self, **kwargs):
        """Query to get the last update time of the table"""
        return f"SELECT last_user_update FROM sys.dm_db_index_usage_stats WHERE object_id=object_id('{kwargs['schema']}.{kwargs['table']}')"

    def table_data_count(self, **kwargs):
        """Query to get the number of rows in the table"""
        return f'SELECT COUNT(*) FROM {kwargs["schema"]}."{kwargs["table"]}"'

    def all_schemas(self):
        """Query to get all schemas of database"""
        pass


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
                        database=self.database,
                        schema=self.schema,
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
        return primary_keys

    async def get_table_last_update_time(self, table):
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
                    schema=self.schema,
                    table=table,
                ),
            ),
            fetch_columns=True,
            fetch_size=self.fetch_size,
            retry_count=self.retry_count,
        ):
            yield data


class MSSQLDataSource(BaseDataSource):
    """Microsoft SQL Server"""

    name = "Microsoft SQL Server"
    service_type = "mssql"

    def __init__(self, configuration):
        """Setup connection to the Microsoft SQL database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.database = self.configuration["database"]
        self.schema = self.configuration["schema"]
        self.mssql_client = MSSQLClient(
            host=self.configuration["host"],
            port=self.configuration["port"],
            user=self.configuration["username"],
            password=self.configuration["password"],
            database=self.configuration["database"],
            tables=self.configuration["tables"],
            schema=self.configuration["schema"],
            ssl_enabled=self.configuration["ssl_enabled"],
            ssl_ca=self.configuration["ssl_ca"],
            validate_host=self.configuration["validate_host"],
            retry_count=self.configuration["retry_count"],
            fetch_size=self.configuration["fetch_size"],
            logger_=self._logger,
        )

    def _set_internal_logger(self):
        self.mssql_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
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
            "schema": {
                "label": "Schema",
                "order": 9,
                "type": "str",
                "value": "dbo",
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "Enable SSL verification",
                "order": 10,
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 11,
                "type": "str",
                "value": "",
            },
            "validate_host": {
                "display": "toggle",
                "label": "Validate host",
                "order": 12,
                "type": "bool",
                "value": False,
            },
        }

    async def close(self):
        self.mssql_client.close()

    async def ping(self):
        """Verify the connection with the database-server configured by user"""
        self._logger.info("Validating the Connector Configuration...")
        try:
            await self.mssql_client.ping()
            self._logger.info("Successfully connected to Microsoft SQL.")
        except Exception as e:
            raise Exception(
                f"Can't connect to Microsoft SQL on {self.mssql_client.host}"
            ) from e

    async def fetch_documents(self, table):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            table (str): Name of table

        Yields:
            Dict: Document to be indexed
        """
        try:
            row_count = await self.mssql_client.get_table_row_count(table=table)
            if row_count > 0:
                # Query to get the table's primary key
                keys = await self.mssql_client.get_table_primary_key(table=table)
                keys = map_column_names(
                    column_names=keys, schema=self.schema, table=table
                )
                if keys:
                    try:
                        last_update_time = (
                            await self.mssql_client.get_table_last_update_time(
                                table=table
                            )
                        )
                    except Exception:
                        self._logger.warning(
                            f"Unable to fetch last_updated_time for {table}"
                        )
                        last_update_time = None
                    streamer = self.mssql_client.data_streamer(table=table)
                    column_names = await anext(streamer)
                    column_names = map_column_names(
                        column_names=column_names, schema=self.schema, table=table
                    )
                    async for row in streamer:
                        row = dict(zip(column_names, row, strict=True))
                        keys_value = ""
                        for key in keys:
                            keys_value += f"{row.get(key)}_" if row.get(key) else ""
                        row.update(
                            {
                                "_id": f"{self.database}_{self.schema}_{table}_{keys_value}",
                                "_timestamp": last_update_time or iso_utc(),
                                "Database": self.database,
                                "Table": table,
                                "schema": self.schema,
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

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        table_count = 0
        async for table in self.mssql_client.get_tables_to_fetch():
            if (
                self.database in TABLES_TO_SKIP.keys()
                and table in TABLES_TO_SKIP[self.database]
            ):
                self._logger.debug(f"Skip table: {table} in database: {self.database}")

            self._logger.debug(f"Found table: {table} in database: {self.database}.")
            table_count += 1
            async for row in self.fetch_documents(table=table):
                yield row, None
        if table_count < 1:
            self._logger.warning(
                f"Fetched 0 tables for schema: {self.schema} and database: {self.database}"
            )
