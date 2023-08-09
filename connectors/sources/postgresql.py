#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Postgresql source module is responsible to fetch documents from PostgreSQL."""
import ssl
from functools import cached_property, partial
from urllib.parse import quote

from asyncpg.exceptions._base import InternalClientError
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import create_async_engine

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

# Below schemas are system schemas and the tables of the systems schema's will not get indexed
SYSTEM_SCHEMA = ["pg_toast", "pg_catalog", "information_schema"]
DEFAULT_SSL_ENABLED = False
DEFAULT_SSL_CA = ""


class PostgreSQLQueries(Queries):
    """Class contains methods which return query"""

    def ping(self):
        """Query to ping source"""
        return "SELECT 1+1"

    def all_tables(self, **kwargs):
        """Query to get all tables"""
        return f"SELECT table_name FROM information_schema.tables WHERE table_catalog = '{kwargs['database']}' and table_schema = '{kwargs['schema']}'"

    def table_primary_key(self, **kwargs):
        """Query to get the primary key"""
        return f"SELECT c.column_name FROM information_schema.table_constraints tc JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{kwargs['table']}' and tc.constraint_schema = '{kwargs['schema']}'"

    def table_data(self, **kwargs):
        """Query to get the table data"""
        return f'SELECT * FROM {kwargs["schema"]}."{kwargs["table"]}"'

    def table_last_update_time(self, **kwargs):
        """Query to get the last update time of the table"""
        return f'SELECT MAX(pg_xact_commit_timestamp(xmin)) FROM {kwargs["schema"]}."{kwargs["table"]}"'

    def table_data_count(self, **kwargs):
        """Query to get the number of rows in the table"""
        return f'SELECT COUNT(*) FROM {kwargs["schema"]}."{kwargs["table"]}"'

    def all_schemas(self):
        """Query to get all schemas of database"""
        return "SELECT schema_name FROM information_schema.schemata"


class PostgreSQLClient:
    def __init__(
        self,
        host,
        port,
        user,
        password,
        database,
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

    async def get_all_schemas(self):
        async for row in fetch(
            cursor_func=partial(self.get_cursor, self.queries.all_schemas()),
            fetch_size=self.fetch_size,
            retry_count=self.retry_count,
        ):
            yield row[0]

    async def get_tables_to_fetch(self, schema):
        tables = configured_tables(self.tables)
        if is_wildcard(tables):
            async for row in fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.all_tables(
                        database=self.database,
                        schema=schema,
                    ),
                ),
                fetch_size=self.fetch_size,
                retry_count=self.retry_count,
            ):
                yield row[0]
        else:
            for table in tables:
                yield table

    async def get_table_row_count(self, schema, table):
        [row_count] = await anext(
            fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_data_count(
                        schema=schema,
                        table=table,
                    ),
                ),
                fetch_size=1,
                retry_count=self.retry_count,
            )
        )
        return row_count

    async def get_table_primary_key(self, schema, table):
        primary_keys = [
            key
            async for [key] in fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_primary_key(
                        schema=schema,
                        table=table,
                    ),
                ),
                fetch_size=self.fetch_size,
                retry_count=self.retry_count,
            )
        ]

        return primary_keys

    async def get_table_last_update_time(self, schema, table):
        [last_update_time] = await anext(
            fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_last_update_time(
                        schema=schema,
                        table=table,
                    ),
                ),
                fetch_size=1,
                retry_count=self.retry_count,
            )
        )
        return last_update_time

    async def data_streamer(self, schema, table):
        """Streaming data from a table

        Args:
            schema (str): Schema.
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
                    schema=schema,
                    table=table,
                ),
            ),
            fetch_columns=True,
            fetch_size=self.fetch_size,
            retry_count=self.retry_count,
        ):
            yield data

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


class PostgreSQLDataSource(BaseDataSource):
    """PostgreSQL"""

    name = "PostgreSQL"
    service_type = "postgresql"

    def __init__(self, configuration):
        """Setup connection to the PostgreSQL database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.database = self.configuration["database"]
        self.postgresql_client = PostgreSQLClient(
            host=self.configuration["host"],
            port=self.configuration["port"],
            user=self.configuration["username"],
            password=self.configuration["password"],
            database=self.configuration["database"],
            tables=self.configuration["tables"],
            ssl_enabled=self.configuration["ssl_enabled"],
            ssl_ca=self.configuration["ssl_ca"],
            retry_count=self.configuration["retry_count"],
            fetch_size=self.configuration["fetch_size"],
            logger_=self._logger,
        )

    def _set_internal_logger(self):
        self.postgresql_client.set_logger(self._logger)

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
            "ssl_enabled": {
                "display": "toggle",
                "label": "Enable SSL verification",
                "order": 9,
                "type": "bool",
                "value": DEFAULT_SSL_ENABLED,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 10,
                "type": "str",
                "value": DEFAULT_SSL_CA,
            },
        }

    async def ping(self):
        """Verify the connection with the database-server configured by user"""
        self._logger.info("Validating the Connector Configuration...")
        try:
            await self.postgresql_client.ping()
            self._logger.info("Successfully connected to Postgresql.")
        except Exception as e:
            raise Exception(
                f"Can't connect to Postgresql on {self.postgresql_client.host}."
            ) from e

    async def fetch_documents(self, table, schema):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            table (str): Name of table
            schema (str): Name of schema

        Yields:
            Dict: Document to be indexed
        """
        try:
            row_count = await self.postgresql_client.get_table_row_count(
                schema=schema, table=table
            )
            if row_count > 0:
                # Query to get the table's primary key
                keys = await self.postgresql_client.get_table_primary_key(
                    schema=schema, table=table
                )
                keys = map_column_names(column_names=keys, schema=schema, table=table)
                if keys:
                    try:
                        last_update_time = (
                            await self.postgresql_client.get_table_last_update_time(
                                schema=schema, table=table
                            )
                        )
                    except Exception as e:
                        self._logger.warning(
                            f"Unable to fetch last_updated_time for {table}"
                        )
                        last_update_time = None
                    streamer = self.postgresql_client.data_streamer(
                        schema=schema, table=table
                    )
                    column_names = await anext(streamer)
                    column_names = map_column_names(
                        column_names=column_names, schema=schema, table=table
                    )
                    async for row in streamer:
                        row = dict(zip(column_names, row, strict=True))
                        keys_value = ""
                        for key in keys:
                            keys_value += f"{row.get(key)}_" if row.get(key) else ""
                        row.update(
                            {
                                "_id": f"{self.database}_{schema}_{table}_{keys_value}",
                                "_timestamp": last_update_time or iso_utc(),
                                "Database": self.database,
                                "Table": table,
                                "schema": schema,
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
        async for schema in self.postgresql_client.get_all_schemas():
            if schema not in SYSTEM_SCHEMA:
                table_count = 0
                async for table in self.postgresql_client.get_tables_to_fetch(schema):
                    self._logger.debug(
                        f"Found table: {table} in database: {self.database}."
                    )
                    table_count += 1
                    async for row in self.fetch_documents(
                        table=table,
                        schema=schema,
                    ):
                        yield row, None
                if table_count < 1:
                    self._logger.warning(
                        f"Fetched 0 tables for schema: {schema} and database: {self.database}"
                    )
