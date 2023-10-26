#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Postgresql source module is responsible to fetch documents from PostgreSQL."""
import ssl
from functools import cached_property, partial
from urllib.parse import quote

import fastjsonschema
from asyncpg.exceptions._base import InternalClientError
from fastjsonschema import JsonSchemaValueException
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import create_async_engine

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.source import BaseDataSource
from connectors.sources.generic_database import (
    DEFAULT_FETCH_SIZE,
    DEFAULT_RETRY_COUNT,
    DEFAULT_WAIT_MULTIPLIER,
    Queries,
    configured_tables,
    fetch,
    hash_id,
    is_wildcard,
    map_column_names,
)
from connectors.utils import (
    RetryStrategy,
    get_pem_format,
    has_duplicates,
    iso_utc,
    retryable,
)


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
        pass


class PostgreSQLAdvancedRulesValidator(AdvancedRulesValidator):
    QUERY_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "tables": {"type": "array", "minItems": 1},
            "query": {"type": "string", "minLength": 1},
        },
        "required": ["tables", "query"],
        "additionalProperties": False,
    }

    SCHEMA_DEFINITION = {"type": "array", "items": QUERY_OBJECT_SCHEMA_DEFINITION}

    SCHEMA = fastjsonschema.compile(definition=SCHEMA_DEFINITION)

    def __init__(self, source):
        self.source = source

    async def validate(self, advanced_rules):
        if len(advanced_rules) == 0:
            return SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            )

        return await self._remote_validation(advanced_rules)

    @retryable(
        retries=DEFAULT_RETRY_COUNT,
        interval=DEFAULT_WAIT_MULTIPLIER,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _remote_validation(self, advanced_rules):
        try:
            PostgreSQLAdvancedRulesValidator.SCHEMA(advanced_rules)
        except JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        tables_to_filter = {
            table
            for query_info in advanced_rules
            for table in query_info.get("tables", [])
        }
        tables = {
            table
            async for table in self.source.postgresql_client.get_tables_to_fetch(
                is_filtering=True
            )
        }

        missing_tables = tables_to_filter - tables

        if len(missing_tables) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Tables not found or inaccessible: {missing_tables}.",
            )

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


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

    async def data_streamer(self, table=None, query=None):
        """Streaming data from a table

        Args:
            table (str): Table.
            query (str): Query.

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
                )
                if query is None
                else query,
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
    advanced_rules_enabled = True

    def __init__(self, configuration):
        """Setup connection to the PostgreSQL database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.database = self.configuration["database"]
        self.schema = self.configuration["schema"]
        self.postgresql_client = PostgreSQLClient(
            host=self.configuration["host"],
            port=self.configuration["port"],
            user=self.configuration["username"],
            password=self.configuration["password"],
            database=self.configuration["database"],
            schema=self.configuration["schema"],
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
            "schema": {
                "label": "Schema",
                "order": 6,
                "type": "str",
            },
            "tables": {
                "display": "textarea",
                "label": "Comma-separated list of tables",
                "options": [],
                "order": 7,
                "tooltip": "This configurable field is ignored when Advanced Sync Rules are used.",
                "type": "list",
            },
            "fetch_size": {
                "default_value": DEFAULT_FETCH_SIZE,
                "display": "numeric",
                "label": "Rows fetched per request",
                "order": 8,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "retry_count": {
                "default_value": DEFAULT_RETRY_COUNT,
                "display": "numeric",
                "label": "Retries per request",
                "order": 9,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
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
            },
        }

    def advanced_rules_validators(self):
        return [PostgreSQLAdvancedRulesValidator(self)]

    async def ping(self):
        """Verify the connection with the database-server configured by user"""
        self._logger.info("Validating the Connector Configuration...")
        try:
            await self.postgresql_client.ping()
            self._logger.info("Successfully connected to Postgresql.")
        except Exception as e:
            msg = f"Can't connect to Postgresql on {self.postgresql_client.host}."
            raise Exception(msg) from e

    def row2doc(self, row, doc_id, table, timestamp):
        row.update(
            {
                "_id": doc_id,
                "_timestamp": timestamp,
                "database": self.database,
                "table": table,
                "schema": self.schema,
            }
        )
        return row

    async def get_primary_key(self, tables):
        primary_key_columns = []
        for table in tables:
            primary_key_columns.extend(
                await self.postgresql_client.get_table_primary_key(table)
            )
        primary_key_columns = sorted(primary_key_columns)
        return map_column_names(
            column_names=primary_key_columns, schema=self.schema, tables=tables
        )

    async def fetch_documents_from_table(self, table):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            table (str): Name of table

        Yields:
            Dict: Document to be indexed
        """
        try:
            docs_generator = self._yield_all_docs_from_tables(table=table)
            async for doc in docs_generator:
                yield doc
        except (InternalClientError, ProgrammingError) as exception:
            self._logger.warning(
                f"Something went wrong while fetching document for table {table}. Error: {exception}"
            )

    async def fetch_documents_from_query(self, tables, query):
        """Fetches all the data from the given query and format them in Elasticsearch documents

        Args:
            tables (str): List of tables
            query (str): Database Query

        Yields:
            Dict: Document to be indexed
        """
        try:
            docs_generator = self._yield_docs_custom_query(tables=tables, query=query)
            async for doc in docs_generator:
                yield doc
        except (InternalClientError, ProgrammingError) as exception:
            self._logger.warning(
                f"Something went wrong while fetching document for query {query} and tables {', '.join(tables)}. Error: {exception}"
            )

    async def _yield_docs_custom_query(self, tables, query):
        primary_key_columns = await self.get_primary_key(tables=tables)
        if not primary_key_columns:
            self._logger.warning(
                f"Skipping tables {', '.join(tables)} from database {self.database} since no primary key is associated with them. Assign primary key to the tables to index it in the next sync interval."
            )
            return

        if has_duplicates(primary_key_columns):
            self._logger.warning(
                f"Skipping custom query for tables {', '.join(tables)} as there are multiple tables with same primary key column name."
            )
            return

        last_update_times = list(
            filter(
                lambda update_time: update_time is not None,
                [
                    await self.postgresql_client.get_table_last_update_time(table)
                    for table in tables
                ],
            )
        )
        last_update_time = (
            max(last_update_times) if len(last_update_times) else iso_utc()
        )

        async for row in self.yield_rows_for_query(
            primary_key_columns=primary_key_columns, tables=tables, query=query
        ):
            doc_id = f"{self.database}_{self.schema}_{hash_id(list(tables), row, primary_key_columns)}"

            yield self.serialize(
                doc=self.row2doc(
                    row=row, doc_id=doc_id, table=tables, timestamp=last_update_time
                )
            )

    async def _yield_all_docs_from_tables(self, table):
        row_count = await self.postgresql_client.get_table_row_count(table=table)
        if row_count > 0:
            # Query to get the table's primary key
            keys = await self.get_primary_key(tables=[table])
            if keys:
                try:
                    last_update_time = (
                        await self.postgresql_client.get_table_last_update_time(
                            table=table
                        )
                    )
                except Exception:
                    self._logger.warning(
                        f"Unable to fetch last_updated_time for {table}"
                    )
                    last_update_time = None
                async for row in self.yield_rows_for_query(
                    primary_key_columns=keys, tables=[table]
                ):
                    doc_id = (
                        f"{self.database}_{self.schema}_{hash_id([table], row, keys)}"
                    )
                    yield self.serialize(
                        doc=self.row2doc(
                            row=row,
                            doc_id=doc_id,
                            table=table,
                            timestamp=last_update_time or iso_utc(),
                        )
                    )
            else:
                self._logger.warning(
                    f"Skipping {table} table from database {self.database} since no primary key is associated with it. Assign primary key to the table to index it in the next sync interval."
                )
        else:
            self._logger.warning(f"No rows found for {table}.")

    async def yield_rows_for_query(self, primary_key_columns, tables, query=None):
        if query is None:
            streamer = self.postgresql_client.data_streamer(table=tables[0])
        else:
            streamer = self.postgresql_client.data_streamer(query=query)
        column_names = await anext(streamer)
        column_names = map_column_names(
            column_names=column_names, schema=self.schema, tables=tables
        )

        if not set(primary_key_columns) - set(column_names):
            async for row in streamer:
                row = dict(zip(column_names, row, strict=True))
                yield row
        else:
            self._logger.warning(
                f"Skipping query {query} for tables {', '.join(tables)} as primary key column name is not present in query."
            )

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            for rule in advanced_rules:
                query = rule.get("query")
                tables = rule.get("tables")

                async for row in self.fetch_documents_from_query(
                    tables=tables, query=query
                ):
                    yield row, None

        else:
            table_count = 0

            async for table in self.postgresql_client.get_tables_to_fetch():
                self._logger.debug(
                    f"Found table: {table} in database: {self.database}."
                )
                table_count += 1
                async for row in self.fetch_documents_from_table(
                    table=table,
                ):
                    yield row, None

            if table_count < 1:
                self._logger.warning(
                    f"Fetched 0 tables for schema: {self.schema} and database: {self.database}"
                )
