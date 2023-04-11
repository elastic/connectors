#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""MySQL source module responsible to fetch documents from MySQL"""
import re

import aiomysql

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.sources.generic_database import (
    WILDCARD,
    Queries,
    configured_tables,
    is_wildcard,
)
from connectors.utils import CancellableSleeps, RetryStrategy, retryable, ssl_context

SPLIT_BY_COMMA_OUTSIDE_BACKTICKS_PATTERN = re.compile(r"`(?:[^`]|``)+`|\w+")

MAX_POOL_SIZE = 10
DEFAULT_FETCH_SIZE = 50
RETRIES = 3
RETRY_INTERVAL = 2
DEFAULT_SSL_ENABLED = False
DEFAULT_SSL_CA = ""


def parse_tables_string_to_list_of_tables(tables_string):
    if tables_string is None or len(tables_string) == 0:
        return []

    return SPLIT_BY_COMMA_OUTSIDE_BACKTICKS_PATTERN.findall(tables_string)


def format_list(list_):
    return ", ".join(list_)


class MySQLQueries(Queries):
    def __init__(self, database):
        self.database = database

    def all_tables(self, **kwargs):
        return f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{self.database}'"

    def table_primary_key(self, table):
        return f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{self.database}' AND TABLE_NAME = '{table}' AND COLUMN_KEY = 'PRI'"

    def table_data(self, table):
        return f"SELECT * FROM {self.database}.{table}"

    def table_last_update_time(self, table):
        return f"SELECT UPDATE_TIME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{self.database}' AND TABLE_NAME = '{table}'"

    def columns(self, table):
        return f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{self.database}' AND TABLE_NAME = '{table}'"

    def ping(self):
        pass

    def table_data_count(self, **kwargs):
        pass

    def all_schemas(self):
        pass


class MySQLAdvancedRulesValidator(AdvancedRulesValidator):
    def __init__(self, source):
        self.source = source

    async def validate(self, advanced_rules):
        return await self._remote_validation(advanced_rules)

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _remote_validation(self, advanced_rules):
        await self.source.ping()

        async with self.source.mysql_client() as client:
            tables = set(await client.get_all_table_names())

        tables_to_filter = set(advanced_rules.keys())
        missing_tables = tables_to_filter - tables

        if len(missing_tables) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Tables not found or inaccessible: {format_list(sorted(list(missing_tables)))}.",
            )

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


class MySQLClient:
    def __init__(
        self,
        host,
        port,
        user,
        password,
        ssl_enabled,
        ssl_certificate,
        database=None,
        max_pool_size=MAX_POOL_SIZE,
        fetch_size=DEFAULT_FETCH_SIZE,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.max_pool_size = max_pool_size
        self.fetch_size = fetch_size
        self.ssl_enabled = ssl_enabled
        self.ssl_certificate = ssl_certificate
        self.queries = MySQLQueries(self.database)
        self.connection_pool = None
        self.connection = None

    async def __aenter__(self):
        connection_string = {
            "host": self.host,
            "port": int(self.port),
            "user": self.user,
            "password": self.password,
            "db": self.database,
            "maxsize": self.max_pool_size,
            "ssl": ssl_context(certificate=self.ssl_certificate)
            if self.ssl_enabled
            else None,
        }
        self.connection_pool = await aiomysql.create_pool(**connection_string)
        self.connection = await self.connection_pool.acquire()

        self._sleeps = CancellableSleeps()

        return self

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        self._sleeps.cancel()

        self.connection_pool.release(self.connection)
        self.connection_pool.close()
        await self.connection_pool.wait_closed()

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_all_table_names(self):
        async with self.connection.cursor(aiomysql.cursors.SSCursor) as cursor:
            await cursor.execute(self.queries.all_tables())
            return list(map(lambda table: table[0], await cursor.fetchall()))

    async def ping(self):
        try:
            await self.connection.ping()
            logger.info("Successfully connected to the MySQL Server.")
        except Exception:
            logger.exception("Error while connecting to the MySQL Server.")
            raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_column_names(self, table, query=None):
        if query is None:
            # fetch all columns on default
            query = self.queries.columns(table)

        async with self.connection.cursor(aiomysql.cursors.SSCursor) as cursor:
            await cursor.execute(query)

            return [f"{table}_{column[0]}" for column in await cursor.fetchall()]

    async def get_primary_key_column_names(self, table):
        return await self.get_column_names(
            table, query=self.queries.table_primary_key(table)
        )

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_last_update_time(self, table):
        async with self.connection.cursor(aiomysql.cursors.SSCursor) as cursor:
            await cursor.execute(self.queries.table_last_update_time(table))

            result = await cursor.fetchone()

            if result is not None:
                return result[0]

            return None

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def yield_rows_for_table(self, table):
        async for row in self._fetchmany_in_batches(self.queries.table_data(table)):
            yield row

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def yield_rows_for_query(self, query):
        async for row in self._fetchmany_in_batches(query):
            yield row

    async def _fetchmany_in_batches(self, query):
        async with self.connection.cursor(aiomysql.cursors.SSCursor) as cursor:
            await cursor.execute(query)

            fetched_rows = 0
            successful_batches = 0

            try:
                while True:
                    rows = await cursor.fetchmany(self.fetch_size)

                    if not rows:
                        break

                    for row in rows:
                        yield row

                    fetched_rows += len(rows)
                    successful_batches += 1

                    await self._sleeps.sleep(0)
            except IndexError as e:
                logger.exception(
                    f"Fetched {fetched_rows} rows in {successful_batches} batches. Encountered exception {e} in batch {successful_batches + 1}."
                )


class MySqlDataSource(BaseDataSource):
    """MySQL"""

    name = "MySQL"
    service_type = "mysql"

    def __init__(self, configuration):
        """Set up the connection to the MySQL server.

        Args:
            configuration (DataSourceConfiguration): Object  of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self._sleeps = CancellableSleeps()
        self.retry_count = self.configuration["retry_count"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]
        self.database = self.configuration["database"]
        self.tables = self.configuration["tables"]
        self.queries = MySQLQueries(self.database)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for MySQL server

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
                "value": 3306,
            },
            "user": {
                "label": "Username",
                "order": 3,
                "type": "str",
                "value": "root",
            },
            "password": {
                "label": "Password",
                "order": 4,
                "sensitive": True,
                "type": "str",
                "value": "changeme",
            },
            "database": {
                "label": "Database",
                "order": 5,
                "type": "str",
                "value": "customerinfo",
            },
            "tables": {
                "display": "textarea",
                "label": "Comma-separated list of tables",
                "order": 6,
                "type": "list",
                "value": WILDCARD,
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "Enable SSL verification",
                "order": 7,
                "type": "bool",
                "value": DEFAULT_SSL_ENABLED,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 8,
                "type": "str",
                "value": DEFAULT_SSL_CA,
            },
            "fetch_size": {
                "default_value": DEFAULT_FETCH_SIZE,
                "display": "numeric",
                "label": "Number of rows fetched per request",
                "order": 9,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": DEFAULT_FETCH_SIZE,
            },
            "retry_count": {
                "default_value": RETRIES,
                "display": "numeric",
                "label": "Maximum retries per request",
                "order": 10,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": RETRIES,
            },
        }

    def mysql_client(self):
        return MySQLClient(
            host=self.configuration["host"],
            port=self.configuration["port"],
            user=self.configuration["user"],
            password=self.configuration["password"],
            database=self.configuration["database"],
            fetch_size=self.configuration["fetch_size"],
            ssl_enabled=self.configuration["ssl_enabled"],
            ssl_certificate=self.configuration["ssl_ca"],
        )

    def advanced_rules_validators(self):
        return [MySQLAdvancedRulesValidator(self)]

    async def close(self):
        self._sleeps.cancel()

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields and validate type for port.
        Also validate, if the configured database and the configured tables are present and accessible by the configured user.

        Raises:
            Exception: Configured keys can't be empty
        """
        self.configuration.check_valid()
        await self._remote_validation()

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _remote_validation(self):
        async with self.mysql_client() as client:
            async with client.connection.cursor() as cursor:
                await self._validate_database_accessible(cursor)
                await self._validate_tables_accessible(cursor)

    async def _validate_database_accessible(self, cursor):
        try:
            await cursor.execute(f"USE {self.database};")
        except aiomysql.Error:
            raise ConfigurableFieldValueError(
                f"The database '{self.database}' is either not present or not accessible for the user '{self.configuration['user']}'."
            )

    async def _validate_tables_accessible(self, cursor):
        non_accessible_tables = []
        tables_to_validate = await self.get_tables_to_fetch()

        for table in tables_to_validate:
            try:
                await cursor.execute(f"SELECT 1 FROM {table} LIMIT 1;")
            except aiomysql.Error:
                non_accessible_tables.append(table)

        if len(non_accessible_tables) > 0:
            raise ConfigurableFieldValueError(
                f"The tables '{format_list(non_accessible_tables)}' are either not present or not accessible for user '{self.configuration['user']}'."
            )

    async def ping(self):
        async with self.mysql_client() as client:
            await client.ping()

    async def fetch_documents(self, table, query=None):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            table (str): Name of table
            query (str): Query to fetch data from a table

        Yields:
            Dict: Document to be indexed
        """

        async with self.mysql_client() as client:
            primary_key_columns = await client.get_primary_key_column_names(table)

            if not primary_key_columns:
                logger.warning(
                    f"Skipping {table} table from database {self.database} since no primary key is associated with it. Assign primary key to the table to index it in the next sync interval."
                )
                return

            last_update_time = await client.get_last_update_time(table)
            column_names = await client.get_column_names(table)
            row_generator = (
                client.yield_rows_for_table(table)
                if query is None
                else client.yield_rows_for_query(query)
            )

            async for row in row_generator:
                row = dict(zip(column_names, row))
                row.update(
                    {
                        "_id": self._generate_id(table, row, primary_key_columns),
                        "_timestamp": last_update_time,
                        "Table": table,
                    }
                )
                yield self.serialize(doc=row)

    def _generate_id(self, table, row, primary_key_columns):
        keys_value = ""
        for key in primary_key_columns:
            keys_value += f"{row.get(key)}_" if row.get(key) else ""

        return f"{table}_{keys_value}"

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()

            for table in advanced_rules:
                query = advanced_rules.get(table)
                logger.debug(
                    f"Fetching rows from table '{table}' in database '{self.database}' with a custom query."
                )
                async for row in self.fetch_documents(table, query):
                    yield row, None
                await self._sleeps.sleep(0)
        else:
            tables_to_fetch = await self.get_tables_to_fetch()

            for table in tables_to_fetch:
                async for row in self.fetch_documents(table):
                    yield row, None
                await self._sleeps.sleep(0)

    async def get_tables_to_fetch(self):
        tables = configured_tables(self.tables)

        async with self.mysql_client() as client:
            return await client.get_all_table_names() if is_wildcard(tables) else tables
