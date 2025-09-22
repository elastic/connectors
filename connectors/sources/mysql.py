#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""MySQL source module responsible to fetch documents from MySQL"""

import re

import aiomysql
import fastjsonschema
from fastjsonschema import JsonSchemaValueException

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.source import DataSourceConfiguration, BaseDataSource, ConfigurableFieldValueError
from connectors.sources.generic_database import (
    configured_tables,
    is_wildcard,
)
from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    iso_utc,
    retryable,
    ssl_context,
)
from connectors.logger import ExtraLogger
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from unittest.mock import AsyncMock, MagicMock

SPLIT_BY_COMMA_OUTSIDE_BACKTICKS_PATTERN = re.compile(r"`(?:[^`]|``)+`|\w+")

MAX_POOL_SIZE = 10
DEFAULT_FETCH_SIZE = 5000
RETRIES = 3
RETRY_INTERVAL = 2


def format_list(list_: Union[str, List[str]]) -> str:
    return ", ".join(list_)


class MySQLAdvancedRulesValidator(AdvancedRulesValidator):
    QUERY_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "tables": {"type": "array", "minItems": 1},
            "query": {"type": "string", "minLength": 1},
            "id_columns": {"type": "array", "minItems": 1},
        },
        "required": ["tables", "query"],
        "additionalProperties": False,
    }

    SCHEMA_DEFINITION = {"type": "array", "items": QUERY_OBJECT_SCHEMA_DEFINITION}

    SCHEMA = fastjsonschema.compile(definition=SCHEMA_DEFINITION)

    def __init__(self, source: "MySqlDataSource") -> None:
        self.source = source

    async def validate(self, advanced_rules: Any) -> SyncRuleValidationResult:
        if len(advanced_rules) == 0:
            return SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            )

        return await self._remote_validation(advanced_rules)

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _remote_validation(self, advanced_rules: List[Union[Dict[str, Union[List[str], str, bool]], Dict[str, str], str, Dict[str, List[str]], Dict[str, Union[str, List[str]]]]]) -> SyncRuleValidationResult:
        try:
            MySQLAdvancedRulesValidator.SCHEMA(advanced_rules)
        except JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        async with self.source.mysql_client() as client:
            tables = set(await client.get_all_table_names())

        tables_to_filter = {
            table
            for query_info in advanced_rules
            for table in query_info.get("tables", [])
        }
        missing_tables = tables_to_filter - tables

        if len(missing_tables) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Tables not found or inaccessible: {format_list(sorted(missing_tables))}.",
            )

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


class MySQLClient:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        ssl_enabled: bool,
        ssl_certificate: str,
        logger_: ExtraLogger,
        database: None=None,
        max_pool_size: int=MAX_POOL_SIZE,
        fetch_size: int=DEFAULT_FETCH_SIZE,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.max_pool_size = max_pool_size
        self.fetch_size = fetch_size
        self.ssl_enabled = ssl_enabled
        self.ssl_certificate = ssl_certificate
        self.connection_pool = None
        self.connection = None
        self._logger = logger_

    async def __aenter__(self) -> "MySQLClient":
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

    async def __aexit__(self, exception_type: None, exception_value: None, exception_traceback: None) -> None:
        self._sleeps.cancel()

        self.connection_pool.release(self.connection)
        self.connection_pool.close()
        await self.connection_pool.wait_closed()

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_all_table_names(self) -> List[str]:
        async with self.connection.cursor(aiomysql.cursors.SSCursor) as cursor:
            await cursor.execute(
                f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{self.database}'"
            )
            return [table[0] for table in await cursor.fetchall()]

    async def ping(self) -> None:
        try:
            await self.connection.ping()
            self._logger.info("Successfully connected to the MySQL Server.")
        except Exception:
            self._logger.exception("Error while connecting to the MySQL Server.")
            raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_column_names_for_query(self, query: str) -> List[Union[Any, str]]:
        async with self.connection.cursor(aiomysql.cursors.SSCursor) as cursor:
            await cursor.execute(f"SELECT q.* FROM ({query}) as q LIMIT 0")

            return [f"{column[0]}" for column in cursor.description]

    async def get_column_names_for_table(self, table: str) -> List[Union[Any, str]]:
        return await self.get_column_names_for_query(
            f"SELECT * FROM `{self.database}`.`{table}`"
        )

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_primary_key_column_names(self, table: str) -> List[Union[Any, str]]:
        async with self.connection.cursor(aiomysql.cursors.SSCursor) as cursor:
            await cursor.execute(
                f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{self.database}' AND TABLE_NAME = '{table}' AND COLUMN_KEY = 'PRI'"
            )

            return [f"{column[0]}" for column in await cursor.fetchall()]

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_last_update_time(self, table: str) -> str:
        async with self.connection.cursor(aiomysql.cursors.SSCursor) as cursor:
            await cursor.execute(
                f"SELECT UPDATE_TIME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{self.database}' AND TABLE_NAME = '{table}'"
            )

            result = await cursor.fetchone()

            if result is not None:
                return result[0]

            return None

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def yield_rows_for_table(self, table, primary_keys, table_row_count):
        offset = 0
        while offset < table_row_count:
            async for row in self._fetchmany_in_batches(
                f"SELECT * FROM `{self.database}`.`{table}` ORDER BY '{', '.join(primary_keys)}' LIMIT {self.fetch_size} OFFSET {offset}"
            ):
                if row:
                    yield row
                else:
                    break
            offset += self.fetch_size

    async def _get_table_row_count_for_query(self, query: str) -> int:
        table_row_count_query = re.sub(
            r"SELECT\s.*?\sFROM",
            "SELECT COUNT(*) FROM",
            query,
            flags=re.IGNORECASE | re.DOTALL,
        )
        async with self.connection.cursor(aiomysql.cursors.SSCursor) as cursor:
            await cursor.execute(table_row_count_query)
            table_row_count = await cursor.fetchone()
            return int(table_row_count[0])

    def _update_query_with_pagination_attributes(
        self, query: str, offset: int, primary_key_columns: List[str]
    ) -> str:
        updated_query = ""
        has_orderby = bool(re.search(r"\bORDER\s+BY\b", query, flags=re.IGNORECASE))
        # Checking if custom query has a semicolon at the end or not
        if query.endswith(";"):
            query = query[:-1]
        if has_orderby:
            updated_query = f"{query} LIMIT {self.fetch_size} OFFSET {offset};"
        else:
            updated_query = f"{query} ORDER BY {', '.join(primary_key_columns)} LIMIT {self.fetch_size} OFFSET {offset};"

        return updated_query

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def yield_rows_for_query(self, query, primary_key_columns):
        table_row_count_for_query = await self._get_table_row_count_for_query(
            query=query
        )
        offset = 0
        while offset < table_row_count_for_query:
            async for row in self._fetchmany_in_batches(
                query=self._update_query_with_pagination_attributes(
                    query=query, offset=offset, primary_key_columns=primary_key_columns
                )
            ):
                if row:
                    yield row
                else:
                    break
            offset += self.fetch_size

    async def _fetchmany_in_batches(self, query):
        async with self.connection.cursor(aiomysql.cursors.SSCursor) as cursor:
            await cursor.execute(query)

            fetched_rows = 0
            successful_batches = 0

            try:
                rows = await cursor.fetchall()

                for row in rows:
                    yield row

                fetched_rows += len(rows)
                successful_batches += 1

                await self._sleeps.sleep(0)
            except IndexError as e:
                self._logger.exception(
                    f"Fetched {fetched_rows} rows in {successful_batches} batches. Encountered exception {e} in batch {successful_batches + 1}."
                )


def row2doc(row: Union[Tuple[int, str, int], List[str]], column_names: List[str], primary_key_columns: Union[str, List[str]], table: Union[str, List[str]], timestamp: Optional[Union[str, datetime]]) -> Dict[str, Union[str, int, datetime, List[str]]]:
    row = dict(zip(column_names, row, strict=True))
    row.update(
        {
            "_id": generate_id(table, row, primary_key_columns),
            "_timestamp": timestamp or iso_utc(),
            "Table": table,
        }
    )

    return row


def generate_id(tables: Union[str, List[str]], row: Dict[str, Union[str, int]], primary_key_columns: Union[str, List[str]]) -> str:
    """Generates an id using table names as prefix in sorted order and primary key values.

    Example:
        tables: table1, table2
        primary key values: 1, 42
        table1_table2_1_42
    """

    if not isinstance(tables, list):
        tables = [tables]

    return (
        f"{'_'.join(sorted(tables))}_"
        f"{'_'.join([str(pk_value) for pk in primary_key_columns if (pk_value := row.get(pk)) is not None])}"
    )


class MySqlDataSource(BaseDataSource):
    """MySQL"""

    name = "MySQL"
    service_type = "mysql"
    advanced_rules_enabled = True

    def __init__(self, configuration: DataSourceConfiguration) -> None:
        super().__init__(configuration=configuration)
        self._sleeps = CancellableSleeps()
        self.retry_count = self.configuration["retry_count"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]
        self.database = self.configuration["database"]
        self.tables = self.configuration["tables"]

    @classmethod
    def get_default_configuration(cls) -> Dict[str, Dict[str, Union[str, int, bool, List[Dict[str, Union[str, bool]]], List[str]]]]:
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
            "user": {
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
                "order": 6,
                "type": "list",
                "value": "*",
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "Enable SSL",
                "order": 7,
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 8,
                "type": "str",
            },
            "fetch_size": {
                "default_value": DEFAULT_FETCH_SIZE,
                "display": "numeric",
                "label": "Rows fetched per request",
                "order": 9,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "retry_count": {
                "default_value": RETRIES,
                "display": "numeric",
                "label": "Retries per request",
                "order": 10,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
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
            logger_=self._logger,
        )

    def advanced_rules_validators(self):
        return [MySQLAdvancedRulesValidator(self)]

    async def close(self) -> None:
        self._sleeps.cancel()

    async def validate_config(self):
        """Validates that user input is not empty and adheres to the specified constraints.
        Also validate, if the configured database and the configured tables are present and accessible using the configured user.

        Raises:
            ConfigurableFieldValueError: The database or the tables do not exist or aren't accessible, or a field contains an empty or wrong value
            ConfigurableFieldDependencyError: A inter-field dependency is not met
        """
        await super().validate_config()
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

    async def _validate_database_accessible(self, cursor: AsyncMock) -> None:
        try:
            await cursor.execute(f"USE `{self.database}`;")
        except aiomysql.Error as e:
            msg = f"The database '{self.database}' is either not present or not accessible for the user '{self.configuration['user']}'."
            raise ConfigurableFieldValueError(msg) from e

    async def _validate_tables_accessible(self, cursor: AsyncMock) -> None:
        non_accessible_tables = []
        tables_to_validate = await self.get_tables_to_fetch()

        for table in tables_to_validate:
            try:
                await cursor.execute(f"SELECT 1 FROM `{table}` LIMIT 1;")
            except aiomysql.Error:
                non_accessible_tables.append(table)

        if len(non_accessible_tables) > 0:
            msg = f"The tables '{format_list(non_accessible_tables)}' are either not present or not accessible for user '{self.configuration['user']}'."
            raise ConfigurableFieldValueError(msg)

    async def ping(self):
        async with self.mysql_client() as client:
            await client.ping()

    async def get_docs(self, filtering=None):
        """Fetch documents by either fetching all documents from the configured tables or using custom queries provided in the advanced rules.

        Yields:
            Dict: Document representing a row and its metadata
        """
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()

            for query_info in advanced_rules:
                tables = query_info.get("tables", [])
                query = query_info.get("query", "")
                id_columns = query_info.get("id_columns", [])
                self._logger.debug(
                    f"Fetching rows from table '{format_list(tables)}' in database '{self.database}' with a custom query and given ID column '{id_columns}'."
                )

                async for row in self.fetch_documents(tables, query, id_columns):
                    yield row, None

                await self._sleeps.sleep(0)
        else:
            tables = await self.get_tables_to_fetch()

            async for row in self.fetch_documents(tables):
                yield row, None

    async def fetch_documents(self, tables: List[str], query: Optional[str]=None, id_columns: None=None) -> None:
        """If query is not present it fetches all rows from all tables.
        Otherwise, the custom query is executed.

        Args:
            tables (str): Name of tables
            query (str): Custom query

        Yields:
            Dict: Document representing a row and its metadata
        """
        if not isinstance(tables, list):
            tables = [tables]

        async with self.mysql_client() as client:
            docs_generator = (
                self._yield_docs_custom_query(client, tables, query, id_columns)
                if query is not None
                else self._yield_all_docs_from_tables(client, tables)
            )

            async for doc in docs_generator:
                yield self.serialize(doc=doc)

    async def _yield_all_docs_from_tables(self, client, tables):
        for table in tables:
            primary_key_columns = await client.get_primary_key_column_names(table)

            if not primary_key_columns:
                self._logger.warning(
                    f"Skipping table {table} from database {self.database} since no primary key is associated with it. Assign primary key to the table to index it in the next sync interval."
                )
                continue

            last_update_time = await client.get_last_update_time(table)
            column_names = await client.get_column_names_for_table(table)

            async with client.connection.cursor(aiomysql.cursors.SSCursor) as cursor:
                await cursor.execute(
                    f"SELECT COUNT(*) FROM `{self.database}`.`{table}`"
                )
                table_row_count = await cursor.fetchone()

            async for row in client.yield_rows_for_table(
                table, primary_key_columns, int(table_row_count[0])
            ):
                yield row2doc(
                    row=row,
                    column_names=column_names,
                    primary_key_columns=primary_key_columns,
                    table=table,
                    timestamp=last_update_time,
                )

    async def _yield_docs_custom_query(self, client: Union[MySQLClient, MagicMock], tables: Union[str, List[str]], query: str, id_columns: None) -> None:
        primary_key_columns = [
            await client.get_primary_key_column_names(table) for table in tables
        ]
        primary_key_columns = sorted(
            [column for columns in primary_key_columns for column in columns]
        )

        if id_columns:
            primary_key_columns = id_columns

        if not primary_key_columns:
            self._logger.warning(
                f"Skipping tables {format_list(tables)} from database {self.database} since no primary key is associated with them. Assign primary key to the tables to index it in the next sync interval."
            )
            return

        last_update_times = list(
            filter(
                lambda update_time: update_time is not None,
                [await client.get_last_update_time(table) for table in tables],
            )
        )
        column_names = await client.get_column_names_for_query(query)

        if set(primary_key_columns) - set(column_names):
            self._logger.warning(
                f"Skipping query {query} for tables {', '.join(tables)} as primary key column name/id_column is not present in query."
            )
            return

        async for row in client.yield_rows_for_query(query, primary_key_columns):
            yield row2doc(
                row=row,
                column_names=column_names,
                primary_key_columns=primary_key_columns,
                table=tables,
                timestamp=max(last_update_times) if len(last_update_times) else None,
            )

    async def get_tables_to_fetch(self) -> Union[str, List[str]]:
        tables = configured_tables(self.tables)

        async with self.mysql_client() as client:
            return await client.get_all_table_names() if is_wildcard(tables) else tables
