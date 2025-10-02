#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""MySQL source module responsible to fetch documents from MySQL"""

import re

import aiomysql
import fastjsonschema
from connectors_sdk.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)

from fastjsonschema import JsonSchemaValueException

from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    retryable,
    ssl_context,
)

from connectors.sources.mysql.mysql_utils import (
    format_list,
    MAX_POOL_SIZE,
    DEFAULT_FETCH_SIZE,
    RETRIES,
    RETRY_INTERVAL
)

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

    def __init__(self, source):
        self.source = source

    async def validate(self, advanced_rules):
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
    async def _remote_validation(self, advanced_rules):
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
        host,
        port,
        user,
        password,
        ssl_enabled,
        ssl_certificate,
        logger_,
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
        self.connection_pool = None
        self.connection = None
        self._logger = logger_

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
            await cursor.execute(
                f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{self.database}'"
            )
            return [table[0] for table in await cursor.fetchall()]

    async def ping(self):
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
    async def get_column_names_for_query(self, query):
        async with self.connection.cursor(aiomysql.cursors.SSCursor) as cursor:
            await cursor.execute(f"SELECT q.* FROM ({query}) as q LIMIT 0")

            return [f"{column[0]}" for column in cursor.description]

    async def get_column_names_for_table(self, table):
        return await self.get_column_names_for_query(
            f"SELECT * FROM `{self.database}`.`{table}`"
        )

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_primary_key_column_names(self, table):
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
    async def get_last_update_time(self, table):
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

    async def _get_table_row_count_for_query(self, query):
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
        self, query, offset, primary_key_columns
    ):
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
