import aiomysql
import re

from connectors.sources.mysql.mysql import (
    MySQLAdvancedRulesValidator,
    MySQLClient,
    DEFAULT_FETCH_SIZE,
    RETRIES,
    RETRY_INTERVAL,
    row2doc,
    format_list
)

from connectors.sources.shared.database.generic_database import (
    configured_tables,
    is_wildcard,
)

from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    retryable,
)

from connectors_sdk.source import BaseDataSource, ConfigurableFieldValueError

SPLIT_BY_COMMA_OUTSIDE_BACKTICKS_PATTERN = re.compile(r"`(?:[^`]|``)+`|\w+")


class MySqlDataSource(BaseDataSource):
    """MySQL"""

    name = "MySQL"
    service_type = "mysql"
    advanced_rules_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self._sleeps = CancellableSleeps()
        self.retry_count = self.configuration["retry_count"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]
        self.database = self.configuration["database"]
        self.tables = self.configuration["tables"]

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

    async def close(self):
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

    async def _validate_database_accessible(self, cursor):
        try:
            await cursor.execute(f"USE `{self.database}`;")
        except aiomysql.Error as e:
            msg = f"The database '{self.database}' is either not present or not accessible for the user '{self.configuration['user']}'."
            raise ConfigurableFieldValueError(msg) from e

    async def _validate_tables_accessible(self, cursor):
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

    async def fetch_documents(self, tables, query=None, id_columns=None):
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

    async def _yield_docs_custom_query(self, client, tables, query, id_columns):
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

    async def get_tables_to_fetch(self):
        tables = configured_tables(self.tables)

        async with self.mysql_client() as client:
            return await client.get_all_table_names() if is_wildcard(tables) else tables
