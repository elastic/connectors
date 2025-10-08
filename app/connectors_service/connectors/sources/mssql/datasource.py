#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from asyncpg.exceptions._base import InternalClientError
from connectors_sdk.source import BaseDataSource
from connectors_sdk.utils import iso_utc
from sqlalchemy.exc import ProgrammingError

from connectors.sources.mssql.client import MSSQLClient
from connectors.sources.mssql.validator import MSSQLAdvancedRulesValidator
from connectors.sources.shared.database.generic_database import (
    DEFAULT_FETCH_SIZE,
    DEFAULT_RETRY_COUNT,
    hash_id,
    map_column_names,
)

# Connector will skip the below tables if it gets from the input
TABLES_TO_SKIP = {"msdb": ["sysutility_ucp_configuration_internal"]}


class MSSQLDataSource(BaseDataSource):
    """Microsoft SQL Server"""

    name = "Microsoft SQL Server"
    service_type = "mssql"
    advanced_rules_enabled = True

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
                "tooltip": "This configurable field is ignored when Advanced Sync Rules are used.",
                "type": "list",
                "value": "*",
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
            "schema": {
                "label": "Schema",
                "order": 9,
                "type": "str",
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
            "validate_host": {
                "display": "toggle",
                "label": "Validate host",
                "order": 12,
                "type": "bool",
                "value": False,
            },
        }

    def advanced_rules_validators(self):
        return [MSSQLAdvancedRulesValidator(self)]

    async def close(self):
        self.mssql_client.close()

    async def ping(self):
        """Verify the connection with the database-server configured by user"""
        self._logger.debug("Validating the Connector Configuration...")
        try:
            await self.mssql_client.ping()
        except Exception as e:
            msg = f"Can't connect to Microsoft SQL on {self.mssql_client.host}"
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
        self._logger.debug(f"Extracting primary keys for tables: {tables}")
        primary_key_columns = []
        for table in tables:
            primary_key_columns.extend(
                await self.mssql_client.get_table_primary_key(table)
            )
        primary_key_columns = sorted(primary_key_columns)
        return map_column_names(
            column_names=primary_key_columns, schema=self.schema, tables=tables
        )

    async def yield_rows_for_query(self, primary_key_columns, tables, query=None):
        if query is None:
            streamer = self.mssql_client.data_streamer(table=tables[0])
        else:
            streamer = self.mssql_client.data_streamer(query=query)
        column_names = await anext(streamer)
        column_names = map_column_names(
            column_names=column_names, schema=self.schema, tables=tables
        )

        if (not set(primary_key_columns) - set(column_names)) or primary_key_columns:
            async for row in streamer:
                row = dict(zip(column_names, row, strict=True))
                yield row
        else:
            self._logger.warning(
                f"Skipping query {query} for tables {', '.join(tables)} as primary key column name is not present in query."
            )

    async def fetch_documents_from_table(self, table):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            table (str): Name of table

        Yields:
            Dict: Document to be indexed
        """
        self._logger.info(f'Fetching records for table "{table}"')
        try:
            docs_generator = self._yield_all_docs_from_tables(table=table)
            async for doc in docs_generator:
                yield doc
        except (InternalClientError, ProgrammingError) as exception:
            self._logger.warning(
                f'Something went wrong while fetching document for table "{table}". Error: {exception}'
            )

    async def fetch_documents_from_query(self, tables, query, id_columns):
        """Fetches all the data from the given query and format them in Elasticsearch documents

        Args:
            table (str): Name of table
            query (str): Database Query

        Yields:
            Dict: Document to be indexed
        """
        self._logger.info(
            f"Fetching records for tables {tables} using the custom query: {query} and available id_columns: {id_columns}"
        )
        try:
            docs_generator = self._yield_docs_custom_query(
                tables=tables, query=query, id_columns=id_columns
            )
            async for doc in docs_generator:
                yield doc
        except (InternalClientError, ProgrammingError) as exception:
            self._logger.warning(
                f"Something went wrong while fetching document for query {query} and tables {', '.join(tables)}. Error: {exception}"
            )

    async def _yield_docs_custom_query(self, tables, query, id_columns=None):
        primary_key_columns = await self.get_primary_key(tables=tables)

        if id_columns:
            primary_key_columns = id_columns

        if not primary_key_columns:
            self._logger.warning(
                f"Skipping tables {', '.join(tables)} from database {self.database} since no primary key is associated with them. Assign primary key to the tables to index it in the next sync interval."
            )
            return

        last_update_times = list(
            filter(
                lambda update_time: update_time is not None,
                [
                    await self.mssql_client.get_table_last_update_time(table)
                    for table in tables
                ],
            )
        )

        last_update_times = [
            time_value for time_value in last_update_times if time_value is not None
        ]

        last_update_time = (
            max(last_update_times) if len(last_update_times) else iso_utc()
        )

        async for row in self.yield_rows_for_query(
            primary_key_columns=primary_key_columns,
            tables=tables,
            query=query,
        ):
            doc_id = f"{self.database}_{self.schema}_{hash_id(list(tables), row, primary_key_columns)}"

            yield self.serialize(
                doc=self.row2doc(
                    row=row, doc_id=doc_id, table=tables, timestamp=last_update_time
                )
            )

    async def _yield_all_docs_from_tables(self, table):
        row_count = await self.mssql_client.get_table_row_count(table=table)
        if row_count > 0:
            # Query to get the table's primary key
            keys = await self.get_primary_key(tables=[table])
            if keys:
                try:
                    last_update_time = (
                        await self.mssql_client.get_table_last_update_time(table=table)
                    )
                except Exception:
                    self._logger.warning(
                        f'Unable to fetch last_updated_time for table "{table}"'
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
                    f'"{self.database}"."{table}" has no primary key and is skipped. Assign primary key to the table to index it in the next sync interval.'
                )
        else:
            self._logger.warning(f'No rows found for table "{table}"')

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        self._logger.info("Successfully connected to Microsoft SQL")
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            self._logger.info(
                f"Fetching records from the database using advanced sync rules: {advanced_rules}"
            )
            for rule in advanced_rules:
                query = rule.get("query")
                tables = rule.get("tables")
                id_columns = rule.get("id_columns", [])

                id_columns_str = ""
                for i, table in enumerate(tables):
                    if i == 0:
                        id_columns_str = f"{self.schema}_{table}"
                    else:
                        id_columns_str = f"{id_columns_str}_{table}"
                id_columns = [f"{id_columns_str}_{column}" for column in id_columns]

                async for row in self.fetch_documents_from_query(
                    tables=tables, query=query, id_columns=id_columns
                ):
                    yield row, None
        else:
            table_count = 0
            async for table in self.mssql_client.get_tables_to_fetch():
                if (
                    self.database in TABLES_TO_SKIP.keys()
                    and table in TABLES_TO_SKIP[self.database]
                ):
                    self._logger.debug(
                        f"Skip table: {table} in database: {self.database}"
                    )
                    continue

                table_count += 1
                async for row in self.fetch_documents_from_table(table=table):
                    yield row, None
            if table_count < 1:
                self._logger.warning(
                    f"Fetched 0 tables for schema: {self.schema} and database: {self.database}"
                )
