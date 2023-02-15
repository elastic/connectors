#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Postgresql source module is responsible to fetch documents from PostgreSQL."""
import ssl
from urllib.parse import quote

from sqlalchemy.ext.asyncio import create_async_engine

from connectors.sources.generic_database import GenericBaseDataSource

# Below schemas are system schemas and the tables of the systems schema's will not get indexed
SYSTEM_SCHEMA = ["pg_toast", "pg_catalog", "information_schema"]
DEFAULT_SSL_DISABLED = True
DEFAULT_SSL_CA = ""

QUERIES = {
    "PING": "SELECT 1+1",
    "ALL_TABLE": "SELECT table_name FROM information_schema.tables WHERE table_catalog = '{database}' and table_schema = '{schema}'",
    "TABLE_PRIMARY_KEY": "SELECT c.column_name FROM information_schema.table_constraints tc JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{table}' and tc.constraint_schema = '{schema}'",
    "TABLE_DATA": 'SELECT * FROM {schema}."{table}"',
    "TABLE_LAST_UPDATE_TIME": 'SELECT MAX(pg_xact_commit_timestamp(xmin)) FROM {schema}."{table}"',
    "TABLE_DATA_COUNT": 'SELECT COUNT(*) FROM {schema}."{table}"',
    "ALL_SCHEMAS": "SELECT schema_name FROM information_schema.schemata",
}


class PostgreSQLDataSource(GenericBaseDataSource):
    """PostgreSQL"""

    name = "PostgreSQL"
    service_type = "postgresql"

    def __init__(self, configuration):
        """Setup connection to the PostgreSQL database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.ssl_disabled = self.configuration["ssl_disabled"]
        self.ssl_ca = self.configuration["ssl_ca"]
        self.connection_string = f"postgresql+asyncpg://{self.user}:{quote(self.password)}@{self.host}:{self.port}/{self.database}"
        self.queries = QUERIES
        self.is_async = True
        self.dialect = "Postgresql"

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for database-server configured by user

        Returns:
            dictionary: Default configuration
        """
        postgresql_configuration = super().get_default_configuration().copy()
        postgresql_configuration.update(
            {
                "ssl_disabled": {
                    "value": DEFAULT_SSL_DISABLED,
                    "label": "SSL verification will be disabled or not",
                    "type": "bool",
                },
                "ssl_ca": {
                    "value": DEFAULT_SSL_CA,
                    "label": "SSL certificate",
                    "type": "str",
                },
            }
        )
        return postgresql_configuration

    def _create_engine(self):
        """Create async engine for postgresql"""
        self.engine = create_async_engine(
            self.connection_string,
            connect_args=self.get_connect_args() if not self.ssl_disabled else {},
        )

    def get_pem_format(self):
        """Convert ca data into PEM format

        Returns:
            string: PEM format
        """
        self.ssl_ca = self.ssl_ca.replace(" ", "\n")
        pem_format = " ".join(self.ssl_ca.split("\n", 1))
        pem_format = " ".join(pem_format.rsplit("\n", 1))
        return pem_format

    def get_connect_args(self):
        """Convert string to pem format and create a SSL context

        Returns:
            dictionary: Connection arguments
        """
        pem_format = self.get_pem_format()
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(cadata=pem_format)
        connect_args = {"ssl": ctx}
        return connect_args

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        schema_list = await anext(self.execute_query(query_name="ALL_SCHEMAS"))
        for [schema] in schema_list:
            if schema not in SYSTEM_SCHEMA:
                async for row in self.fetch_rows(schema=schema):
                    yield row, None
