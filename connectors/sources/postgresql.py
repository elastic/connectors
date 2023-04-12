#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Postgresql source module is responsible to fetch documents from PostgreSQL."""
import ssl
from urllib.parse import quote

from sqlalchemy.ext.asyncio import create_async_engine

from connectors.sources.generic_database import GenericBaseDataSource, Queries
from connectors.utils import get_pem_format

# Below schemas are system schemas and the tables of the systems schema's will not get indexed
SYSTEM_SCHEMA = ["pg_toast", "pg_catalog", "information_schema"]
DEFAULT_SSL_ENABLED = False


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


class PostgreSQLDataSource(GenericBaseDataSource):
    """PostgreSQL"""

    name = "PostgreSQL"
    service_type = "postgresql"

    def __init__(self, configuration):
        """Setup connection to the PostgreSQL database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.ssl_ca = self.configuration["ssl_ca"]
        self.connection_string = f"postgresql+asyncpg://{self.user}:{quote(self.password)}@{self.host}:{self.port}/{self.database}"
        self.queries = PostgreSQLQueries()
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
                },
            }
        )
        return postgresql_configuration

    def _create_engine(self):
        """Create async engine for postgresql"""
        self.engine = create_async_engine(
            self.connection_string,
            connect_args=self.get_connect_args() if self.ssl_enabled else {},
        )

    def get_connect_args(self):
        """Convert string to pem format and create a SSL context

        Returns:
            dictionary: Connection arguments
        """
        pem_format = get_pem_format(key=self.ssl_ca)
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(cadata=pem_format)
        connect_args = {"ssl": ctx}
        return connect_args

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        schema_list = await anext(self.execute_query(query=self.queries.all_schemas()))
        for [schema] in schema_list:
            if schema not in SYSTEM_SCHEMA:
                async for row in self.fetch_rows(schema=schema):
                    yield row, None
