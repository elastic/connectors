#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""SQLAlchemy source module is responsible to fetch documents from PostgreSQL."""
from urllib.parse import quote

from sqlalchemy.ext.asyncio import create_async_engine

from connectors.logger import logger
from connectors.sources.generic_database import GenericBaseDataSource

# Below schemas are system schemas and the tables of the systems schema's will not get indexed
SYSTEM_SCHEMA = ["pg_toast", "pg_catalog", "information_schema"]


class PostgreSQLDataSource(GenericBaseDataSource):
    """Class to fetch documents from Postgresql Server"""

    def __init__(self, connector):
        """Setup connection to the PostgreSQL database-server configured by user

        Args:
            connector (BYOConnector): Object of the BYOConnector class
        """
        super().__init__(connector=connector)
        self.connection_string = f"postgresql+asyncpg://{self.user}:{quote(self.password)}@{self.host}:{self.port}/{self.database}"
        self.queries = {
            "PING": "SELECT 1+1",
            "ALL_TABLE": "SELECT table_name FROM information_schema.tables WHERE table_catalog = '{database}' and table_schema = '{schema}'",
            "TABLE_PRIMARY_KEY": "SELECT c.column_name FROM information_schema.table_constraints tc JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{table}' and tc.constraint_schema = '{schema}'",
            "TABLE_DATA": 'SELECT * FROM {schema}."{table}"',
            "TABLE_LAST_UPDATE_TIME": 'SELECT MAX(pg_xact_commit_timestamp(xmin)) FROM {schema}."{table}"',
            "TABLE_DATA_COUNT": 'SELECT COUNT(*) FROM {schema}."{table}"',
            "ALL_SCHEMAS": "SELECT schema_name FROM information_schema.schemata",
        }
        self.engine = None

    async def ping(self):
        """Verify the connection with the PostgreSQL database-server configured by user"""
        logger.info("Validating the Connector Configuration...")
        try:
            self._validate_configuration()
            self.engine = create_async_engine(self.connection_string)
            await anext(self.execute_query(query_name="PING", engine=self.engine))
            logger.info("Successfully connected to PostgreSQL.")
        except Exception:
            raise Exception(f"Can't connect to Postgresql Server on {self.host}")

    async def get_docs(self):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        schema_list = await anext(
            self.execute_query(query_name="ALL_SCHEMAS", engine=self.engine)
        )
        for [schema] in schema_list:
            if schema not in SYSTEM_SCHEMA:
                async for row in self.fetch_rows(engine=self.engine, schema=schema):
                    yield row, None
