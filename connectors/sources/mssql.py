#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""SQLAlchemy source module is responsible to fetch documents from Microsoft SQL."""
from urllib.parse import quote

from sqlalchemy import create_engine

from connectors.logger import logger
from connectors.sources.generic_database import GenericBaseDataSource

# Below schemas are system schemas and the tables of the systems schema's will not get indexed
SYSTEM_SCHEMA = [
    "INFORMATION_SCHEMA",
    "db_owner",
    "db_accessadmin",
    "db_securityadmin",
    "db_ddladmin",
    "db_backupoperator",
    "db_datareader",
    "db_datawriter",
    "db_denydatareader",
    "db_denydatawriter",
    "sys",
]


class MSSQLDataSource(GenericBaseDataSource):
    """Class to fetch documents from Microsoft SQL Server"""

    def __init__(self, connector):
        """Setup connection to the Microsoft SQL database-server configured by user

        Args:
            connector (BYOConnector): Object of the BYOConnector class
        """
        super().__init__(connector=connector)
        self.connection_string = f"mssql+pymssql://{self.user}:{quote(self.password)}@{self.host}:{self.port}/{self.database}"
        self.queries = {
            "PING": "SELECT 1+1",
            "ALL_TABLE": "SELECT table_name FROM information_schema.tables WHERE TABLE_SCHEMA = '{schema}'",
            "TABLE_PRIMARY_KEY": "SELECT C.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME WHERE C.TABLE_NAME='{table}' and C.TABLE_SCHEMA='{schema}' and T.CONSTRAINT_TYPE='PRIMARY KEY' ",
            "TABLE_DATA": 'SELECT * FROM {schema}."{table}"',
            "TABLE_LAST_UPDATE_TIME": "SELECT last_user_update FROM sys.dm_db_index_usage_stats WHERE object_id=object_id('{schema}.{table}')",
            "TABLE_DATA_COUNT": 'SELECT COUNT(*) FROM {schema}."{table}"',
            "ALL_SCHEMAS": "SELECT s.name from sys.schemas s inner join sys.sysusers u on u.uid = s.principal_id",
        }
        self.engine = None

    async def ping(self):
        """Verify the connection with the Microsoft SQL database-server configured by user"""
        logger.info("Validating the Connector Configuration...")
        try:
            self._validate_configuration()
            self.engine = create_engine(self.connection_string)
            await anext(
                self.execute_query(
                    query_name="PING",
                    engine=self.engine,
                    is_async=False,
                )
            )
            logger.info("Successfully connected to Microsoft SQL.")
        except Exception:
            raise Exception(f"Can't connect to Microsoft SQL Server on {self.host}")

    async def get_docs(self):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        schema_list = await anext(
            self.execute_query(
                query_name="ALL_SCHEMAS", engine=self.engine, is_async=False
            )
        )
        for [schema] in schema_list:
            if schema not in SYSTEM_SCHEMA:
                async for row in self.fetch_rows(
                    engine=self.engine, schema=schema, is_async=False
                ):
                    yield row, None
