#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft SQL source module is responsible to fetch documents from Microsoft SQL."""
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from connectors.sources.generic_database import GenericBaseDataSource

SECURED_CONNECTION = False
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

QUERIES = {
    "PING": "SELECT 1+1",
    "ALL_TABLE": "SELECT table_name FROM information_schema.tables WHERE TABLE_SCHEMA = '{schema}'",
    "TABLE_PRIMARY_KEY": "SELECT C.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME WHERE C.TABLE_NAME='{table}' and C.TABLE_SCHEMA='{schema}' and T.CONSTRAINT_TYPE='PRIMARY KEY' ",
    "TABLE_DATA": 'SELECT * FROM {schema}."{table}"',
    "TABLE_LAST_UPDATE_TIME": "SELECT last_user_update FROM sys.dm_db_index_usage_stats WHERE object_id=object_id('{schema}.{table}')",
    "TABLE_DATA_COUNT": 'SELECT COUNT(*) FROM {schema}."{table}"',
    "ALL_SCHEMAS": "SELECT s.name from sys.schemas s inner join sys.sysusers u on u.uid = s.principal_id",
}


class MSSQLDataSource(GenericBaseDataSource):
    """Microsoft SQL Server"""

    def __init__(self, configuration):
        """Setup connection to the Microsoft SQL database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.mssql_driver = self.configuration["mssql_driver"]
        self.secured_connection = self.configuration["secured_connection"]
        self.connection_string = ""
        self.queries = QUERIES
        self.dialect = "Microsoft SQL"

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for database-server configured by user

        Returns:
            dictionary: Default configuration
        """
        mssql_configuration = super().get_default_configuration().copy()
        mssql_configuration.update(
            {
                "mssql_driver": {
                    "value": "ODBC Driver 18 for SQL Server",
                    "label": "Microsoft SQL Driver",
                    "type": "str",
                },
                "secured_connection": {
                    "value": SECURED_CONNECTION,
                    "label": "Connection will be secured or not",
                    "type": "bool",
                },
            }
        )
        return mssql_configuration

    def _create_engine(self):
        """Create sync engine for mssql"""
        if self.secured_connection:
            self.connection_string = URL.create(
                "mssql+pyodbc",
                username=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
                query={
                    "driver": self.mssql_driver,
                    "TrustServerCertificate": "no",
                    "Encrypt": "Yes",
                },
            )
        else:
            self.connection_string = URL.create(
                "mssql+pyodbc",
                username=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
                query={"driver": self.mssql_driver, "TrustServerCertificate": "yes"},
            )
        self.engine = create_engine(self.connection_string)

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        schema_list = await anext(
            self.execute_query(
                query_name="ALL_SCHEMAS",
            )
        )
        for [schema] in schema_list:
            if schema not in SYSTEM_SCHEMA:
                async for row in self.fetch_rows(
                    schema=schema,
                ):
                    yield row, None
