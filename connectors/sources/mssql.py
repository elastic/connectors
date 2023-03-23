#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft SQL source module is responsible to fetch documents from Microsoft SQL."""
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from connectors.sources.generic_database import GenericBaseDataSource, Queries

SECURED_CONNECTION = False


class MSSQLQueries(Queries):
    """Class contains methods which return query"""

    def ping(self):
        """Query to ping source"""
        return "SELECT 1+1"

    def all_tables(self, **kwargs):
        """Query to get all tables"""
        return f"SELECT table_name FROM information_schema.tables WHERE TABLE_SCHEMA = '{ kwargs['schema'] }'"

    def table_primary_key(self, **kwargs):
        """Query to get the primary key"""
        return f"SELECT C.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME WHERE C.TABLE_NAME='{kwargs['table']}' and C.TABLE_SCHEMA='{kwargs['schema']}' and T.CONSTRAINT_TYPE='PRIMARY KEY'"

    def table_data(self, **kwargs):
        """Query to get the table data"""
        return f'SELECT * FROM {kwargs["schema"]}."{kwargs["table"]}"'

    def table_last_update_time(self, **kwargs):
        """Query to get the last update time of the table"""
        return f"SELECT last_user_update FROM sys.dm_db_index_usage_stats WHERE object_id=object_id('{kwargs['schema']}.{kwargs['table']}')"

    def table_data_count(self, **kwargs):
        """Query to get the number of rows in the table"""
        return f'SELECT COUNT(*) FROM {kwargs["schema"]}."{kwargs["table"]}"'

    def all_schemas(self):
        """Query to get all schemas of database"""
        pass


class MSSQLDataSource(GenericBaseDataSource):
    """Microsoft SQL Server"""

    name = "Microsoft SQL Server"
    service_type = "mssql"

    def __init__(self, configuration):
        """Setup connection to the Microsoft SQL database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.mssql_driver = self.configuration["mssql_driver"]
        self.secured_connection = self.configuration["secured_connection"]
        self.queries = MSSQLQueries()
        self.dialect = "Microsoft SQL"
        self.schema = self.configuration["schema"]

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for database-server configured by user

        Returns:
            dictionary: Default configuration
        """
        mssql_configuration = super().get_default_configuration().copy()
        mssql_configuration.update(
            {
                "schema": {
                    "label": "Schema",
                    "order": 9,
                    "type": "str",
                    "value": "dbo",
                },
                "mssql_driver": {
                    "label": "Microsoft SQL Driver Name (ODBC Driver 18 for SQL Server)",
                    "order": 10,
                    "type": "str",
                    "value": "ODBC Driver 18 for SQL Server",
                },
                "secured_connection": {
                    "display": "toggle",
                    "label": "Connection will be secured or not",
                    "order": 11,
                    "type": "bool",
                    "value": SECURED_CONNECTION,
                },
            }
        )
        return mssql_configuration

    def _create_engine(self):
        """Create sync engine for mssql"""
        if self.secured_connection:
            query = {
                "driver": self.mssql_driver,
                "TrustServerCertificate": "no",
                "Encrypt": "Yes",
            }
        else:
            query = {"driver": self.mssql_driver, "TrustServerCertificate": "yes"}

        connection_string = URL.create(
            "mssql+pyodbc",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query=query,
        )
        self.engine = create_engine(connection_string)

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        async for row in self.fetch_rows(schema=self.schema):
            yield row, None
