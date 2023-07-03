#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft SQL source module is responsible to fetch documents from Microsoft SQL."""
import os
from tempfile import NamedTemporaryFile

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from connectors.sources.generic_database import GenericBaseDataSource, Queries

# Connector will skip the below tables if it gets from the input
TABLES_TO_SKIP = {"msdb": ["sysutility_ucp_configuration_internal"]}


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
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.ssl_ca = self.configuration["ssl_ca"]
        self.validate_host = self.configuration["validate_host"]
        self.queries = MSSQLQueries()
        self.dialect = "Microsoft SQL"
        self.schema = self.configuration["schema"]
        self.tables_to_skip = TABLES_TO_SKIP

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
                    "value": "",
                },
                "validate_host": {
                    "display": "toggle",
                    "label": "Validate host",
                    "order": 12,
                    "type": "bool",
                    "value": False,
                },
            }
        )
        return mssql_configuration

    def _create_engine(self):
        """Create sync engine for mssql"""
        connection_string = URL.create(
            "mssql+pytds",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )
        connect_args = {}
        if self.ssl_enabled:
            self.create_pem_file()
            connect_args = {
                "cafile": self.certfile,
                "validate_host": self.validate_host,
            }
        self.engine = create_engine(connection_string, connect_args=connect_args)

    async def close(self):
        """Close the connection to the database server."""
        if os.path.exists(self.certfile):
            try:
                os.remove(self.certfile)
            except Exception as exception:
                self._logger.warning(
                    f"Something went wrong while removing temporary certificate file. Exception: {exception}"
                )
        if self.connection is None:
            return
        self.connection.close()

    def get_pem_format(self, keys, max_split=-1):
        """Convert keys into PEM format.

        Args:
            keys (str): Key in raw format.
            max_split (int): Specifies how many splits to do. Defaults to -1.

        Returns:
            string: PEM format
        """
        all_cert = ""
        for key in keys.split("-----END CERTIFICATE-----")[:-1]:
            key = key.strip() + "\n" + "-----END CERTIFICATE-----"
            key = key.replace(" ", "\n")
            key = " ".join(key.split("\n", max_split))
            key = " ".join(key.rsplit("\n", max_split))
            all_cert += key + "\n"
        return all_cert

    def create_pem_file(self):
        """Create pem file for SSL Verification"""
        pem_certificates = self.get_pem_format(keys=self.ssl_ca, max_split=1)
        with NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as cert:
            cert.write(pem_certificates)
            self.certfile = cert.name

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        async for row in self.fetch_rows(schema=self.schema):
            yield row, None
