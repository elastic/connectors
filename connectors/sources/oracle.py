#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Oracle source module is responsible to fetch documents from Oracle."""
import os
from urllib.parse import quote

from sqlalchemy import create_engine

from connectors.sources.generic_database import GenericBaseDataSource, Queries

DEFAULT_PROTOCOL = "TCP"
DEFAULT_ORACLE_HOME = ""


class OracleQueries(Queries):
    """Class contains methods which return query"""

    def ping(self):
        """Query to ping source"""
        return "SELECT 1+1 FROM DUAL"

    def all_tables(self, **kwargs):
        """Query to get all tables"""
        return f"SELECT TABLE_NAME FROM all_tables where OWNER = '{kwargs['user']}'"

    def table_primary_key(self, **kwargs):
        """Query to get the primary key"""
        return f"SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '{kwargs['table']}' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = '{kwargs['user']}' AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position"

    def table_data(self, **kwargs):
        """Query to get the table data"""
        return f"SELECT * FROM {kwargs['table']}"

    def table_last_update_time(self, **kwargs):
        """Query to get the last update time of the table"""
        return f"SELECT SCN_TO_TIMESTAMP(MAX(ora_rowscn)) from {kwargs['table']}"

    def table_data_count(self, **kwargs):
        """Query to get the number of rows in the table"""
        return f"SELECT COUNT(*) FROM {kwargs['table']}"

    def all_schemas(self):
        """Query to get all schemas of database"""
        pass  # Multiple schemas not supported in Oracle


class OracleDataSource(GenericBaseDataSource):
    """Oracle Database"""

    name = "Oracle Database"
    service_type = "oracle"

    def __init__(self, configuration):
        """Setup connection to the Oracle database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.is_async = False
        self.oracle_home = self.configuration["oracle_home"]
        self.wallet_config = self.configuration["wallet_configuration_path"]
        self.protocol = self.configuration["oracle_protocol"]
        self.dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL={self.protocol})(HOST={self.host})(PORT={self.port}))(CONNECT_DATA=(SID={self.database})))"
        self.connection_string = (
            f"oracle+oracledb://{self.user}:{quote(self.password)}@{self.dsn}"
        )
        self.queries = OracleQueries()
        self.dialect = "Oracle"

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for database-server configured by user

        Returns:
            dictionary: Default configuration
        """
        oracle_configuration = super().get_default_configuration().copy()
        oracle_configuration.update(
            {
                "oracle_protocol": {
                    "display": "dropdown",
                    "label": "Oracle connection protocol",
                    "options": [
                        {"label": "TCP", "value": "TCP"},
                        {"label": "TCPS", "value": "TCPS"},
                    ],
                    "order": 9,
                    "type": "str",
                    "value": DEFAULT_PROTOCOL,
                },
                "oracle_home": {
                    "default_value": DEFAULT_ORACLE_HOME,
                    "label": "Path of Oracle Service",
                    "order": 10,
                    "required": False,
                    "type": "str",
                },
                "wallet_configuration_path": {
                    "default_value": "",
                    "label": "Path of Oracle Service configuration files",
                    "order": 11,
                    "required": False,
                    "type": "str",
                },
            }
        )
        return oracle_configuration

    def _create_engine(self):
        """Create sync engine for oracle"""
        if self.oracle_home != "":
            os.environ["ORACLE_HOME"] = self.oracle_home
            self.engine = create_engine(
                self.connection_string,
                thick_mode={
                    "lib_dir": f"{self.oracle_home}/lib",
                    "config_dir": self.wallet_config,
                },
            )
        else:
            self.engine = create_engine(self.connection_string)

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        async for row in self.fetch_rows():
            yield row, None
