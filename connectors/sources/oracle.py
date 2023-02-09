#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""SQLAlchemy source module is responsible to fetch documents from Oracle."""
from urllib.parse import quote

from sqlalchemy import create_engine

from connectors.sources.generic_database import GenericBaseDataSource

QUERIES = {
    "PING": "SELECT 1+1 FROM DUAL",
    "ALL_TABLE": "SELECT TABLE_NAME FROM all_tables where OWNER = '{user}'",
    "TABLE_PRIMARY_KEY": "SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '{table}' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position",
    "TABLE_DATA": "SELECT * FROM {table}",
    "TABLE_LAST_UPDATE_TIME": "SELECT SCN_TO_TIMESTAMP(MAX(ora_rowscn)) from {table}",
    "TABLE_DATA_COUNT": "SELECT COUNT(*) FROM {table}",
}


class OracleDataSource(GenericBaseDataSource):
    """Oracle Database"""

    name = "Oracle Database"
    service_type = "oracle"

    def __init__(self, configuration):
        """Setup connection to the Oracle database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.is_async = False
        self.dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL={self.protocol})(HOST={self.host})(PORT={self.port}))(CONNECT_DATA=(SID={self.database})))"
        self.connection_string = (
            f"oracle://{self.user}:{quote(self.password)}@{self.dsn}"
        )
        self.queries = QUERIES
        self.dialect = "Oracle"

    def _create_engine(self):
        """Create sync engine for oracle"""
        self.engine = create_engine(self.connection_string)

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        async for row in self.fetch_rows():
            yield row, None
