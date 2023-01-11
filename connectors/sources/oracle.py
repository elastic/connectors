#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""SQLAlchemy source module is responsible to fetch documents from Oracle."""
from urllib.parse import quote

from sqlalchemy import create_engine

from connectors.logger import logger
from connectors.sources.generic_database import GenericBaseDataSource


class OracleDataSource(GenericBaseDataSource):
    """Class to fetch documents from Oracle Server"""

    def __init__(self, connector):
        """Setup connection to the Oracle database-server configured by user

        Args:
            connector (BYOConnector): Object of the BYOConnector class
        """
        super().__init__(connector=connector)
        self.connection_string = f"oracle://{self.user}:{quote(self.password)}@{self.host}:{self.port}/{self.database}"
        self.queries = {
            "PING": "SELECT 1+1 FROM DUAL",
            "ALL_TABLE": "SELECT TABLE_NAME FROM all_tables where OWNER = '{user}'",
            "TABLE_PRIMARY_KEY": "SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '{table}' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position",
            "TABLE_DATA": "SELECT * FROM {table}",
            "TABLE_LAST_UPDATE_TIME": "SELECT SCN_TO_TIMESTAMP(MAX(ora_rowscn)) from {table}",
            "TABLE_DATA_COUNT": "SELECT COUNT(*) FROM {table}",
        }
        self.engine = None

    async def ping(self):
        """Verify the connection with the Oracle database-server configured by user"""
        logger.info("Validating the Connector Configuration...")
        try:
            self._validate_configuration()
            self.engine = create_engine(self.connection_string)
            await anext(
                self.execute_query(
                    query_name="PING", engine=self.engine, is_async=False
                )
            )
            logger.info("Successfully connected to Oracle.")
        except Exception:
            raise Exception(f"Can't connect to Oracle Server on {self.host}")

    async def get_docs(self):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        async for row in self.fetch_rows(engine=self.engine, is_async=False):
            yield row, None
