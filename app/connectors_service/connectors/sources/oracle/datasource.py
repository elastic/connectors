#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from asyncpg.exceptions._base import InternalClientError
from connectors_sdk.source import BaseDataSource
from connectors_sdk.utils import iso_utc
from sqlalchemy.exc import ProgrammingError

from connectors.sources.oracle.client import OracleClient
from connectors.sources.shared.database.generic_database import (
    DEFAULT_FETCH_SIZE,
    DEFAULT_RETRY_COUNT,
    map_column_names,
)

DEFAULT_PROTOCOL = "TCP"
DEFAULT_ORACLE_HOME = ""
SID = "sid"
SERVICE_NAME = "service_name"


class OracleDataSource(BaseDataSource):
    """Oracle Database"""

    name = "Oracle Database"
    service_type = "oracle"

    def __init__(self, configuration):
        """Setup connection to the Oracle database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.database = (
            self.configuration["sid"]
            if self.configuration["connection_source"] == SID
            else self.configuration["service_name"]
        )
        self.oracle_client = OracleClient(
            host=self.configuration["host"],
            port=self.configuration["port"],
            user=self.configuration["username"],
            password=self.configuration["password"],
            connection_source=self.configuration["connection_source"],
            sid=self.configuration["sid"],
            service_name=self.configuration["service_name"],
            tables=self.configuration["tables"],
            protocol=self.configuration["oracle_protocol"],
            oracle_home=self.configuration["oracle_home"],
            wallet_config=self.configuration["wallet_configuration_path"],
            retry_count=self.configuration["retry_count"],
            fetch_size=self.configuration["fetch_size"],
            logger_=self._logger,
        )

    def _set_internal_logger(self):
        self.oracle_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        return {
            "host": {
                "label": "Host",
                "order": 1,
                "type": "str",
            },
            "port": {
                "display": "numeric",
                "label": "Port",
                "order": 2,
                "type": "int",
            },
            "username": {
                "label": "Username",
                "order": 3,
                "type": "str",
            },
            "password": {
                "label": "Password",
                "order": 4,
                "sensitive": True,
                "type": "str",
            },
            "connection_source": {
                "display": "dropdown",
                "label": "Connection Source",
                "options": [
                    {"label": "SID", "value": SID},
                    {"label": "Service Name", "value": SERVICE_NAME},
                ],
                "order": 5,
                "type": "str",
                "value": SID,
                "tooltip": "Select 'Service Name' option if connecting to a pluggable database",
            },
            "sid": {
                "depends_on": [{"field": "connection_source", "value": SID}],
                "label": "SID",
                "order": 6,
                "type": "str",
            },
            "service_name": {
                "depends_on": [{"field": "connection_source", "value": SERVICE_NAME}],
                "label": "Service Name",
                "order": 7,
                "type": "str",
            },
            "tables": {
                "display": "textarea",
                "label": "Comma-separated list of tables",
                "options": [],
                "order": 8,
                "type": "list",
                "value": "*",
            },
            "fetch_size": {
                "default_value": DEFAULT_FETCH_SIZE,
                "display": "numeric",
                "label": "Rows fetched per request",
                "order": 9,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "retry_count": {
                "default_value": DEFAULT_RETRY_COUNT,
                "display": "numeric",
                "label": "Retries per request",
                "order": 10,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "oracle_protocol": {
                "default_value": DEFAULT_PROTOCOL,
                "display": "dropdown",
                "label": "Oracle connection protocol",
                "options": [
                    {"label": "TCP", "value": "TCP"},
                    {"label": "TCPS", "value": "TCPS"},
                ],
                "order": 11,
                "type": "str",
                "value": DEFAULT_PROTOCOL,
                "ui_restrictions": ["advanced"],
            },
            "oracle_home": {
                "default_value": DEFAULT_ORACLE_HOME,
                "label": "Path to Oracle Home",
                "order": 12,
                "required": False,
                "type": "str",
                "value": DEFAULT_ORACLE_HOME,
                "ui_restrictions": ["advanced"],
            },
            "wallet_configuration_path": {
                "default_value": "",
                "label": "Path to SSL Wallet configuration files",
                "order": 13,
                "required": False,
                "type": "str",
                "ui_restrictions": ["advanced"],
            },
        }

    async def close(self):
        self.oracle_client.close()

    async def ping(self):
        """Verify the connection with the database-server configured by user"""
        self._logger.debug("Validating that the Connector can connect to Oracle...")
        try:
            await self.oracle_client.ping()
            self._logger.debug("Successfully connected to Oracle")
        except Exception as e:
            msg = f"Can't connect to Oracle on {self.oracle_client.host}"
            raise Exception(msg) from e

    async def fetch_documents(self, table):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            table (str): Name of table

        Yields:
            Dict: Document to be indexed
        """
        try:
            self._logger.info(f"Fetching records for table '{table}'")
            row_count = await self.oracle_client.get_table_row_count(table=table)
            if row_count > 0:
                # Query to get the table's primary key
                self._logger.debug(f"Total {row_count} rows found in table '{table}'")
                keys = await self.oracle_client.get_table_primary_key(table=table)
                keys = map_column_names(column_names=keys, tables=[table])
                if keys:
                    try:
                        last_update_time = (
                            await self.oracle_client.get_table_last_update_time(
                                table=table
                            )
                        )
                    except Exception as e:
                        self._logger.warning(
                            f"Unable to fetch last updated time for table '{table}'; error: {e}"
                        )
                        last_update_time = None
                    streamer = self.oracle_client.data_streamer(table=table)
                    column_names = await anext(streamer)
                    column_names = map_column_names(
                        column_names=column_names, tables=[table]
                    )
                    async for row in streamer:
                        row = dict(zip(column_names, row, strict=True))
                        keys_value = ""
                        for key in keys:
                            keys_value += f"{row.get(key)}_" if row.get(key) else ""
                        row.update(
                            {
                                "_id": f"{self.database}_{table}_{keys_value}",
                                "_timestamp": last_update_time or iso_utc(),
                                "Database": self.database,
                                "Table": table,
                            }
                        )
                        yield self.serialize(doc=row)
                else:
                    self._logger.warning(
                        f"Skipping '{table}' table from database '{self.database}' since no primary key is associated with it. Assign a primary key to the table to index it in the next sync interval."
                    )
            else:
                self._logger.warning(f"No records found for table '{table}'")
        except (InternalClientError, ProgrammingError) as exception:
            self._logger.warning(
                f"Something went wrong while fetching records from table '{table}'; error: {exception}"
            )

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        table_count = 0
        async for table in self.oracle_client.get_tables_to_fetch():
            table_count += 1
            async for row in self.fetch_documents(table=table):
                yield row, None
        if table_count < 1:
            self._logger.warning(f"Fetched 0 tables for the database '{self.database}'")
