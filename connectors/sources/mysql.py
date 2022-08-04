#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""MySQL source module responsible to fetch documents from MySQL"""
import aiomysql

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import iso_utc


class MysqlDatabaseConnectionManager(object):
    """This is the context manager which handles MySQL server connection"""

    def __init__(self, connection_string):
        """Initialise connection string to connect to the MySQL server.

        Args:
            connection_string (Dict): Dictionary of connection string parameters
        """
        self.connection_string = connection_string
        self.connection = None

    async def __aenter__(self):
        """Initialize a MySQL database connection"""
        try:
            self.connection = await aiomysql.connect(**self.connection_string)
            self.cursor = await self.connection.cursor()
            return self
        except aiomysql.OperationalError as error:
            raise

    async def __aexit__(self, exception_type, exception_value, traceback):
        """Close the MySQL database connection

        Args:
            exception_type (str): Type of exception
            exception_value (str): Value of exception
            traceback (str): Type of Traceback
        """
        await self.cursor.close()
        self.connection.close()


class MySqlDataSource(BaseDataSource):
    """Class to fetch and modify documents from MySQL server"""

    def __init__(self, connector):
        """Setup connection to the MySQL server.

        Args:
            connector (BYOConnector): Object of the BYOConnector class
        """
        super().__init__(connector=connector)
        self.connection_string = {
            "host": self.configuration["host"],
            "port": self.configuration["port"],
            "user": self.configuration["user"],
            "password": self.configuration["password"],
            "db": None,
        }
        self._first_sync = self._dirty = True

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for MySQL server

        Returns:
            dictionary: Default configuration
        """
        return {
            "host": {
                "value": "localhost",
                "label": "MySQL Host",
                "type": "str",
            },
            "port": {
                "value": 3306,
                "label": "MySQL Port",
                "type": "int",
            },
            "user": {
                "value": "mysql_user",
                "label": "MySQL Username",
                "type": "str",
            },
            "password": {
                "value": "mysql_password",
                "label": "MySQL Password",
                "type": "str",
            },
            "database": {
                "value": [],
                "label": "List of MySQL Databases",
                "type": "list",
            },
        }

    async def ping(self):
        """Verify the connection with MySQL server"""
        async with MysqlDatabaseConnectionManager(
            connection_string=self.connection_string
        ) as context_manager:
            try:
                await context_manager.connection.ping()
                logger.info("Successfully connected to the MySQL Server.")
            except Exception:
                logger.exception("Error while connecting to the MySQL Server.")
                raise

    async def get_docs(self):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        # TODO: Fetch the documents from MySQL server
        # yield dummy document to run ping implementation
        yield {"_id": "123", "timestamp": iso_utc()}, None
        self._dirty = False
