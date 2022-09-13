#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""MySQL source module responsible to fetch documents from MySQL"""
from datetime import date, datetime
from decimal import Decimal

import aiomysql
from bson import Decimal128

from connectors.logger import logger
from connectors.source import BaseDataSource

MAX_POOL_SIZE = 10
QUERIES = {
    "ALL_DATABASE": "SHOW DATABASES",
    "ALL_TABLE": "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{database}'",
    "TABLE_DATA": "SELECT * FROM {database}.{table}",
    "TABLE_PRIMARY_KEY": "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = '{table}' AND COLUMN_KEY = 'PRI'",
    "TABLE_LAST_UPDATE_TIME": "SELECT UPDATE_TIME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = '{table}'",
}


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
            "maxsize": MAX_POOL_SIZE,
        }
        self.connection_pool = None

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for MySQL server

        Returns:
            dictionary: Default configuration
        """
        return {
            "host": {
                "value": "127.0.0.1",
                "label": "MySQL Host",
                "type": "str",
            },
            "port": {
                "value": 3306,
                "label": "MySQL Port",
                "type": "int",
            },
            "user": {
                "value": "root",
                "label": "MySQL Username",
                "type": "str",
            },
            "password": {
                "value": "changeme",
                "label": "MySQL Password",
                "type": "str",
            },
            "database": {
                "value": ["customerinfo"],
                "label": "List of MySQL Databases",
                "type": "list",
            },
            "connector_name": {
                "value": "MySQL Connector",
                "label": "Friendly name for the connector",
                "type": "str",
            },
        }

    async def ping(self):
        """Verify the connection with MySQL server"""
        try:
            self.connection_pool = await aiomysql.create_pool(**self.connection_string)

            async with self.connection_pool.acquire() as connection:
                await connection.ping()
                logger.info("Successfully connected to the MySQL Server.")
        except Exception:
            logger.exception("Error while connecting to the MySQL Server.")
            raise

    async def _execute_query(self, query):
        """Executes the passed query on the MySQL server.

        Args:
            query (str): MySql query to be executed.

        Returns:
            list, tuple: Column names and query response
        """

        async with self.connection_pool.acquire() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(query)
                response = cursor.fetchall().result()
                column_names = [column[0] for column in cursor.description]
                return column_names, response

    async def fetch_rows(self, database):
        """Fetches all the rows from all the tables of the database.

        Args:
            database (str): Name of database
            query (str): Query to fetch database tables

        Yields:
            Dict: Row document to index
        """

        # Query to get all table names from a database
        query = QUERIES["ALL_TABLE"].format(database=database)
        _, query_response = await self._execute_query(query=query)
        if query_response:
            for table in query_response:
                table_name = table[0]
                logger.debug(f"Found table: {table_name} in database: {database}.")

                # Query to get table's data
                query = QUERIES["TABLE_DATA"].format(
                    database=database, table=table_name
                )
                async for row in self.create_document(
                    database=database, table=table_name, query=query
                ):
                    yield row
        else:
            logger.warn(
                f"Fetched 0 tables for the database: {database}. Either the database has no tables or the user does not have access to this database."
            )

    def serialize(self, doc):
        """Reads each element from the document and serialize it as per it’s datatype.

        Args:
            doc (Dict): Dictionary to be serialize

        Returns:
            doc (Dict): Serialized version of dictionary
        """

        def _serialize(value):
            """Serialize input value as per it’s datatype.
            Args:
                value (Any Datatype): Value to be serialize

            Returns:
                value (Any Datatype): Serialized version of input value.
            """

            if isinstance(value, (list, tuple)):
                value = [_serialize(item) for item in value]
            elif isinstance(value, dict):
                for key, svalue in value.items():
                    value[key] = _serialize(svalue)
            elif isinstance(value, (datetime, date)):
                value = value.isoformat()
            elif isinstance(value, Decimal128):
                value = value.to_decimal()
            elif isinstance(value, (bytes, bytearray)):
                value = value.decode(errors="ignore")
            elif isinstance(value, Decimal):
                value = float(value)
            return value

        for key, value in doc.items():
            doc[key] = _serialize(value)

        return doc

    async def create_document(self, database, table, query):
        """Fetches all the table entires and format them in an Elasticsearch document

        Args:
            database (str): Name of database
            table (str): Name of table
            query (str): Query to execute

        Yields:
            Dict: Document to be index
        """

        column_names, query_response = await self._execute_query(query=query)

        # Query to get the table's primary key
        query = QUERIES["TABLE_PRIMARY_KEY"].format(database=database, table=table)
        _, columns = await self._execute_query(query=query)

        keys = []
        if columns:

            # Query to get the table's last update time
            last_update_time_query = QUERIES["TABLE_LAST_UPDATE_TIME"].format(
                database=database, table=table
            )
            _, last_update_time = await self._execute_query(
                query=last_update_time_query
            )

            for column_name in columns:
                keys.append(column_name[0])

            for row in query_response:
                row = dict(zip(column_names, row))
                keys_value = ""
                for key in keys:
                    keys_value += f"{row.get(key)}_" if row.get(key) else ""
                row.update(
                    {
                        "_id": f"{database}_{table}_{keys_value}",
                        "timestamp": last_update_time[0][0],
                        "Database": database,
                        "Table": table,
                    }
                )
                yield self.serialize(doc=row)
        else:
            logger.warn(
                f"Skipping {table} table from database {database} since no primary key is associated with it. Assign primary key to the table to index it in the next sync interval."
            )

    async def _fetch_all_databases(self):
        """Fetches all user databases

        Returns:
            List: List of databases
        """

        # Query to get all databases
        query = QUERIES["ALL_DATABASE"]
        _, query_response = await self._execute_query(query=query)
        databases = [database[0] for database in query_response]
        return databases

    async def get_docs(self):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        databases = (
            self.configuration["database"]
            if self.configuration["database"]
            else await self._fetch_all_databases()
        )
        for database in databases:
            async for row in self.fetch_rows(database=database):
                yield row, None
