#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""MySQL source module responsible to fetch documents from MySQL"""
import asyncio
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
DEFAULT_FETCH_SIZE = 50
DEFAULT_RETRY_COUNT = 3
DEFAULT_WAIT_MULTIPLIER = 2


class MySqlDataSource(BaseDataSource):
    """Class to fetch and modify documents from MySQL server"""

    def __init__(self, connector):
        """Setup connection to the MySQL server.

        Args:
            connector (BYOConnector): Object of the BYOConnector class
        """
        super().__init__(connector=connector)
        self.retry_count = int(
            self.configuration.get("retry_count", DEFAULT_RETRY_COUNT)
        )
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
                "label": "Host",
                "type": "str",
            },
            "port": {
                "value": 3306,
                "label": "Port",
                "type": "int",
            },
            "user": {
                "value": "root",
                "label": "Username",
                "type": "str",
            },
            "password": {
                "value": "changeme",
                "label": "Password",
                "type": "str",
            },
            "database": {
                "value": ["customerinfo"],
                "label": "Databases",
                "type": "list",
            },
            "connector_name": {
                "value": "MySQL Connector",
                "label": "Friendly name for the connector",
                "type": "str",
            },
            "fetch_size": {
                "value": DEFAULT_FETCH_SIZE,
                "label": "How many rows to fetch on each call",
                "type": "int",
            },
            "retry_count": {
                "value": DEFAULT_RETRY_COUNT,
                "label": "How many retry count for fetching rows on each call",
                "type": "int",
            },
        }

    async def close(self):
        if self.connection_pool is None:
            return
        self.connection_pool.close()
        await self.connection_pool.wait_closed()
        self.connection_pool = None

    def _validate_configuration(self):
        """Validates whether user input is empty or not for configuration fields and validate type for port

        Raises:
            Exception: Configured keys can't be empty
        """
        connection_fields = ["host", "port", "user", "password", "database"]
        empty_connection_fields = []
        for field in connection_fields:
            if self.configuration[field] == "":
                empty_connection_fields.append(field)

        if empty_connection_fields:
            raise Exception(
                f"Configured keys: {empty_connection_fields} can't be empty."
            )

        if (
            isinstance(self.configuration["port"], str)
            and not self.configuration["port"].isnumeric()
        ):
            raise Exception("Configured port has to be an integer.")

    async def ping(self):
        """Verify the connection with MySQL server"""
        logger.info("Validating MySQL Configuration...")
        self._validate_configuration()
        connection_string = {
            "host": self.configuration["host"],
            "port": int(self.configuration["port"]),
            "user": self.configuration["user"],
            "password": self.configuration["password"],
            "db": None,
            "maxsize": MAX_POOL_SIZE,
        }
        logger.info("Pinging MySQL...")
        if self.connection_pool is None:
            self.connection_pool = await aiomysql.create_pool(**connection_string)
        try:
            async with self.connection_pool.acquire() as connection:
                await connection.ping()
                logger.info("Successfully connected to the MySQL Server.")
        except Exception:
            logger.exception("Error while connecting to the MySQL Server.")
            raise

    async def _stream_rows(self, database, table, query):
        """Executes the passed query on the MySQL server.

        Args:
            database (str): Name of database
            table (str): Name of table
            query (str): MySql query to be executed.

        Yields:
            list: Column names and query response
        """

        size = int(self.configuration.get("fetch_size", DEFAULT_FETCH_SIZE))
        logger.debug(f"Streaming {database}.{table} {size} rows at a time")

        # retry: Current retry counter
        # yield_once: Yield(fields) once flag
        retry = yield_once = 1

        # rows_fetched: Rows fetched counter
        # cursor_position: Mocked cursor position
        rows_fetched = cursor_position = 0
        while retry <= self.retry_count:
            try:
                async with self.connection_pool.acquire() as connection:
                    async with connection.cursor(aiomysql.cursors.SSCursor) as cursor:
                        await cursor.execute(query)

                        # sending back column names only once
                        if yield_once:
                            yield [column[0] for column in cursor.description]
                            yield_once = 0

                        # setting cursor position where it was failed
                        if cursor_position:
                            await cursor.scroll(cursor_position, mode="absolute")
                            logger.debug(
                                f"Cursor moved to {cursor_position} position for {database}.{table}"
                            )

                        while True:
                            rows = await cursor.fetchmany(size=size)
                            rows_length = len(rows)

                            # resetting cursor position & retry to 0 for next batch
                            if cursor_position:
                                cursor_position = retry = 0

                            if not rows_length:
                                break

                            for row in rows:
                                yield row

                            rows_fetched += rows_length
                            await asyncio.sleep(0)
                        break
            except IndexError as exception:
                logger.exception(
                    f"None of rows fetched from {rows_fetched} rows in {database}.{table}. Exception: {exception}"
                )
                break
            except Exception as exception:
                logger.warn(
                    f"Retry count: {retry} out of {self.retry_count} for rows in {database}.{table}. Exception: {exception}"
                )
                if retry == self.retry_count:
                    raise exception
                cursor_position = rows_fetched
                await asyncio.sleep(DEFAULT_WAIT_MULTIPLIER**retry)
                retry += 1

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
                async for row in self.fetch_documents(
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

    async def fetch_documents(self, database, table, query):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            database (str): Name of database
            table (str): Name of table
            query (str): Query to execute

        Yields:
            Dict: Document to be index
        """

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

            query = QUERIES["TABLE_DATA"].format(database=database, table=table)
            streamer = self._stream_rows(database, table, query=query)
            column_names = await streamer.__anext__()
            for column_name in columns:
                keys.append(column_name[0])

            async for row in streamer:
                row = dict(zip(column_names, row))
                keys_value = ""
                for key in keys:
                    keys_value += f"{row.get(key)}_" if row.get(key) else ""
                row.update(
                    {
                        "_id": f"{database}_{table}_{keys_value}",
                        "_timestamp": last_update_time[0][0],
                        "Database": database,
                        "Table": table,
                    }
                )
                yield self.serialize(doc=row)
        else:
            logger.warn(
                f"Skipping {table} table from database {database} since no primary key is associated with it. Assign primary key to the table to index it in the next sync interval."
            )

    async def _validate_databases(self, databases):
        """Validates all user input databases

        Args:
            databases (list): User input databases

        Returns:
            List: List of invalid databases
        """

        # Query to get all databases
        query = QUERIES["ALL_DATABASE"]
        _, query_response = await self._execute_query(query=query)
        accessible_databases = [database[0] for database in query_response]
        return list(set(databases) - set(accessible_databases))

    async def get_docs(self):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        database_config = self.configuration["database"]
        if isinstance(database_config, str):
            dbs = database_config.split(",")
            databases = list(map(lambda s: s.strip(), dbs))
        else:
            databases = database_config

        inaccessible_databases = await self._validate_databases(databases=databases)
        if inaccessible_databases:
            raise Exception(
                f"Configured databases: {inaccessible_databases} are inaccessible for user {self.configuration['user']}."
            )

        for database in databases:
            async for row in self.fetch_rows(database=database):
                yield row, None
            await asyncio.sleep(0)
