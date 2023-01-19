#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""MySQL source module responsible to fetch documents from MySQL"""
import asyncio
import ssl
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
DEFAULT_SSL_DISABLED = True
DEFAULT_SSL_CA = None


class MySqlDataSource(BaseDataSource):
    """MySQL"""

    def __init__(self, configuration):
        """Set up the connection to the MySQL server.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.retry_count = self.configuration["retry_count"]
        self.connection_pool = None
        self.ssl_disabled = self.configuration["ssl_disabled"]
        self.certificate = self.configuration["ssl_ca"]

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
                "value": "customerinfo",
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
            "ssl_disabled": {
                "value": DEFAULT_SSL_DISABLED,
                "label": "Disable SSL verification",
                "type": "bool",
            },
            "ssl_ca": {
                "value": DEFAULT_SSL_CA,
                "label": "SSL certificate",
                "type": "str",
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

        if not (self.ssl_disabled or self.certificate):
            raise Exception("SSL certificate must be configured.")

    def _ssl_context(self, certificate):
        """Convert string to pem format and create a SSL context

        Args:
            certificate (str): certificate in string format

        Returns:
            ssl_context: SSL context with certificate
        """
        certificate = certificate.replace(" ", "\n")
        pem_format = " ".join(certificate.split("\n", 1))
        pem_format = " ".join(pem_format.rsplit("\n", 1))
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(cadata=pem_format)
        return ctx

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
            "ssl": self._ssl_context(certificate=self.certificate)
            if not self.ssl_disabled
            else None,
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

    async def _connect(self, query_name, fetch_many=False, **query_kwargs):
        """Executes the passed query on the MySQL server.

        Args:
            query_name (str): MySql query name to be executed.
            query_kwargs (dict): Query kwargs to format the query.
            fetch_many (boolean): Should use fetchmany to fetch the response.

        Yields:
            list: Column names and query response
        """

        query = QUERIES[query_name].format(**query_kwargs)
        size = int(self.configuration.get("fetch_size", DEFAULT_FETCH_SIZE))

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

                        if fetch_many:
                            # sending back column names only once
                            if yield_once:
                                yield [
                                    f"{query_kwargs['database']}_{query_kwargs['table']}_{column[0]}"
                                    for column in cursor.description
                                ]
                                yield_once = False

                            # setting cursor position where it was failed
                            if cursor_position:
                                await cursor.scroll(cursor_position, mode="absolute")

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
                        else:
                            yield await cursor.fetchall()
                        break

            except IndexError as exception:
                logger.exception(
                    f"None of responses fetched from {rows_fetched} rows. Exception: {exception}"
                )
                break
            except Exception as exception:
                logger.warning(
                    f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}"
                )
                if retry == self.retry_count:
                    raise exception
                cursor_position = rows_fetched
                await asyncio.sleep(DEFAULT_WAIT_MULTIPLIER**retry)
                retry += 1

    async def fetch_rows(self, database):
        """Fetches all the rows from all the tables of the database.

        Args:
            database (str): Name of database

        Yields:
            Dict: Row document to index
        """
        # Query to get all table names from a database
        response = await anext(self._connect(query_name="ALL_TABLE", database=database))

        if response:
            for table in response:
                table_name = table[0]
                logger.debug(f"Found table: {table_name} in database: {database}.")

                async for row in self.fetch_documents(
                    database=database, table=table_name
                ):
                    yield row
        else:
            logger.warning(
                f"Fetched 0 tables for the database: {database}. As database has no tables."
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

    async def fetch_documents(self, database, table):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            database (str): Name of database
            table (str): Name of table

        Yields:
            Dict: Document to be index
        """

        # Query to get the table's primary key
        response = await anext(
            self._connect(
                query_name="TABLE_PRIMARY_KEY", database=database, table=table
            )
        )

        keys = []
        for column_name in response:
            keys.append(f"{database}_{table}_{column_name[0]}")

        if keys:

            # Query to get the table's last update time
            response = await anext(
                self._connect(
                    query_name="TABLE_LAST_UPDATE_TIME", database=database, table=table
                )
            )
            last_update_time = response[0][0]

            # Query to get the table's data
            streamer = self._connect(
                query_name="TABLE_DATA", fetch_many=True, database=database, table=table
            )
            column_names = await anext(streamer)

            async for row in streamer:
                row = dict(zip(column_names, row))
                keys_value = ""
                for key in keys:
                    keys_value += f"{row.get(key)}_" if row.get(key) else ""
                row.update(
                    {
                        "_id": f"{database}_{table}_{keys_value}",
                        "_timestamp": last_update_time,
                        "Database": database,
                        "Table": table,
                    }
                )
                yield self.serialize(doc=row)
        else:
            logger.warning(
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
        response = await anext(self._connect(query_name="ALL_DATABASE"))
        accessible_databases = [database[0] for database in response]
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
