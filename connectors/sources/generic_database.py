import asyncio
from datetime import date, datetime
from decimal import Decimal
from functools import partial

from asyncpg.exceptions._base import InternalClientError
from bson import Decimal128
from sqlalchemy import text

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import iso_utc

DEFAULT_FETCH_SIZE = 50
DEFAULT_RETRY_COUNT = 3
DEFAULT_WAIT_MULTIPLIER = 2


class GenericBaseDataSource(BaseDataSource):
    """Class contains common functionalities for Generic Database connector"""

    def __init__(self, connector):
        """Setup connection to the database-server configured by user

        Args:
            connector (BYOConnector): Object of the BYOConnector class
        """
        super().__init__(connector=connector)

        # Connector configurations
        self.retry_count = int(
            self.configuration.get("retry_count", DEFAULT_RETRY_COUNT)
        )

        # Connection related configurations
        self.user = self.configuration["user"]
        self.password = self.configuration["password"]
        self.host = self.configuration["host"]
        self.port = self.configuration["port"]
        self.database = self.configuration["database"]
        self.connection = None
        self.queries = None

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for database-server configured by user

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
                "value": 9090,
                "label": "Port",
                "type": "int",
            },
            "user": {
                "value": "admin",
                "label": "Username",
                "type": "str",
            },
            "password": {
                "value": "Password_123",
                "label": "Password",
                "type": "str",
            },
            "database": {
                "value": "xe",
                "label": "Databases",
                "type": "str",
            },
            "connector_name": {
                "value": "Generic Database",
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

    def _validate_configuration(self):
        """Validates the configuration parameters

        Raises:
            Exception: Configured keys can't be empty
        """
        connection_fields = [
            "host",
            "port",
            "user",
            "password",
            "database",
        ]

        if empty_connection_fields := [
            field for field in connection_fields if self.configuration[field] == ""
        ]:
            raise Exception(
                f"Configured keys: {empty_connection_fields} can't be empty."
            )

        if (
            isinstance(self.configuration["port"], str)
            and not self.configuration["port"].isnumeric()
        ):
            raise Exception("Configured port has to be an integer.")

    async def execute_query(
        self, query_name, engine, fetch_many=False, is_async=True, **query_kwargs
    ):
        """Executes a query and yield rows

        Args:
            query_name (str): Name of query.
            engine (Engine): Database engine to be used.
            fetch_many (bool): Flag to use fetchmany method. Defaults to False.
            is_async (bool, optional): Flag to indicates async/sync execution. Defaults to True.

        Raises:
            exception: Raise an exception after retrieving

        Yields:
            list: Column names and query response
        """
        query = self.queries[query_name].format(**query_kwargs)
        size = int(self.configuration.get("fetch_size", DEFAULT_FETCH_SIZE))

        # retry: Current retry counter
        # yield_once: Yield(fields) once flag
        retry = yield_once = 1

        # rows_fetched: Rows fetched counter
        rows_fetched = 0

        while retry <= self.retry_count:
            try:
                if is_async:
                    cursor = await self._async_connect(query=query, engine=engine)
                else:
                    cursor = await self._sync_connect(query=query, engine=engine)
                if fetch_many:
                    # sending back column names only once
                    if yield_once:
                        if query_kwargs["schema"]:
                            yield [
                                f"{query_kwargs['schema']}_{query_kwargs['table']}_{column}".lower()
                                for column in cursor.keys()
                            ]
                        else:
                            yield [
                                f"{query_kwargs['table']}_{column}".lower()
                                for column in cursor.keys()
                            ]
                        yield_once = False

                    while True:
                        rows = cursor.fetchmany(size=size)
                        rows_length = len(rows)

                        if not rows_length:
                            break

                        for row in rows:
                            yield row

                        rows_fetched += rows_length
                        await asyncio.sleep(0)
                else:
                    yield cursor.fetchall()
                break
            except InternalClientError:
                raise
            except Exception as exception:
                logger.warning(
                    f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}"
                )
                if retry == self.retry_count:
                    raise exception
                await asyncio.sleep(DEFAULT_WAIT_MULTIPLIER**retry)
                retry += 1

    async def _async_connect(self, query, engine):
        """Execute the passed query on the Async supported Database server and return cursor.

        Args:
            query (str): Database query to be executed.
            engine (str): Database engine to be used.

        Returns:
            cursor: Asynchronous cursor
        """
        try:
            async with engine.connect() as connection:
                cursor = await connection.execute(text(query))
                return cursor
        except Exception as exception:
            logger.warning(
                f"Something went wrong while executing query. Exception: {exception}"
            )
            raise

    async def close(self):
        """Close the connection to the database server."""
        if self.connection is None:
            return
        self.connection.close()

    async def _sync_connect(self, query, engine):
        """Executes the passed query on the Non-Async supported Database server and return cursor.

        Args:
            query (str): Database query to be executed.
            engine (Engine): Engine to connect with the Database server

        Returns:
            cursor: Synchronous cursor
        """
        try:
            loop = asyncio.get_running_loop()
            self.connection = await loop.run_in_executor(
                executor=None, func=engine.connect
            )
            cursor = await loop.run_in_executor(
                executor=None, func=partial(self.connection.execute, statement=query)
            )
            return cursor
        except Exception as exception:
            logger.warning(
                f"Something went wrong while executing query. Exception: {exception}"
            )
            raise

    def serialize(self, doc):
        """Reads each element from the document and serialize it as per it's datatype.

        Args:
            doc (Dict): Dictionary to be serialize

        Returns:
            doc (Dict): Serialized version of dictionary
        """

        def _serialize(value):
            """Serialize input value as per it's datatype.
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

    async def fetch_documents(self, table, engine, is_async=True, schema=None):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            table (str): Name of table
            engine (Engine): Engine to connect with the Database server
            is_async (bool, optional): Flag to indicates async/sync execution. Defaults to True.
            schema (str): Name of schema. Defaults to None.

        Yields:
            Dict: Document to be indexed
        """
        try:
            [[row_count]] = await anext(
                self.execute_query(
                    query_name="TABLE_DATA_COUNT",
                    engine=engine,
                    is_async=is_async,
                    schema=schema,
                    table=table,
                )
            )
            if row_count:
                # Query to get the table's primary key
                columns = await anext(
                    self.execute_query(
                        query_name="TABLE_PRIMARY_KEY",
                        engine=engine,
                        is_async=is_async,
                        database=self.database,
                        schema=schema,
                        table=table,
                    )
                )
                if schema:
                    keys = [
                        f"{schema}_{table}_{column_name}"
                        for [column_name] in columns
                        if column_name
                    ]
                else:
                    keys = [
                        f"{table}_{column_name}"
                        for [column_name] in columns
                        if column_name
                    ]
                if keys:
                    try:
                        last_update_time = await anext(
                            self.execute_query(
                                query_name="TABLE_LAST_UPDATE_TIME",
                                engine=engine,
                                is_async=is_async,
                                database=self.database,
                                schema=schema,
                                table=table,
                            )
                        )
                        last_update_time = last_update_time[0][0]
                    except Exception:
                        logger.warning(f"Unable to fetch last_updated_time for {table}")
                        last_update_time = None
                    streamer = self.execute_query(
                        query_name="TABLE_DATA",
                        engine=engine,
                        fetch_many=True,
                        is_async=is_async,
                        schema=schema,
                        table=table,
                    )
                    column_names = await anext(streamer)
                    async for row in streamer:
                        row = dict(zip(column_names, row))
                        keys_value = ""
                        for key in keys:
                            keys_value += (
                                f"{row.get(key.lower())}_"
                                if row.get(key.lower())
                                else ""
                            )
                        row.update(
                            {
                                "_id": f"{self.database}_{schema}_{table}_{keys_value}"
                                if schema
                                else f"{self.database}_{table}_{keys_value}",
                                "_timestamp": last_update_time or iso_utc(),
                                "Database": self.database,
                                "Table": table,
                            }
                        )
                        if schema:
                            row["schema"] = schema
                        yield self.serialize(doc=row)
                else:
                    logger.warning(
                        f"Skipping {table} table from database {self.database} since no primary key is associated with it. Assign primary key to the table to index it in the next sync interval."
                    )
            else:
                logger.warning(f"No rows found for {table}.")
        except InternalClientError as exception:
            logger.warning(
                f"Something went wrong while fetching document for table {table}. Error: {exception}"
            )

    async def fetch_rows(self, engine, is_async=True, schema=None):
        """Fetches all the rows from all the tables of the database.

        Args:
            engine (Engine): Engine to connect with the Database server
            is_async (bool, optional): Flag to indicates async/sync execution. Defaults to True.
            schema (str): Name of schema. Defaults to None.

        Yields:
            Dict: Row document to index
        """
        # Query to get all table names from a database
        list_of_tables = await anext(
            self.execute_query(
                query_name="ALL_TABLE",
                engine=engine,
                is_async=is_async,
                user=self.user.upper(),
                database=self.database,
                schema=schema,
            )
        )
        if list_of_tables:
            for [table_name] in list_of_tables:
                logger.debug(f"Found table: {table_name} in database: {self.database}.")
                async for row in self.fetch_documents(
                    table=table_name,
                    engine=engine,
                    is_async=is_async,
                    schema=schema,
                ):
                    yield row
        else:
            if schema:
                logger.warning(
                    f"Fetched 0 tables for schema: {schema} and database: {self.database}"
                )
            else:
                logger.warning(f"Fetched 0 tables for the database: {self.database}")
