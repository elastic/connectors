#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import json
from contextlib import asynccontextmanager
from enum import Enum

import redis.asyncio as redis

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import hash_id, iso_utc

PAGE_SIZE = 1000
MAX_POOL_SIZE = 10


class KeyType(Enum):
    HASH = "hash"
    STREAM = "stream"
    LIST = "list"
    SET = "set"
    ZSET = "zset"
    JSON = "ReJSON-RL"


class RedisClient:
    """Redis client to handle method calls made to Redis"""

    def __init__(self, configuration):
        self.configuration = configuration
        self._logger = logger
        self.host = self.configuration["host"]
        self.port = self.configuration["port"]
        self.database = self.configuration["database"]
        self.username = self.configuration["username"]
        self.password = self.configuration["password"]

    def set_logger(self, logger_):
        self._logger = logger_

    @asynccontextmanager
    async def client(self):
        try:
            pool = redis.ConnectionPool.from_url(
                f"redis://{self.username}:{self.password}@{self.host}:{self.port}",
                decode_responses=True,
                max_connections=MAX_POOL_SIZE,
            )
            client = redis.Redis(connection_pool=pool)
            yield client
        finally:
            await client.aclose()  # pyright: ignore

    async def validate_database(self, db, redis_client):
        try:
            await redis_client.execute_command("SELECT", db)
            return await redis_client.ping()
        except Exception as exception:
            self._logger.warning(f"Database {db} not found. Error: {exception}")

    async def get_databases(self):
        """Returns number of databases from config_get response

        Returns:
            list: List of databases
        """
        if self.database != ["*"]:
            for database in set(self.database):
                yield database
        else:
            try:
                async with self.client() as redis_client:
                    databases = await redis_client.config_get("databases")
                    for database in list(range(int(databases.get("databases", 1)))):
                        yield database
            except Exception as exception:
                self._logger.warning(
                    f"Something went wrong while fetching database list. Error: {exception}",
                    exc_info=True,
                )

    async def get_paginated_key(self, redis_client, pattern, _type=None):
        """Make a paginated call for fetching database keys.

        Args:
            redis_client: redis client object
            pattern: keyname or pattern
            _type: Datatype for filter data

        Yields:
            string: key in database
        """
        async for key in redis_client.scan_iter(
            match=pattern, count=PAGE_SIZE, _type=_type
        ):
            yield key

    async def get_key_value(self, redis_client, key, key_type):
        """Fetch value of key for database.

        Args:
            redis_client: redis client object
            key: Name of key
            key_type: Type of key

        Yields:
            Value of key
        """
        try:
            if key_type == KeyType.HASH.value:
                return await redis_client.hgetall(key)
            elif key_type == KeyType.STREAM.value:
                return await redis_client.xread({key: "0"})
            elif key_type == KeyType.LIST.value:
                return await redis_client.lrange(key, 0, -1)
            elif key_type == KeyType.SET.value:
                return await redis_client.smembers(key)
            elif key_type == KeyType.ZSET.value:
                return await redis_client.zrange(key, 0, -1, withscores=True)
            elif key_type == KeyType.JSON.value:
                value = await redis_client.execute_command("JSON.GET", key)
                return json.loads(value)
            else:
                value = await redis_client.get(key)
                if value is not None:
                    try:
                        value_json = json.loads(value)
                        return value_json
                    except json.JSONDecodeError:
                        return value
        except UnicodeDecodeError as exception:
            self._logger.warning(
                f"Something went wrong while fetching value for key {key}. Error: {exception}",
                exc_info=True,
            )
            return ""


class RedisDataSource(BaseDataSource):
    """Redis"""

    name = "Redis"
    service_type = "redis"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self.redis_client = RedisClient(configuration=configuration)

    @classmethod
    def get_default_configuration(cls):
        return {
            "host": {"label": "Host", "order": 1, "type": "str"},
            "port": {"label": "Port", "order": 2, "type": "int"},
            "username": {
                "label": "Username",
                "order": 3,
                "required": False,
                "type": "str",
            },
            "password": {
                "label": "Password",
                "order": 4,
                "required": False,
                "sensitive": True,
                "type": "str",
            },
            "database": {
                "display": "textarea",
                "label": "Comma-separated list of databases",
                "order": 5,
                "tooltip": "Databases are ignored when Advanced Sync Rules are used.",
                "type": "list",
            },
        }

    def _set_internal_logger(self):
        self.redis_client.set_logger(self._logger)

    async def _remote_validation(self):
        """Validate configured databases
        Raises:
            ConfigurableFieldValueError: Unavailable services error.
        """
        invalid_type = []
        invalid_db = []
        msg = ""
        if self.redis_client.database != ["*"]:
            async with self.redis_client.client() as redis_client:
                for db in self.redis_client.database:
                    if not db.isdigit() or int(db) < 0:
                        invalid_type.append(db)
                    check_db = await self.redis_client.validate_database(
                        db=db, redis_client=redis_client
                    )
                    if not check_db:
                        invalid_db.append(db)
                if invalid_type and invalid_db:
                    msg = f"Database {', '.join(invalid_db)} are not available. Also database element should be integer. Please correct database name: {', '.join(invalid_type)}"
                elif invalid_db:
                    msg = f"Database {', '.join(invalid_db)} are not available."
                elif invalid_type:
                    msg = f"All database element should be integer. Please correct database name: {', '.join(invalid_type)}"
                if msg:
                    raise ConfigurableFieldValueError(msg)

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured databases are available in Redis."""
        await super().validate_config()
        await self._remote_validation()

    async def format_document(self, **kwargs):
        """Prepare document for database records.

        Returns:
            document: Modified document.
        """
        doc_id = hash_id(f"{kwargs.get('db')}/{kwargs.get('key')}")
        document = {
            "_id": doc_id,
            "key": kwargs.get("key"),
            "value": str(kwargs.get("value")),
            "size_in_bytes": kwargs.get("size"),
            "database": kwargs.get("db"),
            "key_type": kwargs.get("key_type"),
            "_timestamp": iso_utc(),
        }
        return document

    async def ping(self):
        try:
            async with self.redis_client.client() as redis_client:
                await redis_client.ping()
                self._logger.info("Successfully connected to Redis.")
        except Exception:
            self._logger.exception("Error while connecting to Redis.")
            raise

    async def get_key_metadata(self, redis_client, key, db):
        key_type = await redis_client.type(key)
        key_value = await self.redis_client.get_key_value(
            redis_client=redis_client, key=key, key_type=key_type
        )
        key_size = await redis_client.memory_usage(key)
        return await self.format_document(
            key=key,
            value=key_value,
            key_type=key_type,
            size=key_size,
            db=db,
        )

    async def get_db_records(self, db, redis_client, pattern="*", _type=None):
        await redis_client.execute_command("SELECT", db)
        async for key in self.redis_client.get_paginated_key(
            redis_client=redis_client, pattern=pattern, _type=_type
        ):
            document = await self.get_key_metadata(
                redis_client=redis_client, key=key, db=db
            )
            yield document

    async def get_docs(self, filtering=None):
        """Get documents from Redis

        Returns:
            dictionary: Document of database content

        Yields:
            dictionary: Document from Redis.
        """
        async with self.redis_client.client() as redis_client:
            async for db in self.redis_client.get_databases():
                async for document in self.get_db_records(
                    db=db, redis_client=redis_client
                ):
                    yield document, None
