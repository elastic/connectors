#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import json
import os
from enum import Enum
from functools import cached_property
from tempfile import NamedTemporaryFile

import redis.asyncio as redis
from connectors_sdk.logger import logger

from connectors.utils import get_pem_format

PAGE_SIZE = 1000


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
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.mutual_tls_enabled = self.configuration["mutual_tls_enabled"]
        self.tls_certfile = self.configuration["tls_certfile"]
        self.tls_keyfile = self.configuration["tls_keyfile"]
        self._redis_client = None
        self.cert_file = ""
        self.key_file = ""

    def set_logger(self, logger_):
        self._logger = logger_

    def store_ssl_key(self, key, suffix):
        if suffix == ".key":
            pem_certificates = get_pem_format(
                key=key, postfix="-----END RSA PRIVATE KEY-----"
            )
        else:
            pem_certificates = get_pem_format(key=key)
        with NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as cert:
            cert.write(pem_certificates)
            return cert.name

    def remove_temp_files(self):
        for file_path in [self.cert_file, self.key_file]:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as exception:
                    self._logger.warning(
                        f"Something went wrong while removing temporary certificate file: {file_path}. Exception: {exception}",
                        exc_info=True,
                    )

    @cached_property
    def _client(self):
        if self.ssl_enabled and self.mutual_tls_enabled:
            self.cert_file = self.store_ssl_key(key=self.tls_certfile, suffix=".crt")
            self.key_file = self.store_ssl_key(key=self.tls_keyfile, suffix=".key")
            query = f"ssl_certfile={self.cert_file}&ssl_keyfile={self.key_file}"
            self._redis_client = redis.from_url(
                f"rediss://{self.username}:{self.password}@{self.host}:{self.port}?{query}",
                decode_responses=True,
            )
        elif self.ssl_enabled:
            self._redis_client = redis.from_url(
                f"rediss://{self.username}:{self.password}@{self.host}:{self.port}",
                decode_responses=True,
            )

        else:
            self._redis_client = redis.from_url(
                f"redis://{self.username}:{self.password}@{self.host}:{self.port}",
                decode_responses=True,
            )
        return self._redis_client

    async def close(self):
        if self._redis_client:
            await self._client.aclose()  # pyright: ignore
        self.remove_temp_files()

    async def validate_database(self, db):
        try:
            await self._client.execute_command("SELECT", db)
            return True
        except Exception as exception:
            self._logger.warning(f"Database {db} not found. Error: {exception}")
            return False

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
                databases = await self._client.config_get("databases")
                for database in list(range(int(databases.get("databases", 1)))):
                    yield database
            except Exception as exception:
                self._logger.warning(
                    f"Something went wrong while fetching database list. Error: {exception}",
                    exc_info=True,
                )

    async def get_paginated_key(self, db, pattern, type_=None):
        """Make a paginated call for fetching database keys.

        Args:
            db: Index of database
            pattern: keyname or pattern
            type_: Datatype for filter data

        Yields:
            string: key in database
        """
        await self._client.execute_command("SELECT", db)
        async for key in self._client.scan_iter(
            match=pattern, count=PAGE_SIZE, _type=type_
        ):
            yield key

    async def get_key_value(self, key, key_type):
        """Fetch value of key for database.

        Args:
            key: Name of key
            key_type: Type of key

        Yields:
            Value of key
        """
        try:
            if key_type == KeyType.HASH.value:
                return await self._client.hgetall(key)
            elif key_type == KeyType.STREAM.value:
                return await self._client.xread({key: "0"})
            elif key_type == KeyType.LIST.value:
                return await self._client.lrange(key, 0, -1)
            elif key_type == KeyType.SET.value:
                return await self._client.smembers(key)
            elif key_type == KeyType.ZSET.value:
                return await self._client.zrange(key, 0, -1, withscores=True)
            elif key_type == KeyType.JSON.value:
                value = await self._client.execute_command("JSON.GET", key)
                return json.loads(value)
            else:
                value = await self._client.get(key)
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

    async def get_key_metadata(self, key):
        key_type = await self._client.type(key)
        key_value = await self.get_key_value(key=key, key_type=key_type)
        key_size = await self._client.memory_usage(key)
        return key_type, key_value, key_size

    async def ping(self):
        await self._client.ping()
