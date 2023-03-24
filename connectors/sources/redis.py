from contextlib import asynccontextmanager

import aioredis
from connectors.source import BaseDataSource

WILDCARD = "*"


async def _get_key_patterns(filtering):
    if filtering and filtering.has_advanced_rules():
        key_patterns = filtering.get_advanced_rules()
    else:
        key_patterns = [WILDCARD]
    return key_patterns


class RedisDataSource(BaseDataSource):
    """Redis"""

    name = "Redis"
    service_type = "redis"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

        self.host = self.configuration["host"]
        self.port = self.configuration["port"]
        self.db = self.configuration["db"]
        self.username = self.configuration["username"]
        self.password = self.configuration["password"]
        self.key_pattern = self.configuration["key_pattern"]

    @classmethod
    def get_default_configuration(cls):
        return {
            "host": {
                "value": "localhost",
                "label": "Host",
                "type": "str",
            },
            "port": {
                "value": 6379,
                "label": "Port",
                "type": "int",
            },
            "username": {
                "value": "username",
                "label": "Username",
                "type": "str"
            },
            "password": {
                "value": "password",
                "label": "Password",
                "type": "str"
            },
            "db": {
                "value": 0,
                "label": "Database",
                "type": "int",
            },
            "key_pattern": {
                "value": "*",
                "label": "Key Pattern",
                "type": "str",
            },
        }

    @asynccontextmanager
    async def _redis_client(self):
        async with aioredis.from_url(f"redis://{self.username}:{self.password}@{self.host}:{self.port}/{self.db}") as redis:
            yield redis

    async def ping(self):
        async with self._redis_client() as redis:
            return await redis.ping()

    async def close(self):
        # No action required as the connection is closed automatically by aioredis's context manager
        pass

    async def get_docs(self, filtering=None):
        async with self._redis_client() as redis:
            key_patterns = await _get_key_patterns(filtering)

            for pattern in key_patterns:
                keys = await redis.keys(pattern)

                for key in keys:
                    value = await redis.get(key)
                    doc = {"_id": key, "value": value}
                    yield self.serialize(doc), None

    async def validate_config(self):
        non_empty_fields = ["host", "password", "key_pattern"]

        for field in non_empty_fields:
            if not self.configuration[field]:
                raise ValueError(f"{field} field must not be empty.")

        if not isinstance(self.configuration["port"], int) or self.configuration["port"] < 1:
            raise ValueError("port must be a positive integer")

        if not isinstance(self.configuration["db"], int) or self.configuration["db"] < 0:
            raise ValueError("db must be a non-negative integer")
