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

import fastjsonschema
import redis.asyncio as redis

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import get_pem_format, hash_id, iso_utc

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


class RedisAdvancedRulesValidator(AdvancedRulesValidator):
    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "database": {"type": "integer", "minLength": 1, "minimum": 0},
            "key_pattern": {"type": "string", "minLength": 1},
            "type": {
                "type": "string",
                "minLength": 1,
            },
        },
        "required": ["database"],
        "anyOf": [
            {"required": ["key_pattern"]},
            {"required": ["type"]},
        ],
        "additionalProperties": False,
    }

    SCHEMA_DEFINITION = {"type": "array", "items": RULES_OBJECT_SCHEMA_DEFINITION}

    SCHEMA = fastjsonschema.compile(definition=SCHEMA_DEFINITION)

    def __init__(self, source):
        self.source = source

    async def validate(self, advanced_rules):
        if len(advanced_rules) == 0:
            return SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            )
        return await self._remote_validation(advanced_rules)

    async def _remote_validation(self, advanced_rules):
        try:
            RedisAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )
        invalid_db = []
        database_to_filter = [rule["database"] for rule in advanced_rules]
        for db in database_to_filter:
            check_db = await self.source.client.validate_database(db=db)
            if not check_db:
                invalid_db.append(db)
        if invalid_db:
            msg = f"Database {','.join(map(str, invalid_db))} are not available."
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=msg,
            )
        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


class RedisDataSource(BaseDataSource):
    """Redis"""

    name = "Redis"
    service_type = "redis"
    advanced_rules_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self.client = RedisClient(configuration=configuration)

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
                "value": "*",
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "SSL/TLS Connection",
                "order": 6,
                "tooltip": "This option establishes a secure connection to Redis using SSL/TLS encryption. Ensure that your Redis deployment supports SSL/TLS connections.",
                "type": "bool",
                "value": False,
            },
            "mutual_tls_enabled": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "display": "toggle",
                "label": "Mutual SSL/TLS Connection",
                "order": 7,
                "tooltip": "This option establishes a secure connection to Redis using mutual SSL/TLS encryption. Ensure that your Redis deployment supports mutual SSL/TLS connections.",
                "type": "bool",
                "value": False,
            },
            "tls_certfile": {
                "depends_on": [{"field": "mutual_tls_enabled", "value": True}],
                "label": "client certificate file for SSL/TLS",
                "order": 8,
                "required": False,
                "tooltip": "Specifies the client certificate from the Certificate Authority. The value of the certificate is used to validate the certificate presented by the Redis instance.",
                "type": "str",
            },
            "tls_keyfile": {
                "depends_on": [{"field": "mutual_tls_enabled", "value": True}],
                "label": "client private key file for SSL/TLS",
                "order": 9,
                "required": False,
                "tooltip": "Specifies the client private key from the Certificate Authority. The value of the key is used to validate the connection in the Redis instance.",
                "type": "str",
            },
        }

    def _set_internal_logger(self):
        self.client.set_logger(self._logger)

    def advanced_rules_validators(self):
        return [RedisAdvancedRulesValidator(self)]

    async def close(self):
        await self.client.close()

    async def _remote_validation(self):
        """Validate configured databases
        Raises:
            ConfigurableFieldValueError: Unavailable services error.
        """
        invalid_type = []
        invalid_db = []
        msg = ""
        if self.client.database != ["*"]:
            try:
                await self.client.ping()
            except Exception:
                self._logger.exception("Error while connecting to Redis.")
                raise
            for db in self.client.database:
                if not db.isdigit() or int(db) < 0:
                    invalid_type.append(db)
                    continue
                check_db = await self.client.validate_database(db=db)
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
            await self.client.ping()
            self._logger.info("Successfully connected to Redis.")
        except Exception:
            self._logger.exception("Error while connecting to Redis.")
            raise

    async def get_db_records(self, db, pattern="*", type_=None):
        async for key in self.client.get_paginated_key(
            db=db, pattern=pattern, type_=type_
        ):
            key_type, key_value, key_size = await self.client.get_key_metadata(key=key)
            yield await self.format_document(
                key=key,
                value=key_value,
                key_type=key_type,
                size=key_size,
                db=db,
            )

    async def get_docs(self, filtering=None):
        """Get documents from Redis

        Returns:
            dictionary: Document of database content

        Yields:
            dictionary: Document from Redis.
        """
        if filtering and filtering.has_advanced_rules():
            for rule in filtering.get_advanced_rules():
                async for document in self.get_db_records(
                    db=rule.get("database"),
                    pattern=rule.get("key_pattern"),
                    type_=rule.get("type"),
                ):
                    yield document, None
        else:
            async for db in self.client.get_databases():
                async for document in self.get_db_records(db=db):
                    yield document, None
