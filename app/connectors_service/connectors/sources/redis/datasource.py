#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from connectors_sdk.source import BaseDataSource, ConfigurableFieldValueError
from connectors_sdk.utils import (
    hash_id,
    iso_utc,
)

from connectors.sources.redis.client import RedisClient
from connectors.sources.redis.validator import RedisAdvancedRulesValidator


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
