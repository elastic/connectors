#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import urllib.parse
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
from tempfile import NamedTemporaryFile

import fastjsonschema
from bson import DBRef, Decimal128, ObjectId
from fastjsonschema import JsonSchemaValueException
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import OperationFailure

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import get_pem_format


class MongoAdvancedRulesValidator(AdvancedRulesValidator):
    """
    Validate advanced rules for MongoDB, so that they're adhering to the motor asyncio API (see: https://motor.readthedocs.io/en/stable/api-asyncio/asyncio_motor_collection.html)
    """

    # see: https://motor.readthedocs.io/en/stable/api-asyncio/asyncio_motor_collection.html#motor.motor_asyncio.AsyncIOMotorCollection.aggregate
    AGGREGATE_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "allowDiskUse": {"type": "boolean"},
            "maxTimeMS": {"type": "integer"},
            "batchSize": {"type": "integer"},
            "let": {"type": "object"},
            "pipeline": {"type": "array", "minItems": 1},
        },
        "additionalProperties": False,
    }

    # see: https://pymongo.readthedocs.io/en/4.3.2/api/pymongo/collection.html#pymongo.collection.Collection.find
    FIND_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "filter": {"type": "object"},
            "projection": {"type": ["array", "object"], "minItems": 1},
            "skip": {"type": "integer"},
            "limit": {"type": "integer"},
            "no_cursor_timeout": {"type": "boolean"},
            "allow_partial_results": {"type": "boolean"},
            "batch_size": {"type": "integer"},
            "return_key": {"type": "boolean"},
            "show_record_id": {"type": "boolean"},
            "max_time_ms": {"type": "integer"},
            "allow_disk_use": {"type": "boolean"},
        },
        "additionalProperties": False,
    }

    SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "aggregate": AGGREGATE_SCHEMA_DEFINITION,
            "find": FIND_SCHEMA_DEFINITION,
        },
        "additionalProperties": False,
        # at most one property -> only "aggregate" OR "find" allowed
        "maxProperties": 1,
    }

    SCHEMA = fastjsonschema.compile(definition=SCHEMA_DEFINITION)

    async def validate(self, advanced_rules):
        try:
            MongoAdvancedRulesValidator.SCHEMA(advanced_rules)

            return SyncRuleValidationResult.valid_result(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES
            )
        except JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )


class MongoDataSource(BaseDataSource):
    """MongoDB"""

    name = "MongoDB"
    service_type = "mongodb"
    advanced_rules_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

        self.client = None
        self.host = self.configuration["host"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.ssl_ca = self.configuration["ssl_ca"]
        self.user = self.configuration["user"]
        self.password = self.configuration["password"]
        self.tls_insecure = self.configuration["tls_insecure"]
        self.collection = None

    @classmethod
    def get_default_configuration(cls):
        return {
            "host": {
                "label": "Server hostname",
                "order": 1,
                "type": "str",
            },
            "user": {
                "label": "Username",
                "order": 2,
                "required": False,
                "type": "str",
            },
            "password": {
                "label": "Password",
                "order": 3,
                "required": False,
                "sensitive": True,
                "type": "str",
            },
            "database": {"label": "Database", "order": 4, "type": "str"},
            "collection": {
                "label": "Collection",
                "order": 5,
                "type": "str",
            },
            "direct_connection": {
                "display": "toggle",
                "label": "Direct connection",
                "order": 6,
                "type": "bool",
                "value": False,
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "SSL/TLS Connection",
                "order": 7,
                "tooltip": "This option establishes a secure connection to the MongoDB server using SSL/TLS encryption. Ensure that your MongoDB deployment supports SSL/TLS connections. Enable if MongoDB cluster uses DNS SRV records.",
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "Certificate Authority (.pem)",
                "order": 8,
                "type": "str",
                "required": False,
                "display": "textarea",
                "tooltip": "Specifies the root certificate from the Certificate Authority. The value of the certificate is used to validate the certificate presented by the Mongo server.",
            },
            "tls_insecure": {
                "display": "toggle",
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "Skip certificate verification",
                "order": 9,
                "tooltip": "This option skips certificate validation for TLS/SSL connections to your MongoDB server. We strongly recommend setting this option to 'disable'.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }

    @contextmanager
    def get_client(self):
        certfile = ""
        try:
            client_params = {}
            if self.configuration["direct_connection"]:
                client_params["directConnection"] = True

            if len(self.user) > 0 or len(self.password) > 0:
                client_params["username"] = self.user
                client_params["password"] = self.password

            if self.configuration["ssl_enabled"]:
                client_params["tls"] = True
                if self.ssl_ca:
                    pem_certificates = get_pem_format(key=self.ssl_ca)
                    with NamedTemporaryFile(
                        mode="w", suffix=".pem", delete=False
                    ) as cert:
                        cert.write(pem_certificates)
                        certfile = cert.name
                    client_params["tlsCAFile"] = certfile
                client_params["tlsInsecure"] = self.tls_insecure
            else:
                client_params["tls"] = False

            client = AsyncIOMotorClient(self.host, **client_params)

            db = client[self.configuration["database"]]
            self.collection = db[self.configuration["collection"]]

            yield client
        finally:
            self.remove_temp_file(certfile)

    def advanced_rules_validators(self):
        return [MongoAdvancedRulesValidator()]

    async def ping(self):
        with self.get_client() as client:
            await client.admin.command("ping")

    def remove_temp_file(self, temp_file):
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except Exception as exception:
                self._logger.warning(
                    f"Something went wrong while removing temporary certificate file. Exception: {exception}",
                    exc_info=True,
                )

    # TODO: That's a lot of work. Find a better way
    def serialize(self, doc):
        def _serialize(value):
            if isinstance(value, ObjectId):
                value = str(value)
            elif isinstance(value, (list, tuple)):
                value = [_serialize(item) for item in value]
            elif isinstance(value, dict):
                for key, svalue in value.items():
                    value[key] = _serialize(svalue)
            elif isinstance(value, datetime):
                value = value.isoformat()
            elif isinstance(value, Decimal128):
                value = value.to_decimal()
            elif isinstance(value, DBRef):
                value = _serialize(value.as_doc().to_dict())
            return value

        for key, value in doc.items():
            doc[key] = _serialize(value)

        return doc

    async def get_docs(self, filtering=None):
        if filtering is not None and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()

            if "find" in advanced_rules:
                find_kwargs = advanced_rules.get("find", {})

                with self.get_client():
                    async for doc in self.collection.find(**find_kwargs):
                        yield self.serialize(doc), None

            elif "aggregate" in advanced_rules:
                aggregate_kwargs = deepcopy(advanced_rules.get("aggregate", {}))
                pipeline = aggregate_kwargs.pop("pipeline", [])

                with self.get_client():
                    async for doc in self.collection.aggregate(
                        pipeline=pipeline, **aggregate_kwargs
                    ):
                        yield self.serialize(doc), None
        else:
            with self.get_client():
                async for doc in self.collection.find():
                    yield self.serialize(doc), None

    def check_conflicting_values(self, value):
        if value == "true":
            value = True
        elif value == "false":
            value = False
        else:
            value = None

        if value != self.ssl_enabled:
            msg = "The value of SSL/TLS must be the same in the hostname and configuration field."
            raise ConfigurableFieldValueError(msg)

    async def validate_config(self):
        await super().validate_config()
        parsed_url = urllib.parse.urlparse(self.host)
        query_params = urllib.parse.parse_qs(parsed_url.query)

        if "ssl" in query_params:
            ssl_value = query_params.get("ssl", ["None"])[0]
            self.check_conflicting_values(ssl_value)

        if "tls" in query_params:
            ssl_value = query_params.get("tls", ["None"])[0]
            self.check_conflicting_values(ssl_value)

        user = self.configuration["user"]
        configured_database_name = self.configuration["database"]
        configured_collection_name = self.configuration["collection"]

        with self.get_client() as client:
            # First check if collection is accessible
            try:
                # This works on both standalone and Managed mongo in the same way
                await client[configured_database_name].validate_collection(
                    configured_collection_name
                )
                return
            except OperationFailure:
                self._logger.debug(
                    f"Unable to validate '{configured_database_name}.{configured_collection_name}' as user '{user}'"
                )

            # If it's not accessible, try to make a good user-friendly error message
            try:
                # We will try to access some databases/collections to give a friendly message
                # That will suggest the name of existing collection - but only if the user
                # that we use to log in into MongoDB has access to it
                existing_database_names = await client.list_database_names()

                self._logger.debug(f"Existing databases: {existing_database_names}")

                if configured_database_name not in existing_database_names:
                    msg = f"Database '{configured_database_name}' does not exist. Existing databases: {', '.join(existing_database_names)}"
                    raise ConfigurableFieldValueError(msg)

                database = client[configured_database_name]

                existing_collection_names = await database.list_collection_names()
                self._logger.debug(
                    f"Existing collections in {configured_database_name}: {existing_collection_names}"
                )

                if configured_collection_name not in existing_collection_names:
                    msg = f"Collection '{configured_collection_name}' does not exist within database '{configured_database_name}'. Existing collections: {', '.join(existing_collection_names)}"
                    raise ConfigurableFieldValueError(msg)
                else:
                    self._logger.debug(
                        f"Found {configured_database_name}.{configured_collection_name} as user {user}"
                    )
            except OperationFailure as e:
                # This happens if the user has no access to operations to list collection/database names
                # Managed MongoDB never gets here, but if we're running against a standalone mongo
                # Then this code can trigger
                msg = f"Database '{configured_database_name}' or collection '{configured_collection_name}' is not accessible by user '{user}'. Verify that these database and collection exist, and specified user has access to it"
                raise ConfigurableFieldValueError(msg) from e
