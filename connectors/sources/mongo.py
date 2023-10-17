#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from copy import deepcopy
from datetime import datetime

import fastjsonschema
from bson import Decimal128, ObjectId
from fastjsonschema import JsonSchemaValueException
from motor.motor_asyncio import AsyncIOMotorClient

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.source import BaseDataSource, ConfigurableFieldValueError


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
                rule_ids=SyncRuleValidationResult.ADVANCED_RULES
            )
        except JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_ids=SyncRuleValidationResult.ADVANCED_RULES,
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

        client_params = {}

        host = self.configuration["host"]
        user = self.configuration["user"]
        password = self.configuration["password"]

        if self.configuration["direct_connection"]:
            client_params["directConnection"] = True

        if len(user) > 0 or len(password) > 0:
            client_params["username"] = user
            client_params["password"] = password

        self.client = AsyncIOMotorClient(host, **client_params)

        self.db = self.client[self.configuration["database"]]
        self.collection = self.db[self.configuration["collection"]]

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
        }

    def advanced_rules_validators(self):
        return [MongoAdvancedRulesValidator()]

    async def ping(self):
        await self.client.admin.command("ping")

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
            return value

        for key, value in doc.items():
            doc[key] = _serialize(value)

        return doc

    async def get_docs(self, filtering=None):
        if filtering is not None and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()

            if "find" in advanced_rules:
                find_kwargs = advanced_rules.get("find", {})

                async for doc in self.collection.find(**find_kwargs):
                    yield self.serialize(doc), None

            elif "aggregate" in advanced_rules:
                aggregate_kwargs = deepcopy(advanced_rules.get("aggregate", {}))
                pipeline = aggregate_kwargs.pop("pipeline", [])

                async for doc in self.collection.aggregate(
                    pipeline=pipeline, **aggregate_kwargs
                ):
                    yield self.serialize(doc), None
        else:
            async for doc in self.collection.find():
                yield self.serialize(doc), None

    async def validate_config(self):
        await super().validate_config()

        client = self.client
        configured_database_name = self.configuration["database"]
        configured_collection_name = self.configuration["collection"]

        existing_database_names = await client.list_database_names()

        self._logger.debug(f"Existing databases: {existing_database_names}")

        if configured_database_name not in existing_database_names:
            raise ConfigurableFieldValueError(
                f"Database ({configured_database_name}) does not exist. Existing databases: {', '.join(existing_database_names)}"
            )

        database = client[configured_database_name]

        existing_collection_names = await database.list_collection_names()
        self._logger.debug(
            f"Existing collections in {configured_database_name}: {existing_collection_names}"
        )

        if configured_collection_name not in existing_collection_names:
            raise ConfigurableFieldValueError(
                f"Collection ({configured_collection_name}) does not exist within database {configured_database_name}. Existing collections: {', '.join(existing_collection_names)}"
            )
