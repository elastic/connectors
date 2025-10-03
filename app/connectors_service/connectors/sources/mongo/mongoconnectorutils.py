import fastjsonschema
from connectors_sdk.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from fastjsonschema import JsonSchemaValueException


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
