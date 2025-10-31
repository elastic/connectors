#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#


import fastjsonschema
from connectors_sdk.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)


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
            RedisAdvancedRulesValidator.SCHEMA(advanced_rules)  # type: ignore[misc]
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
