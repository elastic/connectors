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
from fastjsonschema import JsonSchemaValueException


class GMailAdvancedRulesValidator(AdvancedRulesValidator):
    MESSAGES_SCHEMA_DEFINITION = {
        "type": "array",
        "items": {"type": "string"},
        "minItems": 1,
    }

    SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {"messages": MESSAGES_SCHEMA_DEFINITION},
        "additionalProperties": False,
    }

    SCHEMA = fastjsonschema.compile(
        definition=SCHEMA_DEFINITION,
    )

    async def validate(self, advanced_rules):
        if len(advanced_rules) == 0:
            return SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            )

        try:
            GMailAdvancedRulesValidator.SCHEMA(advanced_rules)

            return SyncRuleValidationResult.valid_result(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES
            )
        except JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )
