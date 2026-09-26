#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import re

import fastjsonschema
from connectors_sdk.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from fastjsonschema import JsonSchemaValueException


class S3AdvancedRulesValidator(AdvancedRulesValidator):
    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "bucket": {"type": "string", "minLength": 1},
            "prefix": {"type": "string"},
            "extension": {"type": "array"},
            "pattern": {"type": "string"},
        },
        "required": ["bucket"],
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
        try:
            S3AdvancedRulesValidator.SCHEMA(advanced_rules)
        except JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )
        for rule in advanced_rules:
            pattern = rule.get("pattern")
            if pattern:
                try:
                    re.compile(pattern)
                except re.error as e:
                    return SyncRuleValidationResult(
                        rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                        is_valid=False,
                        validation_message=f"Invalid regex pattern '{pattern}': {e}",
                    )
        return SyncRuleValidationResult.valid_result(
            rule_id=SyncRuleValidationResult.ADVANCED_RULES
        )
