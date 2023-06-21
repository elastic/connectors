#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import fastjsonschema
from fastjsonschema import JsonSchemaValueException

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.utils import RetryStrategy, retryable

RETRIES = 3
RETRY_INTERVAL = 2


class AtlassianAdvancedRulesValidator(AdvancedRulesValidator):
    QUERY_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "query": {"type": "string", "minLength": 1},
        },
        "required": ["query"],
        "additionalProperties": False,
    }

    SCHEMA_DEFINITION = {"type": "array", "items": QUERY_OBJECT_SCHEMA_DEFINITION}

    SCHEMA = fastjsonschema.compile(definition=SCHEMA_DEFINITION)

    def __init__(self, source):
        self.source = source

    async def validate(self, advanced_rules):
        if len(advanced_rules) == 0:
            return SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            )

        return await self._remote_validation(advanced_rules)

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _remote_validation(self, advanced_rules):
        try:
            AtlassianAdvancedRulesValidator.SCHEMA(advanced_rules)
        except JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )
