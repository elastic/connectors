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

from connectors.sources.dropbox.common import (
    RETRY_COUNT,
    RETRY_INTERVAL,
    InvalidPathException,
)
from connectors.utils import (
    RetryStrategy,
    retryable,
)


class DropBoxAdvancedRulesValidator(AdvancedRulesValidator):
    FILE_CATEGORY_DEFINITION = {
        "type": "object",
        "properties": {
            ".tag": {"type": "string", "minLength": 1},
        },
    }

    OPTIONS_DEFINITION = {
        "type": "object",
        "properties": {
            "path": {"type": "string", "minLength": 1},
            "max_results": {"type": "string", "minLength": 1},
            "order_by": FILE_CATEGORY_DEFINITION,
            "file_status": FILE_CATEGORY_DEFINITION,
            "account_id": {"type": "string", "minLength": 1},
            "file_extensions": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
            },
            "file_categories": {
                "type": "array",
                "items": FILE_CATEGORY_DEFINITION,
                "minItems": 1,
            },
        },
    }

    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "match_field_options": {"type": "object"},
            "options": OPTIONS_DEFINITION,
            "query": {"type": "string", "minLength": 1},
        },
        "required": ["query"],
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

    @retryable(
        retries=RETRY_COUNT,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _remote_validation(self, advanced_rules):
        try:
            DropBoxAdvancedRulesValidator.SCHEMA(advanced_rules)  # type: ignore[misc]
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        invalid_paths = []
        for rule in advanced_rules:
            self.source.dropbox_client.path = rule.get("options", {}).get("path")
            try:
                if self.source.dropbox_client.path:
                    await self.source.dropbox_client.check_path()
            except InvalidPathException:
                invalid_paths.append(self.source.dropbox_client.path)

        if len(invalid_paths) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Invalid paths '{', '.join(invalid_paths)}'.",
            )
        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )
