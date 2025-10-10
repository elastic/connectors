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

from connectors.sources.github.utils import (
    ObjectType,
)


class GitHubAdvancedRulesValidator(AdvancedRulesValidator):
    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "repository": {"type": "string", "minLength": 1},
            "filter": {
                "type": "object",
                "properties": {
                    ObjectType.ISSUE.value.lower(): {"type": "string", "minLength": 1},
                    ObjectType.PR.value: {"type": "string", "minLength": 1},
                    ObjectType.BRANCH.value: {"type": "string", "minLength": 1},
                    ObjectType.PATH.value: {"type": "string", "minLength": 1},
                },
                "minProperties": 1,
                "additionalProperties": False,
            },
        },
        "required": ["repository", "filter"],
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
            GitHubAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        self.source.configured_repos = {rule["repository"] for rule in advanced_rules}
        invalid_repos = await self.source.get_invalid_repos()

        if len(invalid_repos) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Inaccessible repositories '{', '.join(invalid_repos)}'.",
            )

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )
