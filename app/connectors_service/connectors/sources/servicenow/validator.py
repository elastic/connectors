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


class ServiceNowAdvancedRulesValidator(AdvancedRulesValidator):
    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "service": {"type": "string", "minLength": 1},
            "query": {"type": "string", "minLength": 1},
        },
        "required": ["service", "query"],
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
            ServiceNowAdvancedRulesValidator.SCHEMA(advanced_rules)  # type: ignore[misc]
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        services_to_filter = {rule["service"] for rule in advanced_rules}

        (
            _,
            invalid_services,
        ) = await self.source.servicenow_client.filter_services(
            configured_service=services_to_filter.copy()
        )

        if len(invalid_services) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Services '{', '.join(invalid_services)}' are not available. Available services are: '{', '.join(set(services_to_filter) - set(invalid_services))}'",
            )

        await self.source.servicenow_client.close_session()

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )
