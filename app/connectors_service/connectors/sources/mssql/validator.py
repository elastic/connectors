#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import fastjsonschema
from tenacity import retry, stop_after_attempt, wait_exponential

from connectors_sdk.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)

from connectors.sources.shared.database.generic_database import (
    DEFAULT_RETRY_COUNT,
    DEFAULT_WAIT_MULTIPLIER,
)


class MSSQLAdvancedRulesValidator(AdvancedRulesValidator):
    QUERY_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "tables": {"type": "array", "minItems": 1},
            "query": {"type": "string", "minLength": 1},
            "id_columns": {"type": "array", "minItems": 1},
        },
        "required": ["tables", "query"],
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

    @retry(
        stop=stop_after_attempt(DEFAULT_RETRY_COUNT),
        wait=wait_exponential(multiplier=1, exp_base=DEFAULT_WAIT_MULTIPLIER),
        reraise=True,
    )
    async def _remote_validation(self, advanced_rules):
        try:
            MSSQLAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        tables_to_filter = {
            table
            for query_info in advanced_rules
            for table in query_info.get("tables", [])
        }
        tables = {
            table
            async for table in self.source.mssql_client.get_tables_to_fetch(
                is_filtering=True
            )
        }
        missing_tables = tables_to_filter - tables

        if len(missing_tables) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Tables not found or inaccessible: {missing_tables}.",
            )

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )
