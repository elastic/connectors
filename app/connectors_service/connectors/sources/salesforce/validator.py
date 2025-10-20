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

from connectors.sources.salesforce.client import InvalidQueryException
from connectors.sources.salesforce.constants import (
    QUERY_ENDPOINT,
    RETRY_INTERVAL,
    SOSL_SEARCH_ENDPOINT,
)
from connectors.utils import (
    RetryStrategy,
    retryable,
)


class SalesforceAdvancedRulesValidator(AdvancedRulesValidator):
    OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "query": {"type": "string", "minLength": 1},
            "language": {
                "type": "string",
                "minLength": 1,
                "enum": ["SOSL", "SOQL"],
            },
        },
        "required": ["query", "language"],
        "additionalProperties": False,
    }

    SCHEMA_DEFINITION = {"type": "array", "items": OBJECT_SCHEMA_DEFINITION}

    SCHEMA = fastjsonschema.compile(definition=SCHEMA_DEFINITION)

    def __init__(self, source):
        self.source = source

    async def validate(self, advanced_rules):
        return await self._remote_validation(advanced_rules)

    @retryable(
        retries=3,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _remote_validation(self, advanced_rules):
        try:
            SalesforceAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        invalid_queries = []
        for rule in advanced_rules:
            language = rule["language"]
            query = (
                self.source.salesforce_client.modify_soql_query(rule["query"])
                if language == "SOQL"
                else rule["query"]
            )
            try:
                url = (
                    f"{self.source.base_url_}{SOSL_SEARCH_ENDPOINT}"
                    if language == "SOSL"
                    else f"{self.source.base_url_}{QUERY_ENDPOINT}"
                )
                params = {"q": query}
                await self.source.salesforce_client._get_json(
                    url,
                    params=params,
                )
            except InvalidQueryException:
                invalid_queries.append(query)

        if len(invalid_queries) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Found invalid queries: '{' & '.join(invalid_queries)}'. Either object or fields might be invalid.",
            )
        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )
