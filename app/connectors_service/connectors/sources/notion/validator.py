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
from connectors_sdk.logger import logger
from connectors_sdk.source import ConfigurableFieldValueError


class NotionAdvancedRulesValidator(AdvancedRulesValidator):
    DATABASE_QUERY_DEFINITION = {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "database_id": {"type": "string", "minLength": 1},
                "filter": {
                    "type": "object",
                    "properties": {"property": {"type": "string", "minLength": 1}},
                    "additionalProperties": {
                        "type": ["object", "array"],
                    },
                },
            },
            "required": ["database_id"],
        },
    }

    SEARCH_QUERY_DEFINITION = {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "filter": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string", "enum": ["page", "database"]},
                    },
                    "required": ["value"],
                },
            },
            "required": ["query"],
        },
    }

    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "database_query_filters": DATABASE_QUERY_DEFINITION,
            "searches": SEARCH_QUERY_DEFINITION,
        },
        "minProperties": 1,
        "additionalProperties": False,
    }

    SCHEMA = fastjsonschema.compile(definition=RULES_OBJECT_SCHEMA_DEFINITION)

    def __init__(self, source):
        self.source = source
        self._logger = logger

    async def validate(self, advanced_rules):
        if len(advanced_rules) == 0:
            return SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            )

        self._logger.info("Remote validation started")
        return await self._remote_validation(advanced_rules)

    async def _remote_validation(self, advanced_rules):
        try:
            NotionAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )
        invalid_database = []
        databases = []
        page_title = []
        database_title = []
        query = {"filter": {"property": "object", "value": "database"}}
        async for document in self.source.notion_client.async_iterate_paginated_api(
            self.source.notion_client._get_client.search, **query
        ):
            databases.append(document.get("id").replace("-", ""))
        for rule in advanced_rules:
            if "database_query_filters" in rule:
                self._logger.info("Validating database query filters")
                for database in advanced_rules.get("database_query_filters"):
                    database_id = (
                        database.get("database_id").replace("-", "")
                        if "-" in database.get("database_id")
                        else database.get("database_id")
                    )
                    if database_id not in databases:
                        invalid_database.append(database.get("database_id"))
                if invalid_database:
                    return SyncRuleValidationResult(
                        SyncRuleValidationResult.ADVANCED_RULES,
                        is_valid=False,
                        validation_message=f"Invalid database id: {', '.join(invalid_database)}",
                    )
            if "searches" in rule:
                self._logger.info("Validating search filters")
                for database_page in advanced_rules.get("searches"):
                    if database_page.get("filter", {}).get("value") == "page":
                        page_title.append(database_page.get("query"))
                    elif database_page.get("filter", {}).get("value") == "database":
                        database_title.append(database_page.get("query"))
                try:
                    if page_title:
                        await self.source.get_entities("page", page_title)
                    if database_title:
                        await self.source.get_entities("database", database_title)
                except ConfigurableFieldValueError as error:
                    return SyncRuleValidationResult(
                        SyncRuleValidationResult.ADVANCED_RULES,
                        is_valid=False,
                        validation_message=str(error),
                    )
        self._logger.info("Remote validation successful")
        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )
