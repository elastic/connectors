#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import ANY

import pytest

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.sources.atlassian import (
    AtlassianAccessControl,
    AtlassianAdvancedRulesValidator,
)
from connectors.sources.jira import JiraClient, JiraDataSource
from tests.sources.support import create_source


@pytest.mark.parametrize(
    "advanced_rules, expected_validation_result",
    [
        (
            # valid: empty array should be valid
            [],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: empty object should also be valid -> default value in Kibana
            {},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: one custom query
            [{"query": "type=A"}],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: two custom queries
            [{"query": "type=A"}, {"query": "type=B"}],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # invalid: query empty
            [{"query": "type=A"}, {"query": ""}],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: unallowed key
            [{"query": "type=A"}, {"queries": "type=B"}],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: list of strings -> wrong type
            {"query": ["type=A"]},
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: array of arrays -> wrong type
            {"query": ["type=A", ""]},
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_advanced_rules_validation(advanced_rules, expected_validation_result):
    validation_result = await AtlassianAdvancedRulesValidator(
        AdvancedRulesValidator
    ).validate(advanced_rules)

    assert validation_result == expected_validation_result


@pytest.mark.parametrize(
    "user_info, result",
    [
        (
            {
                "self": "url1",
                "accountId": "607194d6bc3c3f006f4c35d6",
                "accountType": "atlassian",
                "displayName": "user1",
                "active": True,
            },
            True,
        ),
        (
            {
                "self": "url1",
                "accountId": "607194d6bc3c3f006f4c35d6",
                "accountType": "app",
                "displayName": "user1",
                "active": True,
            },
            False,
        ),
        (
            {
                "self": "url1",
                "accountId": "607194d6bc3c3f006f4c35d6",
                "accountType": "atlassian",
                "displayName": "user1",
                "active": False,
            },
            False,
        ),
    ],
)
@pytest.mark.asyncio
async def test_active_atlassian_user(user_info, result):
    async with create_source(JiraDataSource) as source:
        validation_result = AtlassianAccessControl(
            source, JiraClient
        ).is_active_atlassian_user(user_info)
        assert validation_result == result
