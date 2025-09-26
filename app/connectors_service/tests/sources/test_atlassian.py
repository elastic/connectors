#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import json
from unittest.mock import ANY
from urllib import parse

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
                "locale": "en-US",
                "emailAddress": "user1@dummy-domain.com",
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
                "locale": "en-US",
                "emailAddress": "user1@dummy-domain.com",
                "active": False,
            },
            False,
        ),
    ],
)
@pytest.mark.asyncio
async def test_active_atlassian_user(user_info, result):
    async with create_source(
        JiraDataSource, jira_url="https://127.0.0.1:8080/test"
    ) as source:
        validation_result = AtlassianAccessControl(
            source, JiraClient
        ).is_active_atlassian_user(user_info)
        assert validation_result == result


user_response = """
[
  {
    "accountId": "5b10a2844c20165700ede21g",
    "accountType": "atlassian",
    "active": false,
    "avatarUrls": {
      "16x16": "https://avatar-management--avatars.server-location.prod.public.atl-paas.net/initials/MK-5.png?size=16&s=16",
      "24x24": "https://avatar-management--avatars.server-location.prod.public.atl-paas.net/initials/MK-5.png?size=24&s=24",
      "32x32": "https://avatar-management--avatars.server-location.prod.public.atl-paas.net/initials/MK-5.png?size=32&s=32",
      "48x48": "https://avatar-management--avatars.server-location.prod.public.atl-paas.net/initials/MK-5.png?size=48&s=48"
    },
    "displayName": "Mia Krystof",
    "key": "",
    "name": "",
    "self": "https://your-domain.atlassian.net/rest/api/3/user?accountId=5b10a2844c20165700ede21g"
  },
  {
    "accountId": "5b10ac8d82e05b22cc7d4ef5",
    "accountType": "atlassian",
    "active": false,
    "avatarUrls": {
      "16x16": "https://avatar-management--avatars.server-location.prod.public.atl-paas.net/initials/AA-3.png?size=16&s=16",
      "24x24": "https://avatar-management--avatars.server-location.prod.public.atl-paas.net/initials/AA-3.png?size=24&s=24",
      "32x32": "https://avatar-management--avatars.server-location.prod.public.atl-paas.net/initials/AA-3.png?size=32&s=32",
      "48x48": "https://avatar-management--avatars.server-location.prod.public.atl-paas.net/initials/AA-3.png?size=48&s=48"
    },
    "displayName": "Emma Richards",
    "key": "",
    "name": "",
    "self": "https://your-domain.atlassian.net/rest/api/3/user?accountId=5b10ac8d82e05b22cc7d4ef5"
  }
]
"""


@pytest.mark.asyncio
async def test_fetch_all_users(mock_responses):
    jira_host = "https://127.0.0.1:8080"
    users_path = "rest/api/3/users/search"

    mock_responses.get(
        f"{parse.urljoin(jira_host, users_path)}?startAt=0",
        status=200,
        payload=json.loads(user_response),
    )
    mock_responses.get(
        f"{parse.urljoin(jira_host, users_path)}?startAt=50", status=200, payload=[]
    )
    async with create_source(JiraDataSource, jira_url=jira_host) as source:
        access_control = AtlassianAccessControl(source, source.jira_client)
        results = []
        async for response in access_control.fetch_all_users(
            url=parse.urljoin(jira_host, users_path)
        ):
            for user in response:
                results.append(user)

        assert len(results) == 2
