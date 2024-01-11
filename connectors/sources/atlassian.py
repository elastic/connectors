#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import fastjsonschema
from fastjsonschema import JsonSchemaValueException

from connectors.access_control import es_access_control_query, prefix_identity
from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.utils import RetryStrategy, iso_utc, retryable

RETRIES = 3
RETRY_INTERVAL = 2
USER_BATCH = 50


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


def prefix_account_id(account_id):
    return prefix_identity("account_id", account_id)


def prefix_group_id(group_id):
    return prefix_identity("group_id", group_id)


def prefix_role_key(role_key):
    return prefix_identity("role_key", role_key)


def prefix_account_name(account_name):
    return prefix_identity("name", account_name.replace(" ", "-"))


def prefix_account_email(email):
    return prefix_identity("email_address", email)


def prefix_account_locale(locale):
    return prefix_identity("locale", locale)


class AtlassianAccessControl:
    def __init__(self, source, client):
        self.source = source
        self.client = client

    def access_control_query(self, access_control):
        return es_access_control_query(access_control)

    async def fetch_all_users(self, url):
        start_at = 0
        while True:
            async for users in self.client.api_call(url=f"{url}?startAt={start_at}"):
                response = await users.json()
                if len(response) == 0:
                    return
                yield response
                start_at += USER_BATCH

    async def fetch_user(self, url):
        async for user in self.client.api_call(url=url):
            yield await user.json()

    async def user_access_control_doc(self, user):
        """Generate a user access control document.

        This method generates a user access control document based on the provided user information.
        The document includes the user's account ID, prefixed account ID, prefixed account name,
        a set of prefixed group IDs, and a set of prefixed role keys. The access control list is
        then constructed using these values.

        Args:
            user (dict): A dictionary containing user information, such as account ID, display name, groups, and application roles.

        Returns:
            dict: A user access control document with the following structure:
                {
                    "_id": <account_id>,
                    "identity": {
                        "account_id": <prefixed_account_id>,
                        "display_name": <_prefixed_account_name>,
                        "locale": <prefix_account_locale>,
                        "emailAddress": prefix_account_email,
                    },
                    "created_at": <iso_utc_timestamp>,
                    ACCESS_CONTROL: [<prefixed_account_id>, <prefixed_group_ids>, <prefixed_role_keys>]
                }
        """
        account_id = user.get("accountId")
        account_name = user.get("displayName")
        email = user.get("emailAddress")
        locale = user.get("locale")

        prefixed_account_email = prefix_account_email(email=email)
        prefixed_account_id = prefix_account_id(account_id=account_id)
        prefixed_account_name = prefix_account_name(account_name=account_name)
        prefixed_account_locale = prefix_account_locale(locale=locale)

        prefixed_group_ids = {
            prefix_group_id(group_id=group.get("groupId", ""))
            for group in user.get("groups", {}).get("items", [])
        }
        prefixed_role_keys = {
            prefix_role_key(role_key=role.get("key", ""))
            for role in user.get("applicationRoles", {}).get("items", [])
        }

        user_document = {
            "_id": account_id,
            "identity": {
                "account_id": prefixed_account_id,
                "display_name": prefixed_account_name,
                "email_address": prefixed_account_email,
                "locale": prefixed_account_locale,
            },
            "created_at": iso_utc(),
        }

        access_control = (
            [prefixed_account_id] + list(prefixed_group_ids) + list(prefixed_role_keys)
        )

        return user_document | self.access_control_query(access_control=access_control)

    def is_active_atlassian_user(self, user_info):
        user_url = user_info.get("self")
        user_name = user_info.get("displayName", "user")
        if not user_url:
            self.source._logger.debug(
                f"Skipping {user_name} as profile URL is not present."
            )
            return False

        if not user_info.get("active"):
            self.source._logger.debug(
                f"Skipping {user_name} as it is inactive or deleted."
            )
            return False

        if user_info.get("accountType") != "atlassian":
            self.source._logger.debug(
                f"Skipping {user_name} because the account type is {user_info.get('accountType')}. Only 'atlassian' account type is supported."
            )
            return False

        return True
