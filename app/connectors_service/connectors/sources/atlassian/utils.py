#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.access_control import prefix_identity


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


def prefix_user(user):
    if not user:
        return
    return prefix_identity("user", user)


def prefix_group(group):
    return prefix_identity("group", group)
