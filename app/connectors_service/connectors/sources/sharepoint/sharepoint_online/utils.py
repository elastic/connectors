#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from datetime import datetime

from connectors.access_control import prefix_identity
from connectors.sources.sharepoint.sharepoint_online.constants import TIMESTAMP_FORMAT, EXCLUDED_SHAREPOINT_PATH_SEGMENTS


class SyncCursorEmpty(Exception):
    """Exception class to notify that incremental sync can't run because sync_cursor is empty.
    See: https://learn.microsoft.com/en-us/graph/delta-query-overview
    """

    pass


def _prefix_group(group):
    return prefix_identity("group", group)


def _prefix_user(user):
    return prefix_identity("user", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_email(email):
    return prefix_identity("email", email)


def _get_login_name(raw_login_name):
    if raw_login_name and (
        raw_login_name.startswith("i:0#.f|membership|")
        or raw_login_name.startswith("c:0o.c|federateddirectoryclaimprovider|")
        or raw_login_name.startswith("c:0t.c|tenant|")
    ):
        parts = raw_login_name.split("|")

        if len(parts) > 2:
            return parts[2]

    return None


def _parse_created_date_time(created_date_time):
    if created_date_time is None:
        return None
    return datetime.strptime(created_date_time, TIMESTAMP_FORMAT)


def _is_excluded_sharepoint_url(url: str) -> bool:
    try:
        return any(
            segment in url.lower() for segment in EXCLUDED_SHAREPOINT_PATH_SEGMENTS
        )
    except Exception:
        return False

