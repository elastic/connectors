#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft Outlook source module is responsible to fetch documents from Outlook server or cloud platforms."""

from datetime import date

import exchangelib

from connectors.access_control import prefix_identity


def ews_format_to_datetime(source_datetime, timezone):
    """Change datetime format to user account timezone
    Args:
        datetime: Datetime in UTC format
        timezone: User account timezone
    Returns:
        Datetime: Date format as user account timezone
    """
    if isinstance(source_datetime, exchangelib.ewsdatetime.EWSDateTime) and isinstance(
        timezone, exchangelib.ewsdatetime.EWSTimeZone
    ):
        return (source_datetime.astimezone(timezone)).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif isinstance(source_datetime, exchangelib.ewsdatetime.EWSDate) or isinstance(
        source_datetime, date
    ):
        return source_datetime.strftime("%Y-%m-%d")
    else:
        return source_datetime


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_display_name(user):
    return prefix_identity("name", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_job(job_title):
    return prefix_identity("job_title", job_title)
