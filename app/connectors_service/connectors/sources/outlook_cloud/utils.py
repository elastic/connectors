#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Small helpers for Outlook Cloud Graph formatting."""

import re
from datetime import datetime, timezone

# Seconds, optional fraction of any length, then `Z` or an offset.
_GRAPH_DATETIME = re.compile(
    r"^(?P<base>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d+))?"
    r"(?P<tz>Z|[+-]\d{2}:?\d{2})?$"
)

LEGACY_IMPORTANCE = {"low": "Low", "normal": "Normal", "high": "High"}


def graph_datetime_to_iso_utc(value):
    """Convert a Graph timestamp to ``YYYY-MM-DDTHH:MM:SSZ`` (UTC).

    Python 3.10's ``datetime.fromisoformat`` rejects ``Z`` and fractions that are not 3
    or 6 digits long (Graph can send 7), so the string is normalized first.
    Empty values are returned unchanged. A string that is not a timestamp raises
    ValueError.
    """
    if not value:
        return value
    match = _GRAPH_DATETIME.match(value.strip())
    if match is None:
        msg = f"Not a Microsoft Graph timestamp: {value!r}"
        raise ValueError(msg)
    tz = match["tz"] or "Z"
    if tz == "Z":
        tz = "+00:00"
    elif ":" not in tz:
        tz = f"{tz[:3]}:{tz[3:]}"
    # Sub-second precision is dropped, so the fraction only has to parse.
    fraction = (match["fraction"] or "0")[:6].ljust(6, "0")
    parsed = datetime.fromisoformat(f"{match['base']}.{fraction}{tz}")
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def graph_importance_to_legacy(importance):
    """`high` becomes `High`, as legacy Outlook indexes it. Unknown values pass through."""
    if not isinstance(importance, str):
        return importance
    return LEGACY_IMPORTANCE.get(importance.lower(), importance)
