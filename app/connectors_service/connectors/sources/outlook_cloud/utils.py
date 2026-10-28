#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import re
from datetime import datetime, timezone

_ISO8601_FRACTION = re.compile(
    r"^(?P<head>.+?)(?P<fraction>\.\d+)(?P<tz>Z|[+-]\d{2}:?\d{2})$"
)


def graph_datetime_to_iso_utc(value):
    """Parse Graph ISO-8601 timestamps to ``YYYY-MM-DDTHH:MM:SSZ``."""
    if not value or not isinstance(value, str):
        return value
    text = value.strip()
    match = _ISO8601_FRACTION.match(text)
    if match:
        tz = match.group("tz")
        tz = "+00:00" if tz == "Z" else tz
        if len(tz) == 5 and tz[3] != ":":
            tz = f"{tz[:3]}:{tz[3:]}"
        fraction = match.group("fraction")[:7]
        text = f"{match.group('head')}{fraction}{tz}"
    elif text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")


def graph_importance_to_legacy(importance):
    if not importance or not isinstance(importance, str):
        return importance
    mapping = {"low": "Low", "normal": "Normal", "high": "High"}
    return mapping.get(importance.lower(), importance)
