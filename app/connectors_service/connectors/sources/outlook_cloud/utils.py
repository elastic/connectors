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

LEGACY_EVENT_TYPE = {
    "singleInstance": "Single",
    "occurrence": "Occurrence",
    "exception": "Exception",
    "seriesMaster": "RecurringMaster",
}

_MONTHS = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
_WEEKDAYS = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)
# exchangelib names for day groups.
_DAY_GROUPS = {
    frozenset(_WEEKDAYS): "Day",
    frozenset(_WEEKDAYS[:5]): "Weekday",
    frozenset(_WEEKDAYS[5:]): "WeekendDay",
}


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


def graph_event_type_to_legacy(event_type):
    """Map to the legacy event type. Unknown values pass through."""
    return LEGACY_EVENT_TYPE.get(event_type, event_type)


def _day_names(days):
    return [day.capitalize() for day in _WEEKDAYS if day in days]


def _relative_day(days):
    """A day or day group, named as exchangelib does."""
    return _DAY_GROUPS.get(frozenset(days)) or ", ".join(_day_names(days))


def graph_recurrence_to_legacy(pattern):
    """Describe a Graph `recurrencePattern` in exchangelib wording. None if missing, unknown or incomplete."""
    if not isinstance(pattern, dict):
        return None
    try:
        interval = pattern["interval"]
        match pattern["type"]:
            case "daily":
                return f"Occurs every {interval} day(s)"
            case "weekly":
                days = ", ".join(_day_names(pattern["daysOfWeek"]))
                first_day = pattern["firstDayOfWeek"].capitalize()
                return (
                    f"Occurs on weekdays {days} of every {interval} week(s) "
                    f"where the first day of the week is {first_day}"
                )
            case "absoluteMonthly":
                return f"Occurs on day {pattern['dayOfMonth']} of every {interval} month(s)"
            case "relativeMonthly":
                return (
                    f"Occurs on weekday {_relative_day(pattern['daysOfWeek'])} in the "
                    f"{pattern['index'].capitalize()} week of every {interval} month(s)"
                )
            case "absoluteYearly":
                return f"Occurs on day {pattern['dayOfMonth']} of {_MONTHS[pattern['month'] - 1]}"
            case "relativeYearly":
                return (
                    f"Occurs on weekday {_relative_day(pattern['daysOfWeek'])} in the "
                    f"{pattern['index'].capitalize()} week of {_MONTHS[pattern['month'] - 1]}"
                )
    except (KeyError, TypeError, AttributeError, IndexError):
        pass
    return None
