#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import datetime
import re
from enum import Enum

from dateutil.parser import ParserError, parser

from connectors.logger import logger

IS_BOOL_FALSE = re.compile("^(false|f|no|n|off)$", re.I)
IS_BOOL_TRUE = re.compile("^(true|t|yes|y|on)$", re.I)


def to_float(value):
    try:
        return float(value)
    except ValueError:
        return value


def to_datetime(value):
    try:
        date_parser = parser()
        parsed_date_or_datetime = date_parser.parse(timestr=value)

        if isinstance(parsed_date_or_datetime, datetime.datetime):
            return parsed_date_or_datetime
        elif isinstance(parsed_date_or_datetime, datetime.date):
            # adds 00:00 to the date and returns datetime
            return datetime.datetime.combine(parsed_date_or_datetime, datetime.time.min)

        return value

    except ParserError:
        return value


def to_bool(value):
    if len(value) == 0 or IS_BOOL_FALSE.match(value):
        return False

    if IS_BOOL_TRUE.match(value):
        return True

    return value


def try_coerce(value):
    coerced_value = to_float(value)

    if isinstance(coerced_value, str):
        coerced_value = to_datetime(value)

    if isinstance(coerced_value, str):
        coerced_value = to_bool(value)

    return coerced_value


class Rule(Enum):
    EQUALS = 1
    STARTS_WITH = 2
    ENDS_WITH = 3
    CONTAINS = 4
    REGEX = 5
    GREATER_THAN = 6
    LESS_THAN = 7


class Policy(Enum):
    INCLUDE = 1
    EXCLUDE = 2


class BasicRule:
    DEFAULT_RULE_ID = "DEFAULT"

    def __init__(self, id_, order, policy, field, rule, value):
        self._id = id_
        self._order = order
        self._policy = policy
        self._field = field
        self._rule = rule
        self._value = value

    @classmethod
    def default_rule(cls):
        return cls(
            id_=BasicRule.DEFAULT_RULE_ID,
            order=0,
            policy=Policy.INCLUDE,
            field="_",
            rule=Rule.EQUALS,
            value=".*",
        )

    def matches(self, document):
        if self.is_default_rule():
            return True

        if self._field not in document:
            return False

        document_value = document[self._field]
        coerced_rule_value = self.coerce_rule_value_based_on_document_value(
            document_value
        )

        match self._rule:
            case Rule.STARTS_WITH:
                return str(document_value).startswith(self._value)
            case Rule.ENDS_WITH:
                return str(document_value).endswith(self._value)
            case Rule.CONTAINS:
                return self._value in str(document_value)
            case Rule.REGEX:
                return re.match(self._value, str(document_value)) is not None
            case Rule.LESS_THAN:
                return document_value < coerced_rule_value
            case Rule.GREATER_THAN:
                return document_value > coerced_rule_value
            case Rule.EQUALS:
                return document_value == coerced_rule_value

    def is_default_rule(self):
        return self._id == BasicRule.DEFAULT_RULE_ID

    def is_include(self):
        return self._policy == Policy.INCLUDE

    def coerce_rule_value_based_on_document_value(self, doc_value):
        try:
            match doc_value:
                case str():
                    return str(self._value)
                case bool():
                    return str(to_bool(self._value))
                case float() | int():
                    return float(self._value)
                case datetime.date() | datetime.datetime:
                    return to_datetime(self._value)
                case _:
                    return str(self._value)
        except ValueError as e:
            logger.debug(
                f"Failed to coerce value '{self._value}' ({type(self._value)}) based on document value '{doc_value}' ({type(doc_value)}) due to error: {type(e)}: {e}"
            )
            return str(self._value)
