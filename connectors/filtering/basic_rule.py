#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import datetime
import re
from enum import Enum

from dateutil.parser import *

from connectors.logger import logger


class BasicRule:
    DEFAULT_RULE_ID = "DEFAULT"

    class Policy(Enum):
        INCLUDE = 1
        EXCLUDE = 2

    class Rule(Enum):
        EQUALS = 1
        STARTS_WITH = 2
        ENDS_WITH = 3
        CONTAINS = 4
        REGEX = 5
        GREATER_THAN = 6
        LESS_THAN = 7

    def __init__(self, id_, order, policy, field, rule, value):
        self._id = id_
        self._order = order
        self._policy = policy
        self._field = field
        self._rule = rule
        self._value = value

    @staticmethod
    def default_rule():
        return BasicRule(
            id_=BasicRule.DEFAULT_RULE_ID,
            order=0,
            policy=BasicRule.Policy.INCLUDE,
            field="_",
            rule=BasicRule.Rule.EQUALS,
            value=".*",
        )

    def matches_document(self, document):
        if self.is_default_rule:
            return True

        if self.field not in document:
            return False

        document_value = document[self.field]
        coerced_rule_value = self.coerce_rule_value_based_on_document_value(
            document_value
        )

        match self.rule:
            case BasicRule.Rule.STARTS_WITH:
                return str(document_value).startswith(self.value)
            case BasicRule.Rule.ENDS_WITH:
                return str(document_value).endswith(self.value)
            case BasicRule.Rule.CONTAINS:
                return self.value in str(document_value)
            case BasicRule.Rule.REGEX:
                return re.match(self.value, str(document_value)) is not None
            case BasicRule.Rule.LESS_THAN:
                return document_value < coerced_rule_value
            case BasicRule.Rule.GREATER_THAN:
                return document_value > coerced_rule_value
            case BasicRule.Rule.EQUALS:
                return document_value == coerced_rule_value

        return False

    @property
    def id_(self):
        return self._id

    @property
    def order(self):
        return self._order

    @property
    def policy(self):
        return self._policy

    @property
    def field(self):
        return self._field

    @property
    def rule(self):
        return self._rule

    @property
    def value(self):
        return self._value

    @property
    def is_default_rule(self):
        return self.id_ == BasicRule.DEFAULT_RULE_ID

    @property
    def is_include(self):
        return self.policy == BasicRule.Policy.INCLUDE

    def coerce_rule_value_based_on_document_value(self, doc_value):
        try:
            match doc_value:
                case str():
                    return str(self.value)
                case bool():
                    return str(BasicRule.__to_bool(self.value))
                case float() | int():
                    return float(self.value)
                case datetime.date() | datetime.datetime:
                    return BasicRule.__to_datetime(self.value)
                case _:
                    return str(self.value)
        except ValueError as e:
            logger.debug(
                f"Failed to coerce value '{self.value}' ({type(self.value)}) based on document value '{doc_value}' ({type(doc_value)}) due to error: {type(e)}: {e}"
            )
            return str(self.value)

    @staticmethod
    def try_coerce(value):
        coerced_value = BasicRule.__to_float(value)

        if isinstance(coerced_value, str):
            coerced_value = BasicRule.__to_datetime(value)

        if isinstance(coerced_value, str):
            coerced_value = BasicRule.__to_bool(value)

        return coerced_value

    @staticmethod
    def __to_float(value):
        try:
            return float(value)
        except ValueError:
            return value

    @staticmethod
    def __to_datetime(value):
        try:
            parsed_date_or_datetime = parse(timestr=value)

            if isinstance(parsed_date_or_datetime, datetime.datetime):
                return parsed_date_or_datetime
            elif isinstance(parsed_date_or_datetime, datetime.date):
                # adds 00:00 to the date and returns datetime
                return datetime.datetime.combine(
                    parsed_date_or_datetime, datetime.time.min
                )
            else:
                return value

        except ParserError:
            return value

    @staticmethod
    def __to_bool(value):
        if len(value) == 0 or re.match("^(false|f|no|n|off)$", value):
            return False

        if re.match("^(true|t|yes|y|on)$", value):
            return True

        return value
