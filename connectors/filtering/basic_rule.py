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


def parse(basic_rules_json):
    """Parse a basic rules json array to BasicRule objects.

    Arguments:
    - `basic_rules_json`: an array of dicts or an empty array

    The parser works in the following way:
      - Map every raw basic rule in the json array to the corresponding BasicRule object
      - Filter out every basic rule, which returns true for is_default_rule()
      - Sort the result in ascending order according to their basic rule order (rules are executed in ascending order)
    """
    if not basic_rules_json:
        return []

    map_to_basic_rules_list = list(
        map(
            lambda basic_rule_json: BasicRule.from_json(basic_rule_json),
            basic_rules_json,
        )
    )

    return sorted(
        list(
            filter(
                lambda basic_rule: not basic_rule.is_default_rule(),
                map_to_basic_rules_list,
            )
        ),
        key=lambda basic_rule: basic_rule.order,
    )


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


class RuleMatchStats:
    """RuleMatchStats records how many documents a basic rule matched and which policy it used.

    It's an internal class and is not expected to be used outside the module.
    """

    def __init__(self, policy, matches_count):
        self.policy = policy
        self.matches_count = matches_count

    def __add__(self, other):
        if other is None:
            return self

        if isinstance(other, int):
            return RuleMatchStats(
                policy=self.policy, matches_count=self.matches_count + other
            )
        else:
            raise NotImplementedError(f"__add__ is not implemented for '{type(other)}'")

    def __eq__(self, other):
        return self.policy == other.policy and self.matches_count == other.matches_count


class BasicRuleEngine:
    """BasicRuleEngine matches a document against a list of basic rules in order.

    The main concern of the engine is to decide, whether a document should be ingested during a sync or not:
        - If a document matches a basic rule and the basic rule uses the `INCLUDE` policy the document will be ingested
        - If a document matches a basic rule and the basic rule uses the `EXCLUDE` policy the document won't be ingested

    It also records stats, which basic rule matched how many documents with a certain policy.
    """

    def __init__(self, rules):
        self.rules = rules
        self.rules_match_stats = {
            BasicRule.DEFAULT_RULE_ID: RuleMatchStats(Policy.INCLUDE, 0)
        }

    def should_ingest(self, document):
        """Check, whether a document should be ingested or not.

        By default, the document will be ingested, if it doesn't match any rule.

        Arguments:
        - `document`: document matched against the basic rules
        """
        if not self.rules:
            self.rules_match_stats[BasicRule.DEFAULT_RULE_ID] += 1
            return True

        for rule in self.rules:
            if not rule:
                continue

            if rule.matches(document):
                logger.debug(
                    f"Document (id: '{document.get('_id')}') matched basic rule (id: '{rule.id_}'). Document will be {rule.policy.value}d"
                )

                self.rules_match_stats.setdefault(
                    rule.id_, RuleMatchStats(rule.policy, 0)
                )
                self.rules_match_stats[rule.id_] += 1

                return rule.is_include()

        # default behavior: ingest document, if no rule matches ("default rule")
        self.rules_match_stats[BasicRule.DEFAULT_RULE_ID] += 1
        logger.debug(
            f"Document (id: '{document.get('_id')}') didn't match any basic rule. Document will be included"
        )
        return True


class InvalidRuleError(ValueError):
    pass


class Rule(Enum):
    EQUALS = 1
    STARTS_WITH = 2
    ENDS_WITH = 3
    CONTAINS = 4
    REGEX = 5
    GREATER_THAN = 6
    LESS_THAN = 7

    RULES = [EQUALS, STARTS_WITH, ENDS_WITH, CONTAINS, REGEX, GREATER_THAN, LESS_THAN]

    @classmethod
    def is_string_rule(cls, string):
        try:
            cls.from_string(string)
            return True
        except InvalidRuleError:
            return False

    @classmethod
    def from_string(cls, string):
        match string.casefold():
            case "equals":
                return Rule.EQUALS
            case "contains":
                return Rule.CONTAINS
            case "ends_with":
                return Rule.ENDS_WITH
            case ">":
                return Rule.GREATER_THAN
            case "<":
                return Rule.LESS_THAN
            case "regex":
                return Rule.REGEX
            case "starts_with":
                return Rule.STARTS_WITH
            case _:
                raise InvalidRuleError(
                    f"'{string}' is an unknown value for the enum Rule. Allowed rules: {Rule.RULES}."
                )


class InvalidPolicyError(ValueError):
    pass


class Policy(Enum):
    INCLUDE = "include"
    EXCLUDE = "exclude"

    POLICIES = [INCLUDE, EXCLUDE]

    @classmethod
    def is_string_policy(cls, string):
        try:
            cls.from_string(string)
            return True
        except InvalidPolicyError:
            return False

    @classmethod
    def from_string(cls, string):
        match string.casefold():
            case "include":
                return Policy.INCLUDE
            case "exclude":
                return Policy.EXCLUDE
            case _:
                raise InvalidPolicyError(
                    f"'{string}' is an unknown value for the enum Policy. Allowed policies: {Policy.POLICIES}"
                )


class BasicRule:
    """A BasicRule is used to match documents based on different comparisons (see `matches` method)."""

    DEFAULT_RULE_ID = "DEFAULT"

    def __init__(self, id_, order, policy, field, rule, value):
        self.id_ = id_
        self.order = order
        self.policy = policy
        self.field = field
        self.rule = rule
        self.value = value

    @classmethod
    def from_json(cls, basic_rule_json):
        return cls(
            id_=basic_rule_json["id"],
            order=basic_rule_json["order"],
            policy=Policy.from_string(basic_rule_json["policy"]),
            field=basic_rule_json["field"],
            rule=Rule.from_string(basic_rule_json["rule"]),
            value=basic_rule_json["value"],
        )

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
        """Check whether a document matches the basic rule.

        A basic rule matches or doesn't match a document based on the following comparisons:
            - STARTS_WITH: Does the document's field value start with the basic rule's value?
            - ENDS_WITH: Does the document's field value end with the basic rule's value?
            - CONTAINS: Does the document's field value contain the basic rule's value?
            - REGEX: Does the document's field value match the basic rule's regex?
            - LESS_THAN: Is the document's field value less than the basic rule's value?
            - GREATER_THAN: Is the document's field value greater than the basic rule's value?
            - EQUALS: Is the document's field value equal to the basic rule's value?

        If the basic rule is the default rule it's always a match (the default rule matches every document).
        If the field is not in the document it's always a no match.

        Arguments:
        - `document`: document to check, if it matches
        """
        if self.is_default_rule():
            return True

        if self.field not in document:
            return False

        document_value = document[self.field]
        coerced_rule_value = self.coerce_rule_value_based_on_document_value(
            document_value
        )

        match self.rule:
            case Rule.STARTS_WITH:
                return str(document_value).startswith(self.value)
            case Rule.ENDS_WITH:
                return str(document_value).endswith(self.value)
            case Rule.CONTAINS:
                return self.value in str(document_value)
            case Rule.REGEX:
                return re.match(self.value, str(document_value)) is not None
            case Rule.LESS_THAN:
                return document_value < coerced_rule_value
            case Rule.GREATER_THAN:
                return document_value > coerced_rule_value
            case Rule.EQUALS:
                return document_value == coerced_rule_value

    def is_default_rule(self):
        return self.id_ == BasicRule.DEFAULT_RULE_ID

    def is_include(self):
        return self.policy == Policy.INCLUDE

    def coerce_rule_value_based_on_document_value(self, doc_value):
        """Coerce the value inside the basic rule.

        This method tries to coerce the value inside the basic rule to the type used in the document.

        Arguments:
        - `doc_value`: value of the field in the document to coerce

        """
        try:
            match doc_value:
                case str():
                    return str(self.value)
                case bool():
                    return str(to_bool(self.value))
                case float() | int():
                    return float(self.value)
                case datetime.date() | datetime.datetime:
                    return to_datetime(self.value)
                case _:
                    return str(self.value)
        except ValueError as e:
            logger.debug(
                f"Failed to coerce value '{self.value}' ({type(self.value)}) based on document value '{doc_value}' ({type(doc_value)}) due to error: {type(e)}: {e}"
            )
            return str(self.value)
