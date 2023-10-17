#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import datetime
import uuid

import pytest

from connectors.filtering.basic_rule import (
    BasicRule,
    BasicRuleEngine,
    Policy,
    Rule,
    RuleMatchStats,
    parse,
    try_coerce,
)
from connectors.utils import Format

BASIC_RULE_ONE_ID = "1"
BASIC_RULE_ONE_ORDER = 1
BASIC_RULE_ONE_POLICY = "include"
BASIC_RULE_ONE_FIELD = "field one"
BASIC_RULE_ONE_RULE = "equals"
BASIC_RULE_ONE_VALUE = "value one"

BASIC_RULE_ONE_JSON = {
    "id": BASIC_RULE_ONE_ID,
    "order": BASIC_RULE_ONE_ORDER,
    "policy": BASIC_RULE_ONE_POLICY,
    "field": BASIC_RULE_ONE_FIELD,
    "rule": BASIC_RULE_ONE_RULE,
    "value": BASIC_RULE_ONE_VALUE,
}

BASIC_RULE_TWO_ID = "2"
BASIC_RULE_TWO_ORDER = 2
BASIC_RULE_TWO_POLICY = "exclude"
BASIC_RULE_TWO_FIELD = "field two"
BASIC_RULE_TWO_RULE = "contains"
BASIC_RULE_TWO_VALUE = "value two"

BASIC_RULE_TWO_JSON = {
    "id": BASIC_RULE_TWO_ID,
    "order": BASIC_RULE_TWO_ORDER,
    "policy": BASIC_RULE_TWO_POLICY,
    "field": BASIC_RULE_TWO_FIELD,
    "rule": BASIC_RULE_TWO_RULE,
    "value": BASIC_RULE_TWO_VALUE,
}

BASIC_RULE_THREE_ID = "3"
BASIC_RULE_THREE_ORDER = 3
BASIC_RULE_THREE_POLICY = "include"
BASIC_RULE_THREE_FIELD = "field three"
BASIC_RULE_THREE_RULE = "contains"
BASIC_RULE_THREE_VALUE = "value three"

BASIC_RULE_THREE_JSON = {
    "id": BASIC_RULE_THREE_ID,
    "order": BASIC_RULE_THREE_ORDER,
    "policy": BASIC_RULE_THREE_POLICY,
    "field": BASIC_RULE_THREE_FIELD,
    "rule": BASIC_RULE_THREE_RULE,
    "value": BASIC_RULE_THREE_VALUE,
}

BASIC_RULE_DEFAULT_ID = "DEFAULT"
BASIC_RULE_DEFAULT_ORDER = 0
BASIC_RULE_DEFAULT_POLICY = "include"
BASIC_RULE_DEFAULT_FIELD = "_"
BASIC_RULE_DEFAULT_RULE = "equals"
BASIC_RULE_DEFAULT_VALUE = ".*"

BASIC_RULE_DEFAULT_JSON = {
    "id": BASIC_RULE_DEFAULT_ID,
    "order": BASIC_RULE_DEFAULT_ORDER,
    "policy": BASIC_RULE_DEFAULT_POLICY,
    "field": BASIC_RULE_DEFAULT_FIELD,
    "rule": BASIC_RULE_DEFAULT_RULE,
    "value": BASIC_RULE_DEFAULT_VALUE,
}

DESCRIPTION_KEY = "description"
DESCRIPTION_VALUE = "abc"

AMOUNT_FLOAT_KEY = "amount-float"
AMOUNT_FLOAT_VALUE = 100.0
AMOUNT_INT_KEY = "amount-int"
AMOUNT_INT_VALUE = 100

CREATED_AT_DATE_KEY = "created_at_date"
# we expect that the data sources parse their dates to datetime
CREATED_AT_DATE_VALUE = datetime.datetime(year=2022, month=1, day=1, hour=0, minute=0)
CREATED_AT_DATETIME_KEY = "created_at_datetime"
CREATED_AT_DATETIME_VALUE = datetime.datetime(
    year=2022, month=1, day=1, hour=5, minute=10, microsecond=5
)

DOCUMENT_ONE = {
    DESCRIPTION_KEY: DESCRIPTION_VALUE,
    AMOUNT_FLOAT_KEY: AMOUNT_FLOAT_VALUE,
    AMOUNT_INT_KEY: AMOUNT_INT_VALUE,
    CREATED_AT_DATE_KEY: CREATED_AT_DATE_VALUE,
    CREATED_AT_DATETIME_KEY: CREATED_AT_DATETIME_VALUE,
}

DOCUMENT_TWO = {
    DESCRIPTION_KEY: DESCRIPTION_VALUE[1:],
    AMOUNT_FLOAT_KEY: AMOUNT_FLOAT_VALUE,
    AMOUNT_INT_KEY: AMOUNT_INT_VALUE,
    CREATED_AT_DATE_KEY: CREATED_AT_DATE_VALUE,
    CREATED_AT_DATETIME_KEY: CREATED_AT_DATETIME_VALUE,
}

DOCUMENT_THREE = {
    DESCRIPTION_KEY: DESCRIPTION_VALUE[2:],
    AMOUNT_FLOAT_KEY: AMOUNT_FLOAT_VALUE,
    AMOUNT_INT_KEY: AMOUNT_INT_VALUE,
    CREATED_AT_DATE_KEY: CREATED_AT_DATE_VALUE,
    CREATED_AT_DATETIME_KEY: CREATED_AT_DATETIME_VALUE,
}

MATCHING_DOCUMENT_ONE_INCLUDE_RULE_ID = "1"

MATCHING_DOCUMENT_ONE_INCLUDE_RULE = BasicRule(
    id_=MATCHING_DOCUMENT_ONE_INCLUDE_RULE_ID,
    order=1,
    policy=Policy.INCLUDE,
    field=DESCRIPTION_KEY,
    rule=Rule.EQUALS,
    value=DESCRIPTION_VALUE,
)

MATCHING_DOCUMENT_ONE_AND_TWO_EXCLUDE_RULE_ID = "2"

MATCHING_DOCUMENT_ONE_AND_TWO_EXCLUDE_RULE = BasicRule(
    id_=MATCHING_DOCUMENT_ONE_AND_TWO_EXCLUDE_RULE_ID,
    order=2,
    policy=Policy.EXCLUDE,
    field=DESCRIPTION_KEY,
    rule=Rule.ENDS_WITH,
    value=DESCRIPTION_VALUE[1:],
)

NON_MATCHING_INCLUDE_RULE = BasicRule(
    id_="3",
    order=3,
    policy=Policy.INCLUDE,
    field=DESCRIPTION_KEY,
    rule=Rule.EQUALS,
    value=DESCRIPTION_VALUE[::-1],
)

NON_MATCHING_EXCLUDE_RULE = BasicRule(
    id_="4",
    order=4,
    policy=Policy.EXCLUDE,
    field=DESCRIPTION_KEY,
    rule=Rule.EQUALS,
    value=DESCRIPTION_VALUE[::-1],
)


@pytest.mark.parametrize(
    "increments, expected_count",
    [([1, 2, 3], 6), ([None, None, None], 0), ([2, None], 2)],
)
def test_rule_match_stats_increment(increments, expected_count):
    rule_match_stats = RuleMatchStats(Policy.INCLUDE, 0)

    for increment in increments:
        rule_match_stats += increment

    assert rule_match_stats.matches_count == expected_count


@pytest.mark.parametrize(
    "rule_match_stats, should_equal",
    [
        ([RuleMatchStats(Policy.INCLUDE, 1), RuleMatchStats(Policy.INCLUDE, 1)], True),
        ([RuleMatchStats(Policy.EXCLUDE, 1), RuleMatchStats(Policy.INCLUDE, 1)], False),
        ([RuleMatchStats(Policy.INCLUDE, 1), RuleMatchStats(Policy.INCLUDE, 2)], False),
        ([RuleMatchStats(Policy.INCLUDE, 1), RuleMatchStats(Policy.EXCLUDE, 2)], False),
    ],
)
def test_rule_match_stats_eq(rule_match_stats, should_equal):
    if should_equal:
        assert all(stats == rule_match_stats[0] for stats in rule_match_stats[1:])
    else:
        assert all(stats != rule_match_stats[0] for stats in rule_match_stats[1:])


@pytest.mark.parametrize(
    "documents_should_ingest_tuples, rules, expected_stats",
    [
        (
            [(DOCUMENT_ONE, True)],
            None,
            {BasicRule.DEFAULT_RULE_ID: RuleMatchStats(Policy.INCLUDE, 1)},
        ),
        (
            [(DOCUMENT_ONE, True)],
            [None],
            {BasicRule.DEFAULT_RULE_ID: RuleMatchStats(Policy.INCLUDE, 1)},
        ),
        (
            [(DOCUMENT_ONE, True)],
            [],
            {BasicRule.DEFAULT_RULE_ID: RuleMatchStats(Policy.INCLUDE, 1)},
        ),
        (
            [(DOCUMENT_ONE, True)],
            [NON_MATCHING_INCLUDE_RULE],
            {BasicRule.DEFAULT_RULE_ID: RuleMatchStats(Policy.INCLUDE, 1)},
        ),
        (
            [(DOCUMENT_ONE, True)],
            [NON_MATCHING_EXCLUDE_RULE],
            {BasicRule.DEFAULT_RULE_ID: RuleMatchStats(Policy.INCLUDE, 1)},
        ),
        (
            [(DOCUMENT_ONE, True)],
            [MATCHING_DOCUMENT_ONE_INCLUDE_RULE],
            {
                BasicRule.DEFAULT_RULE_ID: RuleMatchStats(Policy.INCLUDE, 0),
                MATCHING_DOCUMENT_ONE_INCLUDE_RULE_ID: RuleMatchStats(
                    Policy.INCLUDE, 1
                ),
            },
        ),
        (
            [(DOCUMENT_ONE, False)],
            [MATCHING_DOCUMENT_ONE_AND_TWO_EXCLUDE_RULE],
            {
                BasicRule.DEFAULT_RULE_ID: RuleMatchStats(Policy.INCLUDE, 0),
                MATCHING_DOCUMENT_ONE_AND_TWO_EXCLUDE_RULE_ID: RuleMatchStats(
                    Policy.EXCLUDE, 1
                ),
            },
        ),
        (
            [(DOCUMENT_ONE, True)],
            [
                # should ingest -> INCLUDE matches before EXCLUDE
                MATCHING_DOCUMENT_ONE_INCLUDE_RULE,
                MATCHING_DOCUMENT_ONE_AND_TWO_EXCLUDE_RULE,
            ],
            {
                BasicRule.DEFAULT_RULE_ID: RuleMatchStats(Policy.INCLUDE, 0),
                MATCHING_DOCUMENT_ONE_INCLUDE_RULE_ID: RuleMatchStats(
                    Policy.INCLUDE, 1
                ),
            },
        ),
        (
            [(DOCUMENT_ONE, False)],
            [
                # should NOT ingest -> EXCLUDE matches before INCLUDE
                MATCHING_DOCUMENT_ONE_AND_TWO_EXCLUDE_RULE,
                MATCHING_DOCUMENT_ONE_INCLUDE_RULE,
            ],
            {
                BasicRule.DEFAULT_RULE_ID: RuleMatchStats(Policy.INCLUDE, 0),
                MATCHING_DOCUMENT_ONE_AND_TWO_EXCLUDE_RULE_ID: RuleMatchStats(
                    Policy.EXCLUDE, 1
                ),
            },
        ),
        (
            [
                (DOCUMENT_ONE, True),
                (DOCUMENT_ONE, True),
                (DOCUMENT_TWO, False),
                (DOCUMENT_TWO, False),
                (DOCUMENT_TWO, False),
                (DOCUMENT_THREE, True),
            ],
            [
                MATCHING_DOCUMENT_ONE_INCLUDE_RULE,
                MATCHING_DOCUMENT_ONE_AND_TWO_EXCLUDE_RULE,
            ],
            {
                BasicRule.DEFAULT_RULE_ID: RuleMatchStats(Policy.INCLUDE, 1),
                MATCHING_DOCUMENT_ONE_INCLUDE_RULE_ID: RuleMatchStats(
                    Policy.INCLUDE, 2
                ),
                MATCHING_DOCUMENT_ONE_AND_TWO_EXCLUDE_RULE_ID: RuleMatchStats(
                    Policy.EXCLUDE, 3
                ),
            },
        ),
    ],
)
def test_engine_should_ingest(documents_should_ingest_tuples, rules, expected_stats):
    engine = BasicRuleEngine(rules)

    for document_should_ingest_tuple in documents_should_ingest_tuples:
        document, should_ingest = document_should_ingest_tuple

        if should_ingest:
            assert engine.should_ingest(document)
        else:
            assert not engine.should_ingest(document)

    assert all(
        expected_stats[key] == engine.rules_match_stats[key] for key in expected_stats
    )


def basic_rule_one_policy_and_rule_uppercase():
    basic_rule_uppercase = BASIC_RULE_ONE_JSON

    basic_rule_uppercase["rule"] = basic_rule_uppercase["rule"].upper()
    basic_rule_uppercase["policy"] = basic_rule_uppercase["policy"].upper()

    return basic_rule_uppercase


def contains_rule_one(basic_rules):
    return any(is_rule_one(basic_rule) for basic_rule in basic_rules)


def contains_rule_two(basic_rules):
    return any(is_rule_two(basic_rule) for basic_rule in basic_rules)


def contains_rule_three(basic_rules):
    return any(is_rule_three(basic_rule) for basic_rule in basic_rules)


def contains_default_rule(basic_rules):
    return any(is_default_rule(basic_rule) for basic_rule in basic_rules)


def is_rule_one(basic_rule):
    return (
        basic_rule.id_ == BASIC_RULE_ONE_ID
        and basic_rule.order == BASIC_RULE_ONE_ORDER
        and basic_rule.policy == Policy.from_string(BASIC_RULE_ONE_POLICY)
        and basic_rule.field == BASIC_RULE_ONE_FIELD
        and basic_rule.rule == Rule.from_string(BASIC_RULE_ONE_RULE)
        and basic_rule.value == BASIC_RULE_ONE_VALUE
    )


def is_rule_two(basic_rule):
    return (
        basic_rule.id_ == BASIC_RULE_TWO_ID
        and basic_rule.order == BASIC_RULE_TWO_ORDER
        and basic_rule.policy == Policy.from_string(BASIC_RULE_TWO_POLICY)
        and basic_rule.field == BASIC_RULE_TWO_FIELD
        and basic_rule.rule == Rule.from_string(BASIC_RULE_TWO_RULE)
        and basic_rule.value == BASIC_RULE_TWO_VALUE
    )


def is_rule_three(basic_rule):
    return (
        basic_rule.id_ == BASIC_RULE_THREE_ID
        and basic_rule.order == BASIC_RULE_THREE_ORDER
        and basic_rule.policy == Policy.from_string(BASIC_RULE_THREE_POLICY)
        and basic_rule.field == BASIC_RULE_THREE_FIELD
        and basic_rule.rule == Rule.from_string(BASIC_RULE_THREE_RULE)
        and basic_rule.value == BASIC_RULE_THREE_VALUE
    )


def is_default_rule(basic_rule):
    return (
        basic_rule.id_ == BASIC_RULE_DEFAULT_ID
        and basic_rule.order == BASIC_RULE_DEFAULT_ORDER
        and basic_rule.policy == Policy.from_string(BASIC_RULE_DEFAULT_POLICY)
        and basic_rule.field == BASIC_RULE_DEFAULT_FIELD
        and basic_rule.rule == Rule.from_string(BASIC_RULE_DEFAULT_RULE)
        and basic_rule.value == BASIC_RULE_DEFAULT_VALUE
    )


@pytest.mark.parametrize(
    "policy_string, expected_parsed_policy",
    [
        ("include", Policy.INCLUDE),
        ("INCLUDE", Policy.INCLUDE),
        ("iNcLuDe", Policy.INCLUDE),
        ("exclude", Policy.EXCLUDE),
        ("EXCLUDE", Policy.EXCLUDE),
        ("eXcLuDe", Policy.EXCLUDE),
    ],
)
def test_from_string_policy_factory_method(policy_string, expected_parsed_policy):
    assert Policy.from_string(policy_string) == expected_parsed_policy


@pytest.mark.parametrize(
    "policy_string, is_policy",
    [
        ("include", True),
        ("INCLUDE", True),
        ("iNcLuDe", True),
        ("exclude", True),
        ("EXCLUDE", True),
        ("eXcLuDe", True),
        ("unknown", False),
        ("inclusion", False),
        ("exclusion", False),
    ],
)
def test_string_is_policy(policy_string, is_policy):
    if is_policy:
        assert Policy.is_string_policy(policy_string)
    else:
        assert not Policy.is_string_policy(policy_string)


@pytest.mark.parametrize(
    "rule_string, expected_parsed_rule",
    [
        ("equals", Rule.EQUALS),
        ("EQUALS", Rule.EQUALS),
        ("eQuAlS", Rule.EQUALS),
        ("contains", Rule.CONTAINS),
        ("CONTAINS", Rule.CONTAINS),
        ("cOnTaInS", Rule.CONTAINS),
        ("ends_with", Rule.ENDS_WITH),
        ("ENDS_WITH", Rule.ENDS_WITH),
        ("eNdS_wItH", Rule.ENDS_WITH),
        (">", Rule.GREATER_THAN),
        ("<", Rule.LESS_THAN),
        ("regex", Rule.REGEX),
        ("REGEX", Rule.REGEX),
        ("rEgEx", Rule.REGEX),
        ("starts_with", Rule.STARTS_WITH),
        ("STARTS_WITH", Rule.STARTS_WITH),
        ("sTaRtS_wItH", Rule.STARTS_WITH),
    ],
)
def test_from_string_rule_factory_method(rule_string, expected_parsed_rule):
    assert Rule.from_string(rule_string) == expected_parsed_rule


@pytest.mark.parametrize(
    "rule_string, is_rule",
    [
        ("equals", True),
        ("EQUALS", True),
        ("eQuAlS", True),
        ("contains", True),
        ("CONTAINS", True),
        ("cOnTaInS", True),
        ("ends_with", True),
        ("ENDS_WITH", True),
        ("eNdS_wItH", True),
        (">", True),
        ("<", True),
        ("regex", True),
        ("REGEX", True),
        ("rEgEx", True),
        ("starts_with", True),
        ("STARTS_WITH", True),
        ("sTaRtS_wItH", True),
        ("unknown", False),
        ("starts with", False),
        ("ends with", False),
    ],
)
def test_is_string_rule(rule_string, is_rule):
    if is_rule:
        assert Rule.is_string_rule(rule_string)
    else:
        assert not Rule.is_string_rule(rule_string)


def test_raise_value_error_if_argument_cannot_be_parsed_to_policy():
    with pytest.raises(ValueError):
        Policy.from_string("unknown")


def test_raise_value_error_if_argument_cannot_be_parsed_to_rule():
    with pytest.raises(ValueError):
        Rule.from_string("unknown")


def test_from_json():
    basic_rule = BasicRule.from_json(BASIC_RULE_ONE_JSON)

    assert is_rule_one(basic_rule)


def test_parse_none_to_empty_array():
    raw_basic_rules = None

    assert len(parse(raw_basic_rules)) == 0


def test_parse_empty_basic_rules_to_empty_array():
    raw_basic_rules = []

    assert len(parse(raw_basic_rules)) == 0


def test_parse_one_raw_basic_rule_with_policy_and_rule_lowercase():
    raw_basic_rules = [BASIC_RULE_ONE_JSON]

    parsed_basic_rules = parse(raw_basic_rules)

    assert len(parsed_basic_rules) == 1
    assert contains_rule_one(parsed_basic_rules)


def test_parse_one_raw_basic_rule_with_policy_and_rule_uppercase():
    raw_basic_rules = [basic_rule_one_policy_and_rule_uppercase()]

    parsed_basic_rules = parse(raw_basic_rules)

    assert len(parsed_basic_rules) == 1
    assert contains_rule_one(parsed_basic_rules)


def test_parses_multiple_rules_correctly():
    raw_basic_rules = [BASIC_RULE_ONE_JSON, BASIC_RULE_TWO_JSON]

    parsed_basic_rules = parse(raw_basic_rules)

    assert len(parsed_basic_rules) == 2
    assert contains_rule_one(parsed_basic_rules)
    assert contains_rule_two(parsed_basic_rules)


def test_parser_rejects_default_rule():
    raw_basic_rules = [BASIC_RULE_DEFAULT_JSON, BASIC_RULE_ONE_JSON]

    parsed_basic_rules = parse(raw_basic_rules)

    assert len(parsed_basic_rules) == 1
    assert contains_rule_one(parsed_basic_rules)
    assert not contains_default_rule(parsed_basic_rules)


def test_rules_are_ordered_ascending_with_respect_to_the_order_property():
    raw_basic_rules = [BASIC_RULE_ONE_JSON, BASIC_RULE_THREE_JSON, BASIC_RULE_TWO_JSON]

    parsed_basic_rules = parse(raw_basic_rules)
    first_rule = parsed_basic_rules[0]
    second_rule = parsed_basic_rules[1]
    third_rule = parsed_basic_rules[2]

    assert len(parsed_basic_rules) == 3
    assert is_rule_one(first_rule)
    assert is_rule_two(second_rule)
    assert is_rule_three(third_rule)


def test_matches_default_rule():
    assert BasicRule.default_rule().matches(DOCUMENT_ONE)


def test_no_field_leads_to_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field="non existing",
        rule=Rule.EQUALS,
        value="",
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_starts_with_string_matches():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.STARTS_WITH,
        value=DESCRIPTION_VALUE[:1],
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_starts_with_string_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.STARTS_WITH,
        value="d",
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_ends_with_string_matches():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.ENDS_WITH,
        value=DESCRIPTION_VALUE[1:],
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_ends_with_string_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.ENDS_WITH,
        value="d",
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_contains_with_string_matches():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.CONTAINS,
        value=DESCRIPTION_VALUE[1:2],
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_contains_with_string_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.CONTAINS,
        value="d",
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_regex_matches():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.REGEX,
        value="^a",
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_regex_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.REGEX,
        value="^d",
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_less_than_string_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="bcd",
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_less_than_string_no_match_string_is_smaller_lexicographically():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="a",
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_less_than_string_no_match_string_is_the_same():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="abc",
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_less_than_integer_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_INT_VALUE + 5,
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_less_than_integer_no_match_numbers_are_the_same():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_INT_VALUE,
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_less_than_integer_no_match_document_value_is_greater():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_INT_VALUE - 5,
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_less_than_float_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_FLOAT_VALUE + 5.0,
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_less_than_float_no_match_numbers_are_the_same():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_FLOAT_VALUE,
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_less_than_float_no_match_document_value_is_greater():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_FLOAT_VALUE - 5.0,
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_less_than_datetime_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.LESS_THAN,
        value=str(CREATED_AT_DATETIME_VALUE + datetime.timedelta(days=10)),
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_less_than_datetime_no_match_same_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.LESS_THAN,
        value=str(CREATED_AT_DATETIME_VALUE),
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_less_than_datetime_no_match_later_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.LESS_THAN,
        value=str(CREATED_AT_DATETIME_VALUE - datetime.timedelta(days=10)),
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_less_than_date_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.LESS_THAN,
        value=str(CREATED_AT_DATE_VALUE + datetime.timedelta(days=5)),
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_less_than_date_no_match_same_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.LESS_THAN,
        value=str(CREATED_AT_DATE_VALUE),
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_less_than_date_no_match_later_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.LESS_THAN,
        value=str(CREATED_AT_DATE_VALUE - datetime.timedelta(days=5)),
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_string_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.GREATER_THAN,
        value="a",
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_string_no_match_string_is_greater_lexicographically():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.GREATER_THAN,
        value="bcd",
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_string_no_match_string_is_the_same():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="abc",
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_integer_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.GREATER_THAN,
        value=AMOUNT_INT_VALUE - 5,
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_integer_no_match_numbers_are_the_same():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_INT_VALUE,
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_integer_no_match_document_value_is_less():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.GREATER_THAN,
        value=AMOUNT_INT_VALUE + 5,
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_float_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.GREATER_THAN,
        value=AMOUNT_FLOAT_VALUE - 10.0,
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_float_no_match_numbers_are_the_same():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.GREATER_THAN,
        value=AMOUNT_FLOAT_VALUE,
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_float_no_match_document_value_is_greater():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.GREATER_THAN,
        value=AMOUNT_FLOAT_VALUE + 10.0,
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_datetime_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.GREATER_THAN,
        value=str(CREATED_AT_DATETIME_VALUE - datetime.timedelta(days=10)),
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_datetime_no_match_same_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.GREATER_THAN,
        value=str(CREATED_AT_DATETIME_VALUE),
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_datetime_no_match_earlier_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.GREATER_THAN,
        value=str(CREATED_AT_DATETIME_VALUE + datetime.timedelta(days=10)),
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_date_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.GREATER_THAN,
        value=str(CREATED_AT_DATE_VALUE - datetime.timedelta(days=5)),
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_date_no_match_same_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.GREATER_THAN,
        value=str(CREATED_AT_DATE_VALUE),
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_greater_than_date_no_match_earlier_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.GREATER_THAN,
        value=str(CREATED_AT_DATE_VALUE + datetime.timedelta(days=5)),
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_equals_integer_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.EQUALS,
        value=AMOUNT_INT_VALUE,
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_equals_integer_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.EQUALS,
        value=AMOUNT_INT_VALUE + 5,
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_equals_float_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.EQUALS,
        value=AMOUNT_FLOAT_VALUE,
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_equals_float_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.EQUALS,
        value=AMOUNT_FLOAT_VALUE + 5.0,
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_equals_string_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.EQUALS,
        value=DESCRIPTION_VALUE,
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_equals_string_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.EQUALS,
        value=DESCRIPTION_VALUE[1:],
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_equals_datetime_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.EQUALS,
        value=str(CREATED_AT_DATETIME_VALUE),
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_equals_datetime_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.EQUALS,
        value=str(CREATED_AT_DATETIME_VALUE - datetime.timedelta(days=10)),
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_equals_date_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.EQUALS,
        value=str(CREATED_AT_DATE_VALUE),
    )

    assert basic_rule.matches(DOCUMENT_ONE)


def test_equals_date_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.EQUALS,
        value=str(CREATED_AT_DATE_VALUE - datetime.timedelta(days=15)),
    )

    assert not basic_rule.matches(DOCUMENT_ONE)


def test_coerce_rule_value_to_str():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="string",
    )

    coerced_rule_value = basic_rule.coerce_rule_value_based_on_document_value(
        "also a string"
    )

    assert isinstance(coerced_rule_value, str)
    assert coerced_rule_value == "string"


def test_coerce_rule_value_to_float():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="1.0",
    )

    coerced_rule_value = basic_rule.coerce_rule_value_based_on_document_value(2.0)

    assert isinstance(coerced_rule_value, float)
    assert coerced_rule_value == 1.0


def test_coerce_rule_value_to_float_if_it_is_an_int():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="1",
    )

    coerced_rule_value = basic_rule.coerce_rule_value_based_on_document_value(2)

    assert isinstance(coerced_rule_value, float)
    assert coerced_rule_value == 1.0


def test_coerce_rule_value_to_bool():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="t",
    )

    coerced_rule_value = basic_rule.coerce_rule_value_based_on_document_value(True)

    assert isinstance(coerced_rule_value, str)
    assert bool(coerced_rule_value)


def test_coerce_rule_value_to_datetime_date_if_it_is_datetime():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value=str(datetime.datetime(year=2022, month=1, day=1)),
    )

    coerced_rule_value = basic_rule.coerce_rule_value_based_on_document_value(
        datetime.datetime(year=2023, month=2, day=2)
    )

    assert isinstance(coerced_rule_value, datetime.datetime)
    assert coerced_rule_value == datetime.datetime(year=2022, month=1, day=1)


def test_coerce_rule_value_to_datetime_date_if_it_is_date():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value=str(datetime.date(year=2022, month=1, day=1)),
    )

    coerced_rule_value = basic_rule.coerce_rule_value_based_on_document_value(
        datetime.date(year=2023, month=2, day=2)
    )

    assert isinstance(coerced_rule_value, datetime.date)
    assert coerced_rule_value == datetime.datetime(year=2022, month=1, day=1)


def test_coerce_rule_to_default_if_type_is_not_registered():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="something",
    )

    # dict is not registered as type case in the coerce function
    coerced_rule_value = basic_rule.coerce_rule_value_based_on_document_value(
        {"key": "value"}
    )

    assert isinstance(coerced_rule_value, str)
    assert coerced_rule_value == "something"


def test_coerce_rule_to_default_if_doc_value_type_not_matching_rule_value_type():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="something",
    )

    coerced_rule_value = basic_rule.coerce_rule_value_based_on_document_value(1)

    assert isinstance(coerced_rule_value, str)
    assert coerced_rule_value == "something"


def test_coerce_to_float():
    value = "0.001"

    assert isinstance(try_coerce(value), float)


def test_coerce_date_string_to_datetime():
    value = "03-10-2022"

    assert isinstance(try_coerce(value), datetime.datetime)


def test_coerce_datetime_string_to_datetime():
    value = "03-10-2022 10:00"

    assert isinstance(try_coerce(value), datetime.datetime)


def test_coerce_to_true():
    value = "true"
    coerced_value = try_coerce(value)

    assert isinstance(coerced_value, bool)
    assert coerced_value

    value = "t"
    coerced_value = try_coerce(value)

    assert isinstance(coerced_value, bool)
    assert coerced_value

    value = "yes"
    coerced_value = try_coerce(value)

    assert isinstance(coerced_value, bool)
    assert coerced_value

    value = "y"
    coerced_value = try_coerce(value)

    assert isinstance(coerced_value, bool)
    assert coerced_value

    value = "on"
    coerced_value = try_coerce(value)

    assert isinstance(coerced_value, bool)
    assert coerced_value


def test_coerce_to_false():
    value = ""
    coerced_value = try_coerce(value)

    assert isinstance(try_coerce(value), bool)
    assert not coerced_value

    value = "false"
    coerced_value = try_coerce(value)

    assert isinstance(try_coerce(value), bool)
    assert not coerced_value

    value = "f"
    coerced_value = try_coerce(value)

    assert isinstance(try_coerce(value), bool)
    assert not coerced_value

    value = "no"
    coerced_value = try_coerce(value)

    assert isinstance(try_coerce(value), bool)
    assert not coerced_value

    value = "n"
    coerced_value = try_coerce(value)

    assert isinstance(try_coerce(value), bool)
    assert not coerced_value

    value = "off"
    coerced_value = try_coerce(value)

    assert isinstance(try_coerce(value), bool)
    assert not coerced_value


def test_do_not_coerce_and_return_original_string():
    value = "value"
    coerced_value = try_coerce(value)

    assert isinstance(coerced_value, str)
    assert coerced_value == value


def test_is_include_for_include_policy():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="something",
    )

    assert basic_rule.is_include()


def test_is_not_include_for_exclude_policy():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.EXCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="something",
    )

    assert not basic_rule.is_include()


def test_basic_rule_str():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.EXCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="something",
    )

    assert (
        str(basic_rule)
        == f"Basic rule: id_: 1, order: 1, policy: exclude, field: {DESCRIPTION_KEY}, rule: less_than, value: something"
    )


def test_basic_rule_format():
    basic_rule = BasicRule(
        id_=str(uuid.UUID.bytes),
        order=1,
        policy=Policy.EXCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="something",
    )

    verbose_format_str = format(basic_rule, Format.VERBOSE.value)
    short_format_str = format(basic_rule, Format.SHORT.value)

    assert len(verbose_format_str) > len(short_format_str)
