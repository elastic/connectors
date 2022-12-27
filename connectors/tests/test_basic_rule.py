#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import datetime

from connectors.filtering.basic_rule import BasicRule, Policy, Rule, try_coerce

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

DOCUMENT = {
    DESCRIPTION_KEY: DESCRIPTION_VALUE,
    AMOUNT_FLOAT_KEY: AMOUNT_FLOAT_VALUE,
    AMOUNT_INT_KEY: AMOUNT_INT_VALUE,
    CREATED_AT_DATE_KEY: CREATED_AT_DATE_VALUE,
    CREATED_AT_DATETIME_KEY: CREATED_AT_DATETIME_VALUE,
}


def test_matches_default_rule():
    assert BasicRule.default_rule().matches(DOCUMENT)


def test_no_field_leads_to_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field="non existing",
        rule=Rule.EQUALS,
        value="",
    )

    assert not basic_rule.matches(DOCUMENT)


def test_starts_with_string_matches():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.STARTS_WITH,
        value=DESCRIPTION_VALUE[:1],
    )

    assert basic_rule.matches(DOCUMENT)


def test_starts_with_string_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.STARTS_WITH,
        value="d",
    )

    assert not basic_rule.matches(DOCUMENT)


def test_ends_with_string_matches():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.ENDS_WITH,
        value=DESCRIPTION_VALUE[1:],
    )

    assert basic_rule.matches(DOCUMENT)


def test_ends_with_string_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.ENDS_WITH,
        value="d",
    )

    assert not basic_rule.matches(DOCUMENT)


def test_contains_with_string_matches():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.CONTAINS,
        value=DESCRIPTION_VALUE[1:2],
    )

    assert basic_rule.matches(DOCUMENT)


def test_contains_with_string_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.CONTAINS,
        value="d",
    )

    assert not basic_rule.matches(DOCUMENT)


def test_regex_matches():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.REGEX,
        value="^a",
    )

    assert basic_rule.matches(DOCUMENT)


def test_regex_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.REGEX,
        value="^d",
    )

    assert not basic_rule.matches(DOCUMENT)


def test_less_than_string_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="bcd",
    )

    assert basic_rule.matches(DOCUMENT)


def test_less_than_string_no_match_string_is_smaller_lexicographically():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="a",
    )

    assert not basic_rule.matches(DOCUMENT)


def test_less_than_string_no_match_string_is_the_same():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="abc",
    )

    assert not basic_rule.matches(DOCUMENT)


def test_less_than_integer_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_INT_VALUE + 5,
    )

    assert basic_rule.matches(DOCUMENT)


def test_less_than_integer_no_match_numbers_are_the_same():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_INT_VALUE,
    )

    assert not basic_rule.matches(DOCUMENT)


def test_less_than_integer_no_match_document_value_is_greater():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_INT_VALUE - 5,
    )

    assert not basic_rule.matches(DOCUMENT)


def test_less_than_float_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_FLOAT_VALUE + 5.0,
    )

    assert basic_rule.matches(DOCUMENT)


def test_less_than_float_no_match_numbers_are_the_same():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_FLOAT_VALUE,
    )

    assert not basic_rule.matches(DOCUMENT)


def test_less_than_float_no_match_document_value_is_greater():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_FLOAT_VALUE - 5.0,
    )

    assert not basic_rule.matches(DOCUMENT)


def test_less_than_datetime_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.LESS_THAN,
        value=str(CREATED_AT_DATETIME_VALUE + datetime.timedelta(days=10)),
    )

    assert basic_rule.matches(DOCUMENT)


def test_less_than_datetime_no_match_same_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.LESS_THAN,
        value=str(CREATED_AT_DATETIME_VALUE),
    )

    assert not basic_rule.matches(DOCUMENT)


def test_less_than_datetime_no_match_later_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.LESS_THAN,
        value=str(CREATED_AT_DATETIME_VALUE - datetime.timedelta(days=10)),
    )

    assert not basic_rule.matches(DOCUMENT)


def test_less_than_date_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.LESS_THAN,
        value=str(CREATED_AT_DATE_VALUE + datetime.timedelta(days=5)),
    )

    assert basic_rule.matches(DOCUMENT)


def test_less_than_date_no_match_same_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.LESS_THAN,
        value=str(CREATED_AT_DATE_VALUE),
    )

    assert not basic_rule.matches(DOCUMENT)


def test_less_than_date_no_match_later_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.LESS_THAN,
        value=str(CREATED_AT_DATE_VALUE - datetime.timedelta(days=5)),
    )

    assert not basic_rule.matches(DOCUMENT)


def test_greater_than_string_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.GREATER_THAN,
        value="a",
    )

    assert basic_rule.matches(DOCUMENT)


def test_greater_than_string_no_match_string_is_greater_lexicographically():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.GREATER_THAN,
        value="bcd",
    )

    assert not basic_rule.matches(DOCUMENT)


def test_greater_than_string_no_match_string_is_the_same():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.LESS_THAN,
        value="abc",
    )

    assert not basic_rule.matches(DOCUMENT)


def test_greater_than_integer_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.GREATER_THAN,
        value=AMOUNT_INT_VALUE - 5,
    )

    assert basic_rule.matches(DOCUMENT)


def test_greater_than_integer_no_match_numbers_are_the_same():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.LESS_THAN,
        value=AMOUNT_INT_VALUE,
    )

    assert not basic_rule.matches(DOCUMENT)


def test_greater_than_integer_no_match_document_value_is_less():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.GREATER_THAN,
        value=AMOUNT_INT_VALUE + 5,
    )

    assert not basic_rule.matches(DOCUMENT)


def test_greater_than_float_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.GREATER_THAN,
        value=AMOUNT_FLOAT_VALUE - 10.0,
    )

    assert basic_rule.matches(DOCUMENT)


def test_greater_than_float_no_match_numbers_are_the_same():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.GREATER_THAN,
        value=AMOUNT_FLOAT_VALUE,
    )

    assert not basic_rule.matches(DOCUMENT)


def test_greater_than_float_no_match_document_value_is_greater():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.GREATER_THAN,
        value=AMOUNT_FLOAT_VALUE + 10.0,
    )

    assert not basic_rule.matches(DOCUMENT)


def test_greater_than_datetime_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.GREATER_THAN,
        value=str(CREATED_AT_DATETIME_VALUE - datetime.timedelta(days=10)),
    )

    assert basic_rule.matches(DOCUMENT)


def test_greater_than_datetime_no_match_same_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.GREATER_THAN,
        value=str(CREATED_AT_DATETIME_VALUE),
    )

    assert not basic_rule.matches(DOCUMENT)


def test_greater_than_datetime_no_match_earlier_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.GREATER_THAN,
        value=str(CREATED_AT_DATETIME_VALUE + datetime.timedelta(days=10)),
    )

    assert not basic_rule.matches(DOCUMENT)


def test_greater_than_date_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.GREATER_THAN,
        value=str(CREATED_AT_DATE_VALUE - datetime.timedelta(days=5)),
    )

    assert basic_rule.matches(DOCUMENT)


def test_greater_than_date_no_match_same_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.GREATER_THAN,
        value=str(CREATED_AT_DATE_VALUE),
    )

    assert not basic_rule.matches(DOCUMENT)


def test_greater_than_date_no_match_earlier_time():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.GREATER_THAN,
        value=str(CREATED_AT_DATE_VALUE + datetime.timedelta(days=5)),
    )

    assert not basic_rule.matches(DOCUMENT)


def test_equals_integer_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.EQUALS,
        value=AMOUNT_INT_VALUE,
    )

    assert basic_rule.matches(DOCUMENT)


def test_equals_integer_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_INT_KEY,
        rule=Rule.EQUALS,
        value=AMOUNT_INT_VALUE + 5,
    )

    assert not basic_rule.matches(DOCUMENT)


def test_equals_float_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.EQUALS,
        value=AMOUNT_FLOAT_VALUE,
    )

    assert basic_rule.matches(DOCUMENT)


def test_equals_float_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=AMOUNT_FLOAT_KEY,
        rule=Rule.EQUALS,
        value=AMOUNT_FLOAT_VALUE + 5.0,
    )

    assert not basic_rule.matches(DOCUMENT)


def test_equals_string_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.EQUALS,
        value=DESCRIPTION_VALUE,
    )

    assert basic_rule.matches(DOCUMENT)


def test_equals_string_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=DESCRIPTION_KEY,
        rule=Rule.EQUALS,
        value=DESCRIPTION_VALUE[1:],
    )

    assert not basic_rule.matches(DOCUMENT)


def test_equals_datetime_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.EQUALS,
        value=str(CREATED_AT_DATETIME_VALUE),
    )

    assert basic_rule.matches(DOCUMENT)


def test_equals_datetime_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATETIME_KEY,
        rule=Rule.EQUALS,
        value=str(CREATED_AT_DATETIME_VALUE - datetime.timedelta(days=10)),
    )

    assert not basic_rule.matches(DOCUMENT)


def test_equals_date_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.EQUALS,
        value=str(CREATED_AT_DATE_VALUE),
    )

    assert basic_rule.matches(DOCUMENT)


def test_equals_date_no_match():
    basic_rule = BasicRule(
        id_=1,
        order=1,
        policy=Policy.INCLUDE,
        field=CREATED_AT_DATE_KEY,
        rule=Rule.EQUALS,
        value=str(CREATED_AT_DATE_VALUE - datetime.timedelta(days=15)),
    )

    assert not basic_rule.matches(DOCUMENT)


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
