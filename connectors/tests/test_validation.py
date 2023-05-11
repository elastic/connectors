#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from unittest.mock import AsyncMock, Mock

import pytest

from connectors.filtering.basic_rule import BasicRule
from connectors.filtering.validation import (
    AdvancedRulesValidator,
    BasicRuleAgainstSchemaValidator,
    BasicRuleNoMatchAllRegexValidator,
    BasicRulesSetSemanticValidator,
    BasicRulesSetValidator,
    BasicRuleValidator,
    FilteringValidationResult,
    FilteringValidationState,
    FilteringValidator,
    FilterValidationError,
    SyncRuleValidationResult,
)
from connectors.protocol import Filter

RULE_ONE_ID = 1
RULE_ONE_VALIDATION_MESSAGE = "rule 1 is valid"

RULE_TWO_ID = 2
RULE_TWO_VALIDATION_MESSAGE = "rule 2 is invalid"

RULE_THREE_ID = 3
RULE_THREE_VALIDATION_MESSAGE = "rule 3 is valid"

RULE_FOUR_ID = 4
RULE_FOUR_VALIDATION_MESSAGE = "rule 4 is invalid"

ADVANCED_RULE_ONE_ID = 5
ADVANCED_RULE_ONE_VALIDATION_MESSAGE = "rule 5 is valid"

ADVANCED_RULE_TWO_ID = 6
ADVANCED_RULE_TWO_VALIDATION_MESSAGE = "rule 6 is invalid"

RULE_ONE = {
    "id": RULE_ONE_ID,
    "order": 1,
    "policy": "include",
    "field": "field",
    "rule": "equals",
    "value": "value",
}

RULE_TWO = {
    "id": RULE_TWO_ID,
    "order": 2,
    "policy": "include",
    "field": "field",
    "rule": "equals",
    "value": "value",
}

FILTERING_ONE_BASIC_RULE_WITH_ADVANCED_RULE = Filter(
    {
        "rules": [RULE_ONE],
        "advanced_snippet": {"value": {"query": {}}},
    }
)

FILTERING_TWO_BASIC_RULES_WITHOUT_ADVANCED_RULE = Filter(
    {"rules": [RULE_ONE, RULE_TWO]}
)


@pytest.mark.parametrize(
    "result_one, result_two, should_be_equal",
    [
        (
            SyncRuleValidationResult(RULE_ONE_ID, True, RULE_ONE_VALIDATION_MESSAGE),
            SyncRuleValidationResult(RULE_ONE_ID, True, RULE_ONE_VALIDATION_MESSAGE),
            True,
        ),
        (
            # ids differ
            SyncRuleValidationResult(RULE_ONE_ID, True, RULE_ONE_VALIDATION_MESSAGE),
            SyncRuleValidationResult(RULE_TWO_ID, True, RULE_ONE_VALIDATION_MESSAGE),
            False,
        ),
        (
            # is_valid differs
            SyncRuleValidationResult(RULE_ONE_ID, True, RULE_ONE_VALIDATION_MESSAGE),
            SyncRuleValidationResult(RULE_ONE_ID, False, RULE_ONE_VALIDATION_MESSAGE),
            False,
        ),
        (
            # messages differ
            SyncRuleValidationResult(RULE_ONE_ID, True, RULE_ONE_VALIDATION_MESSAGE),
            SyncRuleValidationResult(RULE_ONE_ID, True, RULE_TWO_VALIDATION_MESSAGE),
            False,
        ),
    ],
)
def test_sync_rule_validation_result_eq(result_one, result_two, should_be_equal):
    assert result_one == result_two if should_be_equal else result_one != result_two


def test_sync_rule_validation_result_eq_wrong_type():
    with pytest.raises(TypeError):
        assert (
            SyncRuleValidationResult(RULE_ONE_ID, True, RULE_ONE_VALIDATION_MESSAGE)
            != 42
        )


@pytest.mark.parametrize(
    "result_one, result_two, should_be_equal",
    [
        (
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
            True,
        ),
        (
            FilteringValidationResult(
                state=FilteringValidationState.VALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_ONE_ID], messages=[RULE_ONE_VALIDATION_MESSAGE]
                    )
                ],
            ),
            FilteringValidationResult(
                state=FilteringValidationState.VALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_ONE_ID], messages=[RULE_ONE_VALIDATION_MESSAGE]
                    )
                ],
            ),
            True,
        ),
        (
            FilteringValidationResult(
                state=FilteringValidationState.VALID,
                errors=[FilterValidationError(ids=[RULE_ONE_ID])],
            ),
            FilteringValidationResult(
                state=FilteringValidationState.VALID,
                errors=[FilterValidationError(ids=[RULE_ONE_ID])],
            ),
            True,
        ),
        (
            # is_valid differs
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
            FilteringValidationResult(
                state=FilteringValidationState.INVALID, errors=[]
            ),
            False,
        ),
        (
            # other object is None
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
            None,
            False,
        ),
        (
            # ids differ
            FilteringValidationResult(
                state=FilteringValidationState.VALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_ONE_ID], messages=[RULE_ONE_VALIDATION_MESSAGE]
                    )
                ],
            ),
            FilteringValidationResult(
                state=FilteringValidationState.VALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_TWO_ID], messages=[RULE_ONE_VALIDATION_MESSAGE]
                    )
                ],
            ),
            False,
        ),
        (
            # messages differ
            FilteringValidationResult(
                state=FilteringValidationState.VALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_ONE_ID], messages=[RULE_ONE_VALIDATION_MESSAGE]
                    )
                ],
            ),
            FilteringValidationResult(
                state=FilteringValidationState.VALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_ONE_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
                    )
                ],
            ),
            False,
        ),
        (
            # error not present
            FilteringValidationResult(
                state=FilteringValidationState.VALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_ONE_ID], messages=[RULE_ONE_VALIDATION_MESSAGE]
                    )
                ],
            ),
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
            False,
        ),
    ],
)
def test_filtering_validation_result_eq(result_one, result_two, should_be_equal):
    assert result_one == result_two if should_be_equal else result_one != result_two


@pytest.mark.parametrize(
    "error_one, error_two, should_be_equal",
    [
        (
            FilterValidationError(
                ids=[RULE_ONE_ID], messages=[RULE_ONE_VALIDATION_MESSAGE]
            ),
            FilterValidationError(
                ids=[RULE_ONE_ID], messages=[RULE_ONE_VALIDATION_MESSAGE]
            ),
            True,
        ),
        (
            FilterValidationError(ids=[RULE_ONE_ID], messages=[]),
            FilterValidationError(ids=[RULE_ONE_ID], messages=[]),
            True,
        ),
        (
            FilterValidationError(ids=[], messages=[RULE_ONE_VALIDATION_MESSAGE]),
            FilterValidationError(ids=[], messages=[RULE_ONE_VALIDATION_MESSAGE]),
            True,
        ),
        (
            FilterValidationError(ids=[], messages=[]),
            FilterValidationError(ids=[], messages=[]),
            True,
        ),
        (
            # ids differ
            FilterValidationError(
                ids=[RULE_ONE_ID], messages=[RULE_ONE_VALIDATION_MESSAGE]
            ),
            FilterValidationError(
                ids=[RULE_TWO_ID], messages=[RULE_ONE_VALIDATION_MESSAGE]
            ),
            False,
        ),
        (
            # messages differ
            FilterValidationError(
                ids=[RULE_ONE_ID], messages=[RULE_ONE_VALIDATION_MESSAGE]
            ),
            FilterValidationError(
                ids=[RULE_ONE_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
            ),
            False,
        ),
        (
            # other is None
            FilterValidationError(
                ids=[RULE_ONE_ID], messages=[RULE_ONE_VALIDATION_MESSAGE]
            ),
            None,
            False,
        ),
    ],
)
def test_filter_validation_error_eq(error_one, error_two, should_be_equal):
    assert error_one == error_two if should_be_equal else error_one != error_two


def test_valid_result_sync_rule():
    result = SyncRuleValidationResult.valid_result(RULE_ONE_ID)

    assert result.rule_id == RULE_ONE_ID
    assert result.is_valid


@pytest.mark.parametrize(
    "sync_rule_validation_results, expected_filtering_validation_result",
    [
        # one valid -> overall valid
        (
            [
                SyncRuleValidationResult(
                    rule_id=RULE_ONE_ID,
                    is_valid=True,
                    validation_message=RULE_ONE_VALIDATION_MESSAGE,
                )
            ],
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
        ),
        (
            [
                SyncRuleValidationResult(
                    rule_id=RULE_TWO_ID,
                    is_valid=False,
                    validation_message=RULE_TWO_VALIDATION_MESSAGE,
                )
            ],
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_TWO_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
                    )
                ],
            ),
        ),
        # one valid, one invalid -> overall invalid
        (
            [
                SyncRuleValidationResult(
                    rule_id=RULE_ONE_ID,
                    is_valid=True,
                    validation_message=RULE_ONE_VALIDATION_MESSAGE,
                ),
                SyncRuleValidationResult(
                    rule_id=RULE_TWO_ID,
                    is_valid=False,
                    validation_message=RULE_TWO_VALIDATION_MESSAGE,
                ),
            ],
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_TWO_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
                    )
                ],
            ),
        ),
        # two valid -> overall valid
        (
            [
                SyncRuleValidationResult(
                    rule_id=RULE_ONE_ID,
                    is_valid=True,
                    validation_message=RULE_ONE_VALIDATION_MESSAGE,
                ),
                SyncRuleValidationResult(
                    rule_id=RULE_THREE_ID,
                    is_valid=True,
                    validation_message=RULE_THREE_VALIDATION_MESSAGE,
                ),
            ],
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
        ),
        # two invalid -> overall invalid
        (
            [
                SyncRuleValidationResult(
                    rule_id=RULE_TWO_ID,
                    is_valid=False,
                    validation_message=RULE_TWO_VALIDATION_MESSAGE,
                ),
                SyncRuleValidationResult(
                    rule_id=RULE_FOUR_ID,
                    is_valid=False,
                    validation_message=RULE_FOUR_VALIDATION_MESSAGE,
                ),
            ],
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_TWO_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
                    ),
                    FilterValidationError(
                        ids=[RULE_FOUR_ID], messages=[RULE_FOUR_VALIDATION_MESSAGE]
                    ),
                ],
            ),
        ),
    ],
)
def test_filtering_validation_result(
    sync_rule_validation_results, expected_filtering_validation_result
):
    filtering_validation_result = FilteringValidationResult()

    for result in sync_rule_validation_results:
        filtering_validation_result += result

    assert filtering_validation_result == expected_filtering_validation_result


@pytest.mark.parametrize(
    "basic_rule_validation_results, advanced_rule_validation_results, expected_result",
    [
        (
            # one basic rule validator (valid) -> overall valid
            [
                SyncRuleValidationResult(
                    rule_id=RULE_ONE_ID,
                    is_valid=True,
                    validation_message=RULE_ONE_VALIDATION_MESSAGE,
                )
            ],
            [],
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
        ),
        (
            # one basic rule validator (invalid) -> overall invalid
            [
                SyncRuleValidationResult(
                    rule_id=RULE_TWO_ID,
                    is_valid=False,
                    validation_message=RULE_TWO_VALIDATION_MESSAGE,
                ),
            ],
            [],
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_TWO_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
                    )
                ],
            ),
        ),
        (
            # two basic rule validators (valid, invalid) -> overall invalid
            [
                SyncRuleValidationResult(
                    rule_id=RULE_ONE_ID,
                    is_valid=True,
                    validation_message=RULE_ONE_VALIDATION_MESSAGE,
                ),
                SyncRuleValidationResult(
                    rule_id=RULE_TWO_ID,
                    is_valid=False,
                    validation_message=RULE_TWO_VALIDATION_MESSAGE,
                ),
            ],
            [],
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_TWO_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
                    )
                ],
            ),
        ),
        (
            # two basic rule validators (valid, valid) -> overall valid
            [
                SyncRuleValidationResult(
                    rule_id=RULE_ONE_ID,
                    is_valid=True,
                    validation_message=RULE_ONE_VALIDATION_MESSAGE,
                ),
                SyncRuleValidationResult(
                    rule_id=RULE_THREE_ID,
                    is_valid=True,
                    validation_message=RULE_THREE_VALIDATION_MESSAGE,
                ),
            ],
            [],
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
        ),
        (
            # two basic rule validators (invalid, invalid) -> overall invalid
            [
                SyncRuleValidationResult(
                    rule_id=RULE_TWO_ID,
                    is_valid=False,
                    validation_message=RULE_TWO_VALIDATION_MESSAGE,
                ),
                SyncRuleValidationResult(
                    rule_id=RULE_FOUR_ID,
                    is_valid=False,
                    validation_message=RULE_FOUR_VALIDATION_MESSAGE,
                ),
            ],
            [],
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_TWO_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
                    ),
                    FilterValidationError(
                        ids=[RULE_FOUR_ID], messages=[RULE_FOUR_VALIDATION_MESSAGE]
                    ),
                ],
            ),
        ),
        (
            # one advanced rule validator (valid) -> overall valid
            [],
            [
                SyncRuleValidationResult(
                    rule_id=ADVANCED_RULE_ONE_ID,
                    is_valid=True,
                    validation_message=ADVANCED_RULE_ONE_VALIDATION_MESSAGE,
                )
            ],
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
        ),
        (
            # one advanced rule validator (invalid) -> overall invalid
            [],
            [
                SyncRuleValidationResult(
                    rule_id=ADVANCED_RULE_TWO_ID,
                    is_valid=False,
                    validation_message=ADVANCED_RULE_TWO_VALIDATION_MESSAGE,
                )
            ],
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[ADVANCED_RULE_TWO_ID],
                        messages=[ADVANCED_RULE_TWO_VALIDATION_MESSAGE],
                    )
                ],
            ),
        ),
        (
            # one basic rule and one advanced rule validator (valid, valid) -> overall valid
            [
                SyncRuleValidationResult(
                    rule_id=RULE_ONE_ID,
                    is_valid=True,
                    validation_message=RULE_ONE_VALIDATION_MESSAGE,
                )
            ],
            [
                SyncRuleValidationResult(
                    rule_id=ADVANCED_RULE_ONE_ID,
                    is_valid=True,
                    validation_message=ADVANCED_RULE_ONE_VALIDATION_MESSAGE,
                )
            ],
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
        ),
        (
            # one basic rule and one advanced rule validator (invalid, valid) -> overall invalid
            [
                SyncRuleValidationResult(
                    rule_id=RULE_TWO_ID,
                    is_valid=False,
                    validation_message=RULE_TWO_VALIDATION_MESSAGE,
                )
            ],
            [
                SyncRuleValidationResult(
                    rule_id=ADVANCED_RULE_ONE_ID,
                    is_valid=True,
                    validation_message=ADVANCED_RULE_ONE_VALIDATION_MESSAGE,
                )
            ],
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_TWO_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
                    )
                ],
            ),
        ),
        (
            # one basic rule and one advanced rule validator (invalid, valid) -> overall invalid
            [
                SyncRuleValidationResult(
                    rule_id=RULE_ONE_ID,
                    is_valid=True,
                    validation_message=RULE_ONE_VALIDATION_MESSAGE,
                )
            ],
            [
                SyncRuleValidationResult(
                    rule_id=ADVANCED_RULE_TWO_ID,
                    is_valid=False,
                    validation_message=ADVANCED_RULE_TWO_VALIDATION_MESSAGE,
                )
            ],
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[ADVANCED_RULE_TWO_ID],
                        messages=[ADVANCED_RULE_TWO_VALIDATION_MESSAGE],
                    )
                ],
            ),
        ),
        (
            # one basic rule and one advanced rule validator (invalid, invalid) -> overall invalid
            [
                SyncRuleValidationResult(
                    rule_id=RULE_TWO_ID,
                    is_valid=False,
                    validation_message=RULE_TWO_VALIDATION_MESSAGE,
                )
            ],
            [
                SyncRuleValidationResult(
                    rule_id=ADVANCED_RULE_TWO_ID,
                    is_valid=False,
                    validation_message=ADVANCED_RULE_TWO_VALIDATION_MESSAGE,
                )
            ],
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_TWO_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
                    ),
                    FilterValidationError(
                        ids=[ADVANCED_RULE_TWO_ID],
                        messages=[ADVANCED_RULE_TWO_VALIDATION_MESSAGE],
                    ),
                ],
            ),
        ),
        (
            # validators returning None
            [None],
            [None],
            # should be ignored without breaking
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
        ),
        (
            # one basic rule validator validates a set of rules and returns a list of results
            [
                # results from one set validator
                [
                    SyncRuleValidationResult(
                        rule_id=RULE_TWO_ID,
                        is_valid=False,
                        validation_message=RULE_TWO_VALIDATION_MESSAGE,
                    ),
                    SyncRuleValidationResult(
                        rule_id=ADVANCED_RULE_TWO_ID,
                        is_valid=False,
                        validation_message=ADVANCED_RULE_TWO_VALIDATION_MESSAGE,
                    ),
                ]
            ],
            [],
            # both results should be added as separate errors
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_TWO_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
                    ),
                    FilterValidationError(
                        ids=[ADVANCED_RULE_TWO_ID],
                        messages=[ADVANCED_RULE_TWO_VALIDATION_MESSAGE],
                    ),
                ],
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_filtering_validator(
    basic_rule_validation_results, advanced_rule_validation_results, expected_result
):
    basic_rule_validators = validator_fakes(basic_rule_validation_results)
    advanced_rule_validators = validator_fakes(
        advanced_rule_validation_results, is_basic_rule_validator=False
    )

    filtering_validator = FilteringValidator(
        basic_rule_validators, advanced_rule_validators
    )

    validation_result = await filtering_validator.validate(
        FILTERING_ONE_BASIC_RULE_WITH_ADVANCED_RULE
    )

    assert validation_result == expected_result

    assert_validators_called_with(
        basic_rule_validators, FILTERING_ONE_BASIC_RULE_WITH_ADVANCED_RULE["rules"]
    )
    assert_validators_called_with(
        advanced_rule_validators,
        FILTERING_ONE_BASIC_RULE_WITH_ADVANCED_RULE["advanced_snippet"],
    )


@pytest.mark.asyncio
async def test_filtering_validator_validate_when_advanced_rules_empty_then_skip_validation():
    invalid_validation_result = SyncRuleValidationResult(
        rule_id=RULE_TWO_ID,
        is_valid=False,
        validation_message=RULE_TWO_VALIDATION_MESSAGE,
    )

    advanced_rule_validators = validator_fakes(
        [invalid_validation_result],
        is_basic_rule_validator=False,
    )

    filtering_validator = FilteringValidator([], advanced_rule_validators)

    validation_result = await filtering_validator.validate(
        Filter({"basic_rules": [], "advanced_snippet": {"value": {}}})
    )

    assert validation_result.state == FilteringValidationState.VALID


def validator_fakes(results, is_basic_rule_validator=True):
    validators = []

    # validator 1 returns result 1, validator 2 returns result 2 ...
    for idx, result in enumerate(results):
        # We need to create different classes, otherwise we would always override the class method behavior
        # and every call would return the last result. The classes also need to inherit from the correct base class
        # so the issubclass checks in the validate function succeed

        if is_basic_rule_validator:
            validator_super_type = (
                BasicRulesSetValidator
                if isinstance(result, list)
                else BasicRuleValidator
            )
        else:
            validator_super_type = AdvancedRulesValidator

        validator_fake = type(
            f"fake_validator_{idx}",
            (validator_super_type,),
            {
                "validate": Mock(return_value=result)
                if is_basic_rule_validator
                else AsyncMock(return_value=result)
            },
        )
        validators.append(validator_fake)

    return validators


def assert_validators_called_with(validators, payload):
    for validator in validators:
        if issubclass(validator, BasicRulesSetValidator):
            validator.validate.assert_called_with(payload)
        elif isinstance(payload, list):
            for item in payload:
                validator.validate.assert_called_with(item)


@pytest.mark.parametrize(
    "basic_rules_validation_results, expected_result",
    [
        (
            [
                SyncRuleValidationResult(
                    rule_id=RULE_ONE_ID,
                    is_valid=True,
                    validation_message=RULE_ONE_VALIDATION_MESSAGE,
                ),
                SyncRuleValidationResult(
                    rule_id=RULE_THREE_ID,
                    is_valid=True,
                    validation_message=RULE_THREE_VALIDATION_MESSAGE,
                ),
            ],
            FilteringValidationResult(state=FilteringValidationState.VALID, errors=[]),
        ),
        (
            [
                SyncRuleValidationResult(
                    rule_id=RULE_ONE_ID,
                    is_valid=True,
                    validation_message=RULE_ONE_VALIDATION_MESSAGE,
                ),
                SyncRuleValidationResult(
                    rule_id=RULE_TWO_ID,
                    is_valid=False,
                    validation_message=RULE_TWO_VALIDATION_MESSAGE,
                ),
            ],
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_TWO_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
                    )
                ],
            ),
        ),
        (
            [
                SyncRuleValidationResult(
                    rule_id=RULE_TWO_ID,
                    is_valid=False,
                    validation_message=RULE_TWO_VALIDATION_MESSAGE,
                ),
                SyncRuleValidationResult(
                    rule_id=RULE_FOUR_ID,
                    is_valid=False,
                    validation_message=RULE_FOUR_VALIDATION_MESSAGE,
                ),
            ],
            FilteringValidationResult(
                state=FilteringValidationState.INVALID,
                errors=[
                    FilterValidationError(
                        ids=[RULE_TWO_ID], messages=[RULE_TWO_VALIDATION_MESSAGE]
                    ),
                    FilterValidationError(
                        ids=[RULE_FOUR_ID], messages=[RULE_FOUR_VALIDATION_MESSAGE]
                    ),
                ],
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_filtering_validator_multiple_basic_rules(
    basic_rules_validation_results, expected_result
):
    basic_rule_validator = BasicRulesSetValidator
    # side_effect: return result 1 on first call, result 2 on second call ...
    basic_rule_validator.validate = Mock(side_effect=[basic_rules_validation_results])

    filtering_validator = FilteringValidator([basic_rule_validator], [])

    validation_result = await filtering_validator.validate(
        FILTERING_TWO_BASIC_RULES_WITHOUT_ADVANCED_RULE
    )

    assert validation_result == expected_result


def basic_rule_json(merge_with=None, delete_keys=None):
    # Default arguments are mutable
    if delete_keys is None:
        delete_keys = []

    if merge_with is None:
        merge_with = {}

    basic_rule = {
        "id": "1",
        "policy": "include",
        "field": "field",
        "rule": ">",
        "value": "100",
        "order": 10,
    }

    # merge dicts
    basic_rule = basic_rule | merge_with

    for key in delete_keys:
        del basic_rule[key]

    return basic_rule


@pytest.mark.parametrize(
    "basic_rule, should_be_valid",
    [
        (basic_rule_json(), True),
        # default rule should be skipped for match all regex validation
        (basic_rule_json(merge_with={"id": BasicRule.DEFAULT_RULE_ID}), True),
        # valid regexps
        (basic_rule_json(merge_with={"rule": "regex", "value": "abc"}), True),
        # for other rule types it should be possible to use values which look like match all regexps
        (basic_rule_json(merge_with={"rule": "contains", "value": ".*"}), True),
        (basic_rule_json(merge_with={"rule": "contains", "value": "(.*)"}), True),
        # invalid rules
        (basic_rule_json(merge_with={"rule": "regex", "value": ".*"}), False),
        (basic_rule_json(merge_with={"rule": "regex", "value": "(.*)"}), False),
    ],
)
def test_basic_rule_validate_no_match_all_regex(basic_rule, should_be_valid):
    if should_be_valid:
        assert BasicRuleNoMatchAllRegexValidator.validate(basic_rule).is_valid
    else:
        assert not BasicRuleNoMatchAllRegexValidator.validate(basic_rule).is_valid


@pytest.mark.parametrize(
    "basic_rule, should_be_valid",
    [
        (basic_rule_json(merge_with={"policy": "include"}), True),
        (basic_rule_json(merge_with={"policy": "exclude"}), True),
        (basic_rule_json(merge_with={"rule": "equals"}), True),
        (basic_rule_json(merge_with={"rule": "contains"}), True),
        (basic_rule_json(merge_with={"rule": "starts_with"}), True),
        (basic_rule_json(merge_with={"rule": "ends_with"}), True),
        (basic_rule_json(merge_with={"rule": ">"}), True),
        (basic_rule_json(merge_with={"rule": "<"}), True),
        (basic_rule_json(merge_with={"rule": "regex"}), True),
        (basic_rule_json(delete_keys=["id"]), False),
        (basic_rule_json(delete_keys=["policy"]), False),
        (basic_rule_json(delete_keys=["field"]), False),
        (basic_rule_json(delete_keys=["rule"]), False),
        (basic_rule_json(delete_keys=["value"]), False),
        (basic_rule_json(delete_keys=["order"]), False),
        (
            # id empty
            basic_rule_json(merge_with={"id": ""}),
            False,
        ),
        (
            # field empty
            basic_rule_json(merge_with={"field": ""}),
            False,
        ),
        (
            # value empty
            basic_rule_json(merge_with={"value": ""}),
            False,
        ),
        (
            # order empty
            basic_rule_json(merge_with={"order": None}),
            False,
        ),
        (
            # invalid policy
            basic_rule_json(merge_with={"policy": "unknown"}),
            False,
        ),
        (
            # invalid rule
            basic_rule_json(merge_with={"rule": "unknown"}),
            False,
        ),
    ],
)
def test_basic_rule_against_schema_validation(basic_rule, should_be_valid):
    if should_be_valid:
        assert BasicRuleAgainstSchemaValidator.validate(basic_rule).is_valid
    else:
        assert not BasicRuleAgainstSchemaValidator.validate(basic_rule).is_valid


@pytest.mark.parametrize(
    "basic_rules, should_be_valid",
    [
        (
            [
                # not valid -> equal rules with different ids
                basic_rule_json(merge_with={"id": "1"}),
                basic_rule_json(merge_with={"id": "2"}),
            ],
            False,
        ),
        (
            [
                # not valid -> semantically equal rules with contradicting policies
                basic_rule_json(merge_with={"id": "1", "policy": "include"}),
                basic_rule_json(merge_with={"id": "2", "policy": "exclude"}),
            ],
            False,
        ),
        (
            [
                # valid -> different fields
                basic_rule_json(merge_with={"id": "1", "field": "field one"}),
                basic_rule_json(merge_with={"id": "2", "field": "field two"}),
            ],
            True,
        ),
        (
            [
                # valid -> different values
                basic_rule_json(merge_with={"id": "1", "value": "value one"}),
                basic_rule_json(merge_with={"id": "2", "value": "value two"}),
            ],
            True,
        ),
        (
            [
                # valid -> different rules
                basic_rule_json(merge_with={"id": "1", "rule": "contains"}),
                basic_rule_json(merge_with={"id": "2", "rule": "ends_with"}),
            ],
            True,
        ),
    ],
)
def test_basic_rules_set_no_conflicting_policies_validation(
    basic_rules, should_be_valid
):
    validation_results = BasicRulesSetSemanticValidator.validate(basic_rules)

    if should_be_valid:
        assert all(result.is_valid for result in validation_results)
    else:
        assert not any(result.is_valid for result in validation_results)

    validation_results_rule_ids = set(
        map(lambda result: result.rule_id, validation_results)
    )
    basic_rule_ids = set(map(lambda rule: rule["id"], basic_rules))

    assert validation_results_rule_ids == basic_rule_ids


@pytest.mark.parametrize(
    "string, expected_state",
    [
        ("valid", FilteringValidationState.VALID),
        ("invalid", FilteringValidationState.INVALID),
        ("edited", FilteringValidationState.EDITED),
    ],
)
def test_filtering_validation_state_from_string(string, expected_state):
    assert FilteringValidationState(string) == expected_state


@pytest.mark.asyncio
async def test_filtering_validator_validate_single_advanced_rules_validator():
    invalid_validation_result = SyncRuleValidationResult(
        rule_id=RULE_TWO_ID,
        is_valid=False,
        validation_message=RULE_TWO_VALIDATION_MESSAGE,
    )

    # single validator, not wrapped in a list
    advanced_rule_validator = validator_fakes(
        [invalid_validation_result],
        is_basic_rule_validator=False,
    )[0]

    filtering_validator = FilteringValidator([], advanced_rule_validator)

    validation_result = await filtering_validator.validate(
        Filter(
            {
                "basic_rules": [],
                "advanced_snippet": {"value": {"query": "SELECT * FROM table;"}},
            }
        )
    )

    assert validation_result.state == FilteringValidationState.INVALID
