#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from copy import deepcopy
from enum import Enum

import fastjsonschema

from connectors.filtering.basic_rule import BasicRule, Policy, Rule
from connectors.logger import logger
from connectors.utils import Format


class ValidationTarget(Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    UNSET = None


class InvalidFilteringError(Exception):
    pass


class SyncRuleValidationResult:
    """Represent the validation result for a basic or an advanced rule."""

    ADVANCED_RULES = "advanced_snippet"

    def __init__(self, rule_id, is_valid, validation_message):
        self.rule_id = rule_id
        self.is_valid = is_valid
        self.validation_message = validation_message

    @classmethod
    def valid_result(cls, rule_id):
        return SyncRuleValidationResult(
            rule_id=rule_id, is_valid=True, validation_message="Valid rule"
        )

    def __eq__(self, other):
        if not isinstance(other, SyncRuleValidationResult):
            raise TypeError(
                f"Can't compare SyncRuleValidationResult with {type(other)}"
            )

        return (
            self.rule_id == other.rule_id
            and self.is_valid == other.is_valid
            and self.validation_message == other.validation_message
        )


class FilterValidationError:
    """Represent an error occurring during filtering validation.

    FilterValidationError can contain multiple ids/messages (f.e. if two basic rules are valid for themselves but
    not in the context of each other) -> both rules belong to one error.
    """

    def __init__(self, ids=None, messages=None):
        if ids is None:
            ids = []
        if messages is None:
            messages = []

        self.ids = ids
        self.messages = messages

    def __eq__(self, other):
        if other is None:
            return False

        return self.ids == other.ids and self.messages == other.messages

    def __str__(self):
        return f"(ids: {self.ids}, messages: {self.messages})"


class FilteringValidationState(Enum):
    VALID = "valid"
    INVALID = "invalid"
    EDITED = "edited"

    @classmethod
    def to_s(cls, value):
        match value:
            case FilteringValidationState.VALID:
                return "valid"
            case FilteringValidationState.INVALID:
                return "invalid"
            case FilteringValidationState.EDITED:
                return "edited"


class FilteringValidationResult:
    """Composed of multiple FilterValidationErrors.

    One FilteringValidationResult is composed of one or multiple FilterValidationErrors if the result is invalid.
    These errors will be derived from a single SyncRuleValidationResult which can be added to a FilteringValidationResult.
    """

    def __init__(self, state=FilteringValidationState.VALID, errors=None):
        if errors is None:
            errors = []

        self.state = state
        self.errors = errors

    def __add__(self, other):
        if other is None:
            return self

        if isinstance(other, SyncRuleValidationResult):
            if not other.is_valid:
                return FilteringValidationResult(
                    state=FilteringValidationState.INVALID,
                    errors=deepcopy(self.errors)
                    + [
                        FilterValidationError(
                            ids=[other.rule_id], messages=[other.validation_message]
                        )
                    ],
                )
            else:
                return self
        else:
            raise NotImplementedError(
                f"Result of type '{type(other)}' cannot be added to '{type(FilteringValidationResult)}'"
            )

    def __eq__(self, other):
        if other is None:
            return False

        return self.state == other.state and self.errors == other.errors

    def to_dict(self):
        return {
            "state": FilteringValidationState.to_s(self.state),
            "errors": [vars(error) for error in self.errors],
        }


class FilteringValidator:
    """Facade for basic and advanced rule validators.

    The FilteringValidator class acts as a facade for basic rule and advanced rule validators,
    calling their validate methods and aggregating the result in one FilteringValidationResult.
    """

    def __init__(
        self, basic_rules_validators=None, advanced_rules_validators=None, logger_=None
    ):
        self.basic_rules_validators = (
            [] if basic_rules_validators is None else basic_rules_validators
        )
        self.advanced_rules_validators = (
            [] if advanced_rules_validators is None else advanced_rules_validators
        )
        self._logger = logger_ or logger

    async def validate(self, filtering):
        def _is_valid_str(result):
            if result is None:
                return "Unknown (check validator implementation as it should never return 'None')"

            return "valid" if result.is_valid else "invalid"

        self._logger.info("Filtering validation started")
        basic_rules = filtering.basic_rules
        basic_rules_ids = [basic_rule["id"] for basic_rule in basic_rules]

        filtering_validation_result = FilteringValidationResult()

        for validator in self.basic_rules_validators:
            if issubclass(validator, BasicRulesSetValidator):
                # pass the whole set/list of rules at once (validate constraints between rules)
                results = validator.validate(basic_rules)
                for result in results:
                    filtering_validation_result += result

                    logger.debug(
                        f"Basic rules set: '{basic_rules_ids}' validation result (Validator: {validator.__name__}): {_is_valid_str(result)}"
                    )

            if issubclass(validator, BasicRuleValidator):
                for basic_rule in basic_rules:
                    # pass rule by rule (validate rule in isolation)
                    validator_result = validator.validate(basic_rule)
                    filtering_validation_result += validator_result

                    logger.debug(
                        f"{str(basic_rule)} validation result (Validator: {validator.__name__}): {_is_valid_str(validator_result)}"
                    )

        if filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            advanced_rules_validators = (
                self.advanced_rules_validators
                if isinstance(self.advanced_rules_validators, list)
                else [self.advanced_rules_validators]
            )

            for validator in advanced_rules_validators:
                filtering_validation_result += await validator.validate(advanced_rules)

        self._logger.debug(
            f"Filtering validation result: {filtering_validation_result.state}"
        )

        if filtering_validation_result.errors:
            self._logger.error(
                f"Filtering validation errors: {[str(error) for error in filtering_validation_result.errors]}"
            )

        return filtering_validation_result


class BasicRulesSetValidator:
    """Validate constraints between different rules."""

    @classmethod
    def validate(cls, rules):
        raise NotImplementedError


class BasicRulesSetSemanticValidator(BasicRulesSetValidator):
    """BasicRulesSetSemanticValidator can be used to validate that a set of filtering rules does not contain semantic duplicates.

    A semantic duplicate is defined as two basic rules having the same values for `field`, `rule` and `value`.
    Therefore, two basic rules are also seen as semantic duplicates, if their `policy` values differ.

    If a semantic duplicate is detected both rules will be marked as invalid.
    """

    @classmethod
    def validate(cls, rules):
        rules_dict = {}

        for rule in rules:
            basic_rule = BasicRule.from_json(rule)
            # we want to check whether another rule already uses the exact same values for 'field', 'rule' and 'value'
            # to detect semantic duplicates
            field_rule_value_hash = hash(
                (basic_rule.field, basic_rule.rule, basic_rule.value)
            )

            if field_rule_value_hash in rules_dict:
                semantic_duplicate = rules_dict[field_rule_value_hash]

                return cls.semantic_duplicates_validation_results(
                    basic_rule, semantic_duplicate
                )

            rules_dict[field_rule_value_hash] = basic_rule

        return [
            SyncRuleValidationResult.valid_result(rule_id=rule.id_)
            for rule in rules_dict.values()
        ]

    @classmethod
    def semantic_duplicates_validation_results(cls, basic_rule, semantic_duplicate):
        def semantic_duplicate_msg(rule_one, rule_two):
            return f"{format(rule_one, Format.SHORT.value)} is semantically equal to {format(rule_two, Format.SHORT.value)}."

        return [
            # We need two error messages to highlight both rules in the UI
            SyncRuleValidationResult(
                rule_id=basic_rule.id_,
                is_valid=False,
                validation_message=semantic_duplicate_msg(
                    basic_rule, semantic_duplicate
                ),
            ),
            SyncRuleValidationResult(
                rule_id=semantic_duplicate.id_,
                is_valid=False,
                validation_message=semantic_duplicate_msg(
                    semantic_duplicate, basic_rule
                ),
            ),
        ]


class BasicRuleValidator:
    """Validate a single rule in isolation."""

    @classmethod
    def validate(cls, rule):
        raise NotImplementedError


class BasicRuleNoMatchAllRegexValidator(BasicRuleValidator):
    """BasicRuleNoMatchAllRegexValidator can be used to check that a basic rule does not use a match all regex."""

    MATCH_ALL_REGEXPS = [".*", "(.*)"]

    @classmethod
    def validate(cls, basic_rule_json):
        basic_rule = BasicRule.from_json(basic_rule_json)
        # default rule uses match all regex, which is intended
        if basic_rule.is_default_rule():
            return SyncRuleValidationResult.valid_result(rule_id=basic_rule.id_)

        if basic_rule.rule == Rule.REGEX and any(
            match_all_regex == basic_rule.value
            for match_all_regex in BasicRuleNoMatchAllRegexValidator.MATCH_ALL_REGEXPS
        ):
            return SyncRuleValidationResult(
                rule_id=basic_rule.id_,
                is_valid=False,
                validation_message=f"{format(basic_rule, Format.SHORT.value)} uses a match all regexps {BasicRuleNoMatchAllRegexValidator.MATCH_ALL_REGEXPS}, which are not allowed.",
            )

        return SyncRuleValidationResult.valid_result(rule_id=basic_rule.id_)


class BasicRuleAgainstSchemaValidator(BasicRuleValidator):
    """BasicRuleAgainstSchemaValidator can be used to check if basic rule follows specified json schema."""

    SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "id": {"type": "string", "minLength": 1},
            "policy": {"format": "policy"},
            "field": {"type": "string", "minLength": 1},
            "rule": {"format": "rule"},
            "value": {"type": "string", "minLength": 1},
            "order": {"type": "number", "minLength": 1},
        },
        "required": ["id", "policy", "field", "rule", "value", "order"],
    }

    CUSTOM_FORMATS = {
        "policy": lambda policy_string: Policy.is_string_policy(policy_string),
        "rule": lambda rule_string: Rule.is_string_rule(rule_string),
    }

    SCHEMA = fastjsonschema.compile(
        definition=SCHEMA_DEFINITION, formats=CUSTOM_FORMATS
    )

    @classmethod
    def validate(cls, rule):
        try:
            BasicRuleAgainstSchemaValidator.SCHEMA(rule)

            return SyncRuleValidationResult.valid_result(rule["id"])
        except fastjsonschema.JsonSchemaValueException as e:
            # id field could be missing
            rule_id = rule["id"] if "id" in rule else None

            return SyncRuleValidationResult(
                rule_id=rule_id, is_valid=False, validation_message=e.message
            )


class AdvancedRulesValidator:
    """Validate advanced rules."""

    def validate(self, advanced_rules):
        raise NotImplementedError
