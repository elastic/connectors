#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
""" Helpers to build sources + FQN-based Registry
"""

import importlib
import re
from datetime import date, datetime
from decimal import Decimal
from enum import Enum

from bson import Decimal128

from connectors.filtering.validation import (
    BasicRuleAgainstSchemaValidator,
    BasicRuleNoMatchAllRegexValidator,
    BasicRulesSetSemanticValidator,
    FilteringValidator,
)
from connectors.logger import logger


class ValidationTypes(Enum):
    LESS_THAN = "less_than"
    GREATER_THAN = "greater_than"
    LIST_TYPE = "list_type"
    INCLUDED_IN = "included_in"
    REGEX = "regex"
    UNSET = None


class Field:
    def __init__(
        self, name, label=None, value="", type="str", depends_on=None, validations=None
    ):
        if label is None:
            label = name
        if depends_on is None:
            depends_on = []
        if validations is None:
            validations = []

        self.name = name
        self.label = label
        self._type = type
        self.value = self._convert(value, type)
        self.depends_on = depends_on
        self.validations = validations

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value
        self.value = self._convert(self.value, self._type)

    def _convert(self, value, type_):
        if not isinstance(value, str):
            # we won't convert the value if it's not a str
            return value

        if type_ == "int":
            return int(value)
        elif type_ == "float":
            return float(value)
        elif type_ == "bool":
            return value.lower() in ("y", "yes", "true", "1")
        elif type_ == "list":
            return [item.strip() for item in value.split(",")]
        return value


class DataSourceConfiguration:
    """Holds the configuration needed by the source class"""

    def __init__(self, config):
        self._raw_config = config
        self._config = {}
        self._defaults = {}
        if self._raw_config is not None:
            for key, value in self._raw_config.items():
                if isinstance(value, dict):
                    self.set_field(
                        key,
                        value.get("label"),
                        value.get("value", ""),
                        value.get("type", "str"),
                        value.get("depends_on", []),
                        value.get("validations", []),
                    )
                else:
                    self.set_field(key, label=key.capitalize(), value=str(value))

    def set_defaults(self, default_config):
        for name, item in default_config.items():
            self._defaults[name] = item["value"]
            if name in self._config:
                self._config[name].type = item["type"]

    def __getitem__(self, key):
        if key not in self._config and key in self._defaults:
            return self._defaults[key]
        return self._config[key].value

    def get(self, key, default=None):
        if key not in self._config:
            return self._defaults.get(key, default)
        return self._config[key].value

    def has_field(self, name):
        return name in self._config

    def set_field(
        self, name, label=None, value="", type="str", depends_on=None, validations=None
    ):
        self._config[name] = Field(name, label, value, type, depends_on, validations)

    def get_field(self, name):
        return self._config[name]

    def get_fields(self):
        return self._config.values()

    def is_empty(self):
        return len(self._config) == 0

    def to_dict(self):
        return dict(self._raw_config)

    def check_valid(self):
        """Validates every Field against its `validations`.

        Raises ConfigurableFieldValueError if any validation errors are found.
        If no errors are raised, then everything is valid.
        """
        validation_errors = []

        for _, field in self._config.items():
            if not self.dependencies_satisfied(field):
                # we don't validate a field if its dependencies are not met
                logger.debug(
                    f"Field {field.label} was not validated because its dependencies were not met."
                )
                continue

            validation_errors.extend(self.validate_field(field))

        if len(validation_errors) > 0:
            raise ConfigurableFieldValueError(
                f"Field validation errors: {'; '.join(validation_errors)}"
            )

    def dependencies_satisfied(self, field):
        """Used to check if a Field has its dependencies satisfied.

        Returns True if all dependencies are satisfied, or no dependencies exist.
        Returns False if one or more dependencies are not satisfied.
        """
        if len(field.depends_on) <= 0:
            return True

        for dependency in field.depends_on:
            if dependency["field"] not in self._config:
                # cannot check dependency if field does not exist
                raise ConfigurableFieldDependencyError(
                    f'Configuration `{field.label}` depends on configuration `{dependency["field"]}`, but it does not exist.'
                )

            if self._config[dependency["field"]].value != dependency["value"]:
                return False

        return True

    def validate_field(self, field):
        """Used to validate the `value` of a Field using its `validations`.

        Returns a list of errors as strings.
        If the list is empty, then the Field `value` is valid.
        """
        value = field.value
        label = field.label

        validation_errors = []

        for validation in field.validations:
            validation_type = validation["type"]
            constraint = validation["constraint"]

            match validation_type:
                case ValidationTypes.LESS_THAN.value:
                    if value < constraint:
                        # valid
                        continue
                    else:
                        validation_errors.append(
                            f"`{label}` value `{value}` should be less than {constraint}."
                        )
                case ValidationTypes.GREATER_THAN.value:
                    if value > constraint:
                        # valid
                        continue
                    else:
                        validation_errors.append(
                            f"`{label}` value `{value}` should be greater than {constraint}."
                        )
                case ValidationTypes.LIST_TYPE.value:
                    for item in value:
                        if (constraint == "str" and not isinstance(item, str)) or (
                            constraint == "int" and not isinstance(item, int)
                        ):
                            validation_errors.append(
                                f"`{label}` list value `{item}` should be of type {constraint}."
                            )
                case ValidationTypes.INCLUDED_IN.value:
                    if field.type == "list":
                        for item in value:
                            if item not in constraint:
                                validation_errors.append(
                                    f"`{label}` list value `{item}` should be one of {', '.join(str(x) for x in constraint)}."
                                )
                    else:
                        if value not in constraint:
                            validation_errors.append(
                                f"`{label}` list value `{value}` should be one of {', '.join(str(x) for x in constraint)}."
                            )
                case ValidationTypes.REGEX.value:
                    if not re.fullmatch(constraint, value):
                        validation_errors.append(
                            f"`{label}` value `{value}` failed regex check {constraint}."
                        )

        return validation_errors


class BaseDataSource:
    """Base class, defines a loose contract."""

    name = None
    service_type = None

    def __init__(self, configuration):
        if not isinstance(configuration, DataSourceConfiguration):
            raise TypeError(
                f"Configuration expected type is {DataSourceConfiguration.__name__}, actual: {type(configuration).__name__}."
            )

        self.configuration = configuration
        self.configuration.set_defaults(self.get_default_configuration())

    def __str__(self):
        return f"Datasource `{self.__class__.name}`"

    @classmethod
    def get_simple_configuration(cls):
        """Used to return the default config to Kibana"""
        res = {}

        for config_name, fields in cls.get_default_configuration().items():
            entry = {
                "default_value": None,
                "depends_on": [],
                "display": "text",
                "label": "",
                "options": [],
                "order": 1,
                "required": True,
                "sensitive": False,
                "tooltip": None,
                "type": "str",
                "ui_restrictions": [],
                "validations": [],
                "value": "",
            }

            for field_property, value in fields.items():
                if field_property == "label":
                    entry[field_property] = value if value else config_name.upper()
                else:
                    entry[field_property] = value

            res[config_name] = entry
        return res

    @classmethod
    def get_default_configuration(cls):
        """Returns a dict with a default configuration"""
        raise NotImplementedError

    @classmethod
    def basic_rules_validators(cls):
        """Return default basic rule validators.

        Basic rule validators are executed in the order they appear in the list.
        Default behavior can be overridden completely or additional custom validators can be plugged in.
        """
        return [
            BasicRuleAgainstSchemaValidator,
            BasicRuleNoMatchAllRegexValidator,
            BasicRulesSetSemanticValidator,
        ]

    async def validate_filtering(self, filtering):
        """Execute all basic rule and advanced rule validators."""

        return await FilteringValidator(
            self.basic_rules_validators(), self.advanced_rules_validators()
        ).validate(filtering)

    def advanced_rules_validators(self):
        """Return advanced rule validators.

        Advanced rules validators are data source specific so there are no default validators.
        This method can be overridden to plug in custom advanced rule validators into the filtering validation.
        """
        return []

    async def changed(self):
        """When called, returns True if something has changed in the backend.

        Otherwise, returns False and the next sync is skipped.

        Some backends don't provide that information.
        In that case, this always return True.
        """
        return True

    async def validate_config(self):
        """When called, validates configuration of the connector that is contained in self.configuration

        If connector configuration is invalid, this method will raise an exception
        with human-friendly and actionable description
        """
        # TODO: when validate_config is implemented everywhere, we should make this method raise NotImplementedError
        pass

    async def ping(self):
        """When called, pings the backend

        If the backend has an issue, raises an exception
        """
        raise NotImplementedError

    async def close(self):
        """Called when the source is closed.

        Can be used to close connections
        """
        pass

    async def get_docs(self, filtering=None):
        """Returns an iterator on all documents present in the backend

        Each document is a tuple with:
        - a mapping with the data to index
        - a coroutine to download extra data (attachments)

        The mapping should have least an `id` field
        and optionally a `timestamp` field in ISO 8601 UTC

        The coroutine is called if the document needs to be synced
        and has attachments. It needs to return a mapping to index.

        It has two arguments: doit and timestamp
        If doit is False, it should return None immediately.
        If timestamp is provided, it should be used in the mapping.

        Example:

           async def get_file(doit=True, timestamp=None):
               if not doit:
                   return
               return {'TEXT': 'DATA', 'timestamp': timestamp,
                       'id': 'doc-id'}
        """
        raise NotImplementedError

    def tweak_bulk_options(self, options):
        """Receives the bulk options every time a sync happens, so they can be
        tweaked if needed.


        Returns None. The changes are done in-place
        """
        pass

    def serialize(self, doc):
        """Reads each element from the document and serializes it with respect to its datatype.

        Args:
            doc (Dict): Dictionary to be serialized

        Returns:
            doc (Dict): Serialized version of dictionary
        """

        def _serialize(value):
            """Serialize input value with respect to its datatype.
            Args:
                value (Any Datatype): Value to be serialized

            Returns:
                value (Any Datatype): Serialized version of input value.
            """

            if isinstance(value, (list, tuple)):
                value = [_serialize(item) for item in value]
            elif isinstance(value, dict):
                for key, svalue in value.items():
                    value[key] = _serialize(svalue)
            elif isinstance(value, (datetime, date)):
                value = value.isoformat()
            elif isinstance(value, Decimal128):
                value = value.to_decimal()
            elif isinstance(value, (bytes, bytearray)):
                value = value.decode(errors="ignore")
            elif isinstance(value, Decimal):
                value = float(value)
            return value

        for key, value in doc.items():
            doc[key] = _serialize(value)

        return doc


def get_source_klass(fqn):
    """Converts a Fully Qualified Name into a class instance."""
    module_name, klass_name = fqn.split(":")
    module = importlib.import_module(module_name)
    return getattr(module, klass_name)


def get_source_klasses(config):
    """Returns an iterator of all registered sources."""
    for fqn in config["sources"].values():
        yield get_source_klass(fqn)


def get_source_klass_dict(config):
    """Returns a service type - source klass dictionary"""
    return {name: get_source_klass(fqn) for name, fqn in config["sources"].items()}


class ConfigurableFieldValueError(Exception):
    pass


class ConfigurableFieldDependencyError(Exception):
    pass
