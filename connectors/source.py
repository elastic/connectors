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
from functools import cache
from pydoc import locate

from bson import Decimal128

from connectors.content_extraction import ContentExtraction
from connectors.filtering.validation import (
    BasicRuleAgainstSchemaValidator,
    BasicRuleNoMatchAllRegexValidator,
    BasicRulesSetSemanticValidator,
    FilteringValidator,
)
from connectors.logger import logger
from connectors.utils import hash_id

DEFAULT_CONFIGURATION = {
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

TYPE_DEFAULTS = {
    str: "",
    int: None,
    float: None,
    bool: None,
    list: [],
}


class ValidationTypes(Enum):
    LESS_THAN = "less_than"
    GREATER_THAN = "greater_than"
    LIST_TYPE = "list_type"
    INCLUDED_IN = "included_in"
    REGEX = "regex"
    UNSET = None


class Field:
    def __init__(
        self,
        name,
        default_value=None,
        depends_on=None,
        label=None,
        required=True,
        field_type="str",
        validations=None,
        value=None,
    ):
        if depends_on is None:
            depends_on = []
        if label is None:
            label = name
        if validations is None:
            validations = []

        self.default_value = self._convert(default_value, field_type)
        self.depends_on = depends_on
        self.label = label
        self.name = name
        self.required = required
        self._field_type = field_type
        self.validations = validations
        self._value = self._convert(value, field_type)

    @property
    def field_type(self):
        return self._field_type

    @field_type.setter
    def field_type(self, value):
        self._field_type = value
        self.value = self._convert(self.value, self.field_type)

    @property
    def value(self):
        """Returns either the `value` or `default_value` of a Field.
        The `default_value` will only be returned if the Field is not required
        and the `value` is empty.
        """
        if self.required:
            return self._value

        if self.is_value_empty():
            return self.default_value

        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def _convert(self, value, field_type_):
        cast_type = locate(field_type_)
        if cast_type not in TYPE_DEFAULTS:
            # unsupported type
            return value

        if isinstance(value, cast_type):
            return value

        # list requires special type casting
        if cast_type == list:
            if isinstance(value, str):
                return [item.strip() for item in value.split(",")] if value else []
            elif isinstance(value, int):
                return [value]
            elif isinstance(value, set):
                return list(value)
            elif isinstance(value, dict):
                return list(value.items())
            else:
                return [value] if value is not None else []

        if value is None or value == "":
            return TYPE_DEFAULTS[cast_type]

        return cast_type(value)

    def is_value_empty(self):
        """Checks if the `value` field is empty or not.
        This always checks `value` and never `default_value`.
        """
        value = self._value

        match value:
            case str():
                return value is None or value == ""
            case list():
                return (
                    value is None
                    or len(value) <= 0
                    or all([x in (None, "") for x in value])
                )
            case _:
                # int and bool
                return value is None

    def validate(self):
        """Used to validate the `value` of a Field using its `validations`.
        If `value` is empty and the field is not required,
        the validation is run on the `default_value` instead.

        Returns a list of errors as strings.
        If the list is empty, then the Field `value` is valid.
        """
        value = self.value
        label = self.label

        validation_errors = []

        for validation in self.validations:
            validation_type = validation["type"]
            constraint = validation["constraint"]

            match validation_type:
                case ValidationTypes.LESS_THAN.value:
                    if value < constraint:
                        # valid
                        continue
                    else:
                        validation_errors.append(
                            f"'{label}' value '{value}' should be less than {constraint}."
                        )
                case ValidationTypes.GREATER_THAN.value:
                    if value > constraint:
                        # valid
                        continue
                    else:
                        validation_errors.append(
                            f"'{label}' value '{value}' should be greater than {constraint}."
                        )
                case ValidationTypes.LIST_TYPE.value:
                    if not isinstance(value, list):
                        validation_errors.append(
                            f"Cannot list_type validate '{label}' because its value '{value}' is not a list."
                        )
                        continue

                    for item in value:
                        if (constraint == "str" and not isinstance(item, str)) or (
                            constraint == "int" and not isinstance(item, int)
                        ):
                            validation_errors.append(
                                f"'{label}' list value '{item}' should be of type {constraint}."
                            )
                case ValidationTypes.INCLUDED_IN.value:
                    if isinstance(value, list):
                        for item in value:
                            if item not in constraint:
                                validation_errors.append(
                                    f"'{label}' list value '{item}' should be one of {', '.join(str(x) for x in constraint)}."
                                )
                    else:
                        if value not in constraint:
                            validation_errors.append(
                                f"'{label}' list value '{value}' should be one of {', '.join(str(x) for x in constraint)}."
                            )
                case ValidationTypes.REGEX.value:
                    if not isinstance(value, str):
                        validation_errors.append(
                            f"Cannot regex validate '{label}' because '{value}' is not a string."
                        )
                        continue

                    if not re.fullmatch(constraint, value):
                        validation_errors.append(
                            f"'{label}' value '{value}' failed regex check {constraint}."
                        )

        return validation_errors


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
                        value.get("default_value", None),
                        value.get("depends_on", []),
                        value.get("label"),
                        value.get("required", True),
                        value.get("type", "str"),
                        value.get("validations", []),
                        value.get("value", None),
                    )
                else:
                    self.set_field(key, label=key.capitalize(), value=str(value))

    def set_defaults(self, default_config):
        for name, item in default_config.items():
            self._defaults[name] = item.get("value")
            if name in self._config:
                self._config[name].field_type = item["type"]

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
        self,
        name,
        default_value=None,
        depends_on=None,
        label=None,
        required=True,
        field_type="str",
        validations=None,
        value=None,
    ):
        self._config[name] = Field(
            name,
            default_value,
            depends_on,
            label,
            required,
            field_type,
            validations,
            value,
        )

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
                    f"'{field.label}' was not validated because its dependencies were not met."
                )
                continue

            if field.required and field.is_value_empty():
                # a value is invalid if it is both required and empty
                validation_errors.extend([f"'{field.label}' cannot be empty."])
                continue

            # finally check actual validations
            validation_errors.extend(field.validate())

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
                    f'\'{field.label}\' depends on configuration \'{dependency["field"]}\', but it does not exist.'
                )

            if self._config[dependency["field"]].value != dependency["value"]:
                return False

        return True


class BaseDataSource:
    """Base class, defines a loose contract."""

    name = None
    service_type = None
    basic_rules_enabled = True
    advanced_rules_enabled = False
    dls_enabled = False
    incremental_sync_enabled = False

    def __init__(self, configuration):
        # Initialize to the global logger
        self._logger = logger
        if not isinstance(configuration, DataSourceConfiguration):
            raise TypeError(
                f"Configuration expected type is {DataSourceConfiguration.__name__}, actual: {type(configuration).__name__}."
            )

        self.configuration = configuration
        self.configuration.set_defaults(self.get_default_configuration())
        self._features = None
        # A dictionary, the structure of which is connector dependent, to indicate a point where the sync is at
        self._sync_cursor = None

        if self.configuration.get("use_text_extraction_service"):
            self.extraction_service = ContentExtraction()
            self.download_dir = self.extraction_service.get_volume_dir()
        else:
            self.extraction_service = None
            self.download_dir = None

    def __str__(self):
        return f"Datasource `{self.__class__.name}`"

    def set_logger(self, logger_):
        self._logger = logger_
        self._set_internal_logger()

    def _set_internal_logger(self):
        # no op for BaseDataSource
        # if there are internal class (e.g. Client class) to which the logger need to be set,
        # this method needs to be implemented
        pass

    @classmethod
    def get_simple_configuration(cls):
        """Used to return the default config to Kibana"""
        res = {}

        for config_name, fields in cls.get_default_configuration().items():
            entry = DEFAULT_CONFIGURATION.copy()

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

    @classmethod
    def hash_id(cls, _id):
        """Called, when an `_id` is too long to be ingested into elasticsearch.

        This method can be overridden to execute a hash function on a document `_id`,
        which returns a hashed `_id` with a length below the elasticsearch `_id` size limit.
        On default it uses md5 for hashing an `_id`.
        """

        return hash_id(_id)

    @classmethod
    def features(cls):
        """Returns features available for the data source"""
        return {
            "sync_rules": {
                "basic": {
                    "enabled": cls.basic_rules_enabled,
                },
                "advanced": {
                    "enabled": cls.advanced_rules_enabled,
                },
            },
            "document_level_security": {
                "enabled": cls.dls_enabled,
            },
            "incremental_sync": {
                "enabled": cls.incremental_sync_enabled,
            },
        }

    def set_features(self, features):
        if self._features is not None:
            self._logger.warning(f"'_features' already set in {self.__class__.name}")
        self._logger.debug(f"Setting '_features' for {self.__class__.name}")
        self._features = features

    async def validate_filtering(self, filtering):
        """Execute all basic rule and advanced rule validators."""

        return await FilteringValidator(
            self.basic_rules_validators(),
            self.advanced_rules_validators(),
            self._logger,
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
        self.configuration.check_valid()

    def validate_config_fields(self):
        """ "Checks if any fields in a configuration are missing.
        If a field is missing, raises an error.
        Ignores additional non-standard fields.

        Args:
            default_config (dict): the default configuration for the connector
            current_config (dict): the currently existing configuration for the connector
        """

        default_config = self.get_simple_configuration()
        current_config = self.configuration.to_dict()

        missing_fields = list(set(default_config.keys()) - set(current_config.keys()))

        if len(missing_fields) > 0:
            raise MalformedConfigurationError(
                f'Connector has missing configuration fields: {", ".join(missing_fields)}'
            )

        return

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

    def access_control_query(self, access_control):
        raise NotImplementedError

    async def get_access_control(self):
        """Returns an asynchronous iterator on the permission documents present in the backend.

        Each document is a dictionary containing permission data indexed into a corresponding permissions index.
        """
        raise NotImplementedError

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

    async def get_docs_incrementally(self, sync_cursor, filtering=None):
        """Returns an iterator on all documents changed since the sync_cursor

        Each document is a tuple with:
        - a mapping with the data to index
        - a coroutine to download extra data (attachments)
        - an operation, can be one of index, update or delete

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

    def sync_cursor(self):
        """Returns the sync cursor of the current sync"""
        return self._sync_cursor

    @staticmethod
    def is_premium():
        """Returns True if this DataSource is a Premium (paid license gated) connector.
        Otherwise, returns False.

        NOTE modifying license key logic violates the Elastic License 2.0 that this code is licensed under
        """
        return False


@cache
def get_source_klass(fqn):
    """Converts a Fully Qualified Name into a class instance."""
    module_name, klass_name = fqn.split(":")
    logger.debug(f"Importing module {module_name}")
    module = importlib.import_module(module_name)
    return getattr(module, klass_name)


def get_source_klasses(config):
    """Returns an iterator of all registered sources."""
    for fqn in config["sources"].values():
        yield get_source_klass(fqn)


class ConfigurableFieldValueError(Exception):
    pass


class ConfigurableFieldDependencyError(Exception):
    pass


class MalformedConfigurationError(Exception):
    pass
