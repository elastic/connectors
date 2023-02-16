#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import importlib
from datetime import date, datetime
from decimal import Decimal

from bson import Decimal128

from connectors.filtering.validation import (
    BasicRuleAgainstSchemaValidator,
    BasicRuleNoMatchAllRegexValidator,
    BasicRulesSetSemanticValidator,
    FilteringValidator,
)

""" Helpers to build sources + FQN-based Registry
"""


class Field:
    def __init__(self, name, label=None, value="", type="str"):
        if label is None:
            label = name
        self.name = name
        self.label = label
        self._type = type
        self.value = self._convert(value, type)

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

    def set_field(self, name, label=None, value="", type="str"):
        self._config[name] = Field(name, label, value, type)

    def get_field(self, name):
        return self._config[name]

    def get_fields(self):
        return self._config.values()

    def is_empty(self):
        return len(self._config) == 0

    def to_dict(self):
        return dict(self._raw_config)


class BaseDataSource:
    """Base class, defines a loose contract."""

    name = None
    service_type = None

    def __init__(self, configuration):
        self.configuration = configuration
        assert isinstance(self.configuration, DataSourceConfiguration)
        self.configuration.set_defaults(self.get_default_configuration())

    def __str__(self):
        return f"Datasource `{self.__class__.name}`"

    @classmethod
    def get_simple_configuration(cls):
        """Used to return the default config to Kibana"""
        res = {}
        for name, item in cls.get_default_configuration().items():
            entry = {"label": item.get("label", name.upper())}
            if item["value"] is None:
                entry["value"] = ""
            else:
                if isinstance(item["value"], bool):
                    entry["value"] = item["value"] and "true" or "false"
                else:
                    entry["value"] = str(item["value"])

            res[name] = entry
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
    result = {}
    for name, fqn in config["sources"].items():
        result[name] = get_source_klass(fqn)
    return result
