#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
""" Helpers to build sources + FQN-based Registry
"""
import importlib


class Field:
    def __init__(self, name, label=None, value="", type="str"):
        if label is None:
            label = name
        self.label = label
        self.value = value
        self.type = type


class DataSourceConfiguration:
    """Holds the configuration needed by the source class"""

    def __init__(self, config):
        self._config = {}
        for key, value in config.items():
            self.set_field(
                key,
                value.get("label"),
                value.get("value", ""),
                value.get("type", "str"),
            )

    def __getitem__(self, key):
        return self._config[key].value

    def set_field(self, name, label=None, value="", type="str"):
        self._config[name] = Field(name, label, value, type)

    def get_field(self, name):
        return self._config[name]

    def get_fields(self):
        return self._config.values()


class BaseDataSource:
    """Base class, defines a lose contract."""

    def __init__(self, connector):
        self.connector = connector
        self.configuration = connector.configuration

    @classmethod
    def get_default_configuration(cls):
        """Returns a dict with a default configuration"""
        raise NotImplementedError

    async def changed(self):
        return True

    async def ping(self):
        raise NotImplementedError

    async def get_docs(self):
        raise NotImplementedError


def _get_klass(fqn):
    """Converts a Fully Qualified Name into a class instance."""
    module_name, klass_name = fqn.split(":")
    module = importlib.import_module(module_name)
    return getattr(module, klass_name)


_CACHED_SOURCES = {}


def get_data_source(connector, config):
    """Returns a source class instance, given a service type"""
    service_type = connector.service_type
    if service_type not in _CACHED_SOURCES:
        _CACHED_SOURCES[service_type] = _get_klass(config["sources"][service_type])(
            connector
        )
    return _CACHED_SOURCES[service_type]


def get_data_sources(config):
    """Returns an iterator of all registered sources."""
    for name, fqn in config["sources"].items():
        yield _get_klass(fqn)
