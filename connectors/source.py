#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
""" Helpers to build sources
"""


class Field:
    def __init__(self, name, label=None, value="", type="str"):
        if label is None:
            label = name
        self.label = label
        self.value = value
        self.type = type


class Configuration:
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
    def __init__(self, connector):
        self.connector = connector
        self.configuration = connector.configuration

    @classmethod
    def get_default_configuration(cls):
        """Returns a dict with a default configuration"""
        raise NotImplementedError

    async def ping(self):
        raise NotImplementedError

    async def get_docs(self):
        raise NotImplementedError
