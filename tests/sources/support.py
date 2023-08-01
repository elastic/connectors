#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.source import DEFAULT_CONFIGURATION, DataSourceConfiguration


def create_source(klass, **extras):
    config = klass.get_default_configuration()
    for k, v in extras.items():
        if k in config:
            config[k].update({"value": v})
        else:
            config[k] = DEFAULT_CONFIGURATION.copy() | {"value": v}

    return klass(configuration=DataSourceConfiguration(config))


async def assert_basics(klass, field, value):
    config = DataSourceConfiguration(klass.get_default_configuration())
    assert config[field] == value
    source = create_source(klass)
    await source.ping()
    await source.changed()


class AsyncSourceContextManager:
    def __init__(self, klass, **extras):
        self.klass = klass
        self.extras = extras
        self.source = None

    async def __aenter__(self):
        config = self.klass.get_default_configuration()
        for k, v in self.extras.items():
            if k in config:
                config[k].update({"value": v})
            else:
                config[k] = DEFAULT_CONFIGURATION.copy() | {"value": v}

        self.source = self.klass(configuration=DataSourceConfiguration(config))
        return self.source

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.source.close()
