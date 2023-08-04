#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from contextlib import asynccontextmanager

from connectors.source import DEFAULT_CONFIGURATION, DataSourceConfiguration


@asynccontextmanager
async def create_source(klass, **extras):
    config = klass.get_default_configuration()
    for k, v in extras.items():
        if k in config:
            config[k].update({"value": v})
        else:
            config[k] = DEFAULT_CONFIGURATION.copy() | {"value": v}

    source = klass(configuration=DataSourceConfiguration(config))
    yield source
    await source.close()


async def assert_basics(klass, field, value):
    config = DataSourceConfiguration(klass.get_default_configuration())
    assert config[field] == value
    async with create_source(klass) as source:
        await source.ping()
        await source.changed()
