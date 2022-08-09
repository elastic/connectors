#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.source import DataSourceConfiguration


class FakeConnector:
    def __init__(self, config):
        self.configuration = DataSourceConfiguration(config)


def create_source(klass):
    config = klass.get_default_configuration()
    conn = FakeConnector(config)
    return klass(conn)


async def assert_basics(klass, field, value):
    config = DataSourceConfiguration(klass.get_default_configuration())
    assert config[field] == value
    source = create_source(klass)
    await source.ping()
