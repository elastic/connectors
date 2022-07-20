#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.sources.mongo import MongoDataSource
from connectors.source import DataSourceConfiguration


def test_get_configuration():

    klass = MongoDataSource

    # make sure the config can be read
    config = DataSourceConfiguration(klass.get_default_configuration())
    assert config["host"] == "mongodb://127.0.0.1:27021"
