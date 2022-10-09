#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.source import (
    Field,
    DataSourceConfiguration,
    get_source_klass,
    get_data_sources,
)

CONFIG = {
    "host": {
        "value": "mongodb://127.0.0.1:27021",
        "label": "MongoDB Host",
        "type": "str",
    },
    "database": {
        "value": "sample_airbnb",
        "label": "MongoDB Database",
        "type": "str",
    },
    "collection": {
        "value": "listingsAndReviews",
        "label": "MongoDB Collection",
        "type": "str",
    },
}


def test_field():
    # stupid holder
    f = Field("name")
    assert f.label == "name"


def test_data_source_configuration():

    c = DataSourceConfiguration(CONFIG)
    assert c["database"] == "sample_airbnb"
    assert c.get_field("database").label == "MongoDB Database"
    assert sorted([f.name for f in c.get_fields()]) == sorted(CONFIG.keys())
    c.set_field("new", value="one")
    assert c["new"] == "one"


def test_default():
    c = DataSourceConfiguration(CONFIG)
    assert c.get("database") == "sample_airbnb"
    assert c.get("dd", 1) == 1


class MyConnector:
    id = "1"
    service_type = "yea"

    def __init__(self, *args):
        pass


def test_get_source_klass():
    assert get_source_klass("test_source:MyConnector") is MyConnector


def test_get_data_sources():
    settings = {
        "sources": {"yea": "test_source:MyConnector", "yea2": "test_source:MyConnector"}
    }

    sources = list(get_data_sources(settings))
    assert sources == [MyConnector, MyConnector]
