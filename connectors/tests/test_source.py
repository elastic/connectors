#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest import mock

import pytest

from connectors.filtering.validation import (
    BasicRuleAgainstSchemaValidator,
    BasicRuleNoMatchAllRegexValidator,
    BasicRulesSetSemanticValidator,
)
from connectors.source import (
    BaseDataSource,
    DataSourceConfiguration,
    Field,
    advanced_rules_present,
    extract_advanced_rules,
    get_data_sources,
    get_source_klass,
)

EMPTY_FILTERING = {}

ADVANCED_RULES_EMPTY = {"advanced_snippet": {}}

ADVANCED_RULES = {"db": {"table": "SELECT * FROM db.table"}}

ADVANCED_RULES_NON_EMPTY = {"advanced_snippet": ADVANCED_RULES}

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

RULES = [
    {
        "id": 1,
    }
]
BASIC_RULES_NON_EMPTY = {"rules": RULES}
ADVANCED_AND_BASIC_RULES_NON_EMPTY = {
    "advanced_snippet": {"db": {"table": "SELECT * FROM db.table"}},
    "rules": RULES,
}


def test_field():
    # stupid holder
    f = Field("name")
    assert f.label == "name"
    assert f.type == "str"


def test_field_convert():
    assert Field("name", value="1", type="int").value == 1
    assert Field("name", value="1.2", type="float").value == 1.2
    assert Field("name", value="YeS", type="bool").value
    assert Field("name", value="1,2,3", type="list").value == ["1", "2", "3"]
    assert not Field("name", value="false", type="bool").value


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


@pytest.mark.asyncio
@mock.patch("connectors.filtering.validation.FilteringValidator.validate")
async def test_validate_filter(validator_mock):
    validator_mock.return_value = "valid"

    assert await BaseDataSource.validate_filtering({}) == "valid"


@pytest.mark.parametrize(
    "filtering, should_advanced_rules_be_present",
    [
        (ADVANCED_RULES_NON_EMPTY, True),
        (ADVANCED_AND_BASIC_RULES_NON_EMPTY, True),
        (ADVANCED_RULES_EMPTY, False),
        (BASIC_RULES_NON_EMPTY, False),
        (EMPTY_FILTERING, False),
        (None, False),
    ],
)
def test_advanced_rules_present(filtering, should_advanced_rules_be_present):
    assert advanced_rules_present(filtering) == should_advanced_rules_be_present


@pytest.mark.parametrize(
    "filtering, expected_advanced_rules",
    (
        [
            (ADVANCED_RULES_NON_EMPTY, ADVANCED_RULES),
            (ADVANCED_AND_BASIC_RULES_NON_EMPTY, ADVANCED_RULES),
            (ADVANCED_RULES_EMPTY, {}),
            (BASIC_RULES_NON_EMPTY, {}),
            (EMPTY_FILTERING, {}),
            (None, {}),
        ]
    ),
)
def test_extract_advanced_rules(filtering, expected_advanced_rules):
    assert extract_advanced_rules(filtering) == expected_advanced_rules


@pytest.mark.asyncio
async def test_base_class():
    configuration = DataSourceConfiguration({})

    with pytest.raises(NotImplementedError):
        BaseDataSource(configuration=configuration)

    # default rule validators for every data source (order matters)
    assert BaseDataSource.basic_rules_validators() == [
        BasicRuleAgainstSchemaValidator,
        BasicRuleNoMatchAllRegexValidator,
        BasicRulesSetSemanticValidator,
    ]

    # should be empty as advanced rules are specific to a data source
    assert not len(BaseDataSource.advanced_rules_validators())

    # ABCs
    class DataSource(BaseDataSource):
        @classmethod
        def get_default_configuration(cls):
            return {
                "host": {
                    "value": "127.0.0.1",
                    "label": "Host",
                    "type": "str",
                },
                "port": {
                    "value": 3306,
                    "label": "Port",
                    "type": "int",
                },
                "direct": {
                    "value": True,
                    "label": "Direct connect",
                    "type": "bool",
                },
                "user": {
                    "value": "root",
                    "label": "Username",
                    "type": "str",
                },
            }

    ds = DataSource(configuration=configuration)
    ds.get_default_configuration()["port"]["value"] == 3306

    options = {"a": "1"}
    ds.tweak_bulk_options(options)
    assert options == {"a": "1"}

    # data we send back to kibana
    # we want to make sure we only send back label+value
    expected = {
        "host": {"label": "Host", "value": "127.0.0.1"},
        "port": {"label": "Port", "value": "3306"},
        "direct": {"label": "Direct connect", "value": "true"},
        "user": {"label": "Username", "value": "root"},
    }
    assert ds.get_simple_configuration() == expected

    with pytest.raises(NotImplementedError):
        await ds.ping()

    await ds.close()

    with pytest.raises(NotImplementedError):
        await ds.get_docs()
