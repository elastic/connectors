#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime
from decimal import Decimal
from unittest import mock

import pytest
from bson import Decimal128

from connectors.filtering.validation import (
    BasicRuleAgainstSchemaValidator,
    BasicRuleNoMatchAllRegexValidator,
    BasicRulesSetSemanticValidator,
)
from connectors.source import (
    BaseDataSource,
    ConfigurationFieldEmptyError,
    ConfigurationFieldMissingError,
    DataSourceConfiguration,
    Field,
    get_source_klass,
    get_source_klass_dict,
    get_source_klasses,
    validate_non_empty_config_fields,
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

DATE_STRING_ISO_FORMAT = "2023-01-01T13:37:42+02:00"


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


def test_get_source_klasses():
    settings = {
        "sources": {"yea": "test_source:MyConnector", "yea2": "test_source:MyConnector"}
    }

    sources = list(get_source_klasses(settings))
    assert sources == [MyConnector, MyConnector]


def test_get_source_klass_dict():
    settings = {
        "sources": {"yea": "test_source:MyConnector", "yea2": "test_source:MyConnector"}
    }

    source_klass_dict = get_source_klass_dict(settings)
    assert source_klass_dict["yea"] == MyConnector
    assert source_klass_dict["yea2"] == MyConnector


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


@pytest.mark.asyncio
@mock.patch("connectors.filtering.validation.FilteringValidator.validate")
async def test_validate_filter(validator_mock):
    validator_mock.return_value = "valid"

    assert (
        await DataSource(configuration=DataSourceConfiguration({})).validate_filtering(
            {}
        )
        == "valid"
    )


@pytest.mark.asyncio
async def test_invalid_configuration_raises_error():
    configuration = {}

    with pytest.raises(TypeError) as e:
        DataSource(configuration=configuration)

    assert e.match(".*DataSourceConfiguration.*")  # expected
    assert e.match(".*dict.*")  # actual


@pytest.mark.asyncio
async def test_base_class():
    configuration = DataSourceConfiguration({})

    with pytest.raises(NotImplementedError):
        BaseDataSource(configuration=configuration)

    ds = DataSource(configuration=configuration)
    assert ds.get_default_configuration()["port"]["value"] == 3306

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

    # default rule validators for every data source (order matters)
    assert BaseDataSource.basic_rules_validators() == [
        BasicRuleAgainstSchemaValidator,
        BasicRuleNoMatchAllRegexValidator,
        BasicRulesSetSemanticValidator,
    ]

    # should be empty as advanced rules are specific to a data source
    assert not len(DataSource(configuration=configuration).advanced_rules_validators())


@pytest.mark.parametrize(
    "raw_doc, expected_doc",
    [
        (
            {
                "key_1": "value",
                "key_2": datetime.fromisoformat(DATE_STRING_ISO_FORMAT),
                "key_3": Decimal(1234),
                "key_4": Decimal128(Decimal("0.0005")),
                "key_5": bytes("value", "utf-8"),
            },
            {
                "key_1": "value",
                "key_2": DATE_STRING_ISO_FORMAT,
                "key_3": 1234,
                "key_4": Decimal("0.0005"),
                "key_5": "value",
            },
        ),
        (
            {
                "key_1": {
                    "nested_key_1": {
                        "nested_key_2": datetime.fromisoformat(DATE_STRING_ISO_FORMAT)
                    }
                }
            },
            {"key_1": {"nested_key_1": {"nested_key_2": DATE_STRING_ISO_FORMAT}}},
        ),
        (
            {"key_1": [datetime.fromisoformat(DATE_STRING_ISO_FORMAT), "abc", 123]},
            {"key_1": [DATE_STRING_ISO_FORMAT, "abc", 123]},
        ),
        ({}, {}),
    ],
)
@pytest.mark.asyncio
async def test_serialize(raw_doc, expected_doc):
    with mock.patch.object(
        BaseDataSource, "get_default_configuration", return_value={}
    ):
        source = BaseDataSource(DataSourceConfiguration(CONFIG))
        serialized_doc = source.serialize(raw_doc)

        assert serialized_doc.keys() == expected_doc.keys()

        for serialized_doc_key, expected_doc_key in zip(serialized_doc, expected_doc):
            assert serialized_doc[serialized_doc_key] == expected_doc[expected_doc_key]


@pytest.mark.parametrize(
    "config, names, expected_any_field_is_empty, expected_empty_fields",
    [
        ({"field_one": "value_one"}, [], False, []),
        ({"field_one": "value_one"}, ["field_one"], False, []),
        ({"field_one": ""}, ["field_one"], True, ["field_one"]),
    ],
)
def test_any_field_is_empty(
    config, names, expected_any_field_is_empty, expected_empty_fields
):
    any_field_is_empty, actual_empty_fields = DataSourceConfiguration(
        config
    ).any_field_is_empty(names)

    assert any_field_is_empty == expected_any_field_is_empty
    assert actual_empty_fields == expected_empty_fields


def test_fields_any_field_is_empty_raises_on_missing_field():
    config = DataSourceConfiguration({"field_one": "value_one"})

    with pytest.raises(ConfigurationFieldMissingError):
        config.any_field_is_empty("field_two")


def test_validate_non_empty_config_fields_raise_error():
    with pytest.raises(ConfigurationFieldEmptyError):
        validate_non_empty_config_fields(
            ["field_one"], DataSourceConfiguration({"field_one": ""})
        )


def test_validate_non_empty_config_fields_no_error():
    try:
        validate_non_empty_config_fields(
            ["field_one"], DataSourceConfiguration({"field_one": "value_one"})
        )
    except ConfigurationFieldEmptyError:
        raise AssertionError(f"Unexpected {type(ConfigurationFieldEmptyError)}")
