#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime
from decimal import Decimal
from unittest import TestCase, mock

import pytest
from bson import Decimal128

from connectors.filtering.validation import (
    BasicRuleAgainstSchemaValidator,
    BasicRuleNoMatchAllRegexValidator,
    BasicRulesSetSemanticValidator,
)
from connectors.protocol import Features
from connectors.source import (
    BaseDataSource,
    ConfigurableFieldDependencyError,
    ConfigurableFieldValueError,
    DataSourceConfiguration,
    Field,
    MalformedConfigurationError,
    ValidationTypes,
    get_source_klass,
    get_source_klasses,
)

CONFIG = {
    "host": {
        "label": "MongoDB Host",
        "type": "str",
        "value": "mongodb://127.0.0.1:27021",
    },
    "database": {
        "label": "MongoDB Database",
        "type": "str",
        "value": "sample_airbnb",
    },
    "collection": {
        "label": "MongoDB Collection",
        "type": "str",
        "value": "listingsAndReviews",
    },
    "connector_nickname": {
        "default_value": "MongoDB Connector",
        "label": "Connector Nickname",
        "type": "str",
        "value": "",
    },
    "retry_count": {
        "default_value": 3,
        "label": "Retry Count",
        "type": "int",
        "value": None,
    },
    "tables": {
        "default_value": ["*"],
        "label": "List of Tables",
        "type": "list",
        "value": [""],
    },
}

DATE_STRING_ISO_FORMAT = "2023-01-01T13:37:42+02:00"


def test_field():
    # stupid holder
    f = Field("name")
    assert f.label == "name"
    assert f.field_type == "str"


def test_field_convert():
    assert Field("name", field_type="str").value == ""
    assert Field("name", value="", field_type="str").value == ""
    assert Field("name", value="1", field_type="str").value == "1"
    assert Field("name", value="foo", field_type="str").value == "foo"
    assert Field("name", value=None, field_type="str").value == ""
    assert (
        Field("name", value={"foo": "bar"}, field_type="str").value == "{'foo': 'bar'}"
    )

    assert Field("name", field_type="int").value is None
    assert Field("name", value="1", field_type="int").value == 1
    assert Field("name", value="", field_type="int").value is None
    assert Field("name", value=None, field_type="int").value is None

    assert Field("name", field_type="float").value is None
    assert Field("name", value="1.2", field_type="float").value == 1.2
    assert Field("name", value="", field_type="float").value is None
    assert Field("name", value=None, field_type="float").value is None

    assert Field("name", field_type="bool").value is None
    assert Field("name", value="foo", field_type="bool").value is True
    assert Field("name", value="", field_type="bool").value is None
    assert Field("name", value=None, field_type="bool").value is None

    assert Field("name", field_type="list").value == []
    assert Field("name", value="1", field_type="list").value == ["1"]
    assert Field("name", value="1,2,3", field_type="list").value == ["1", "2", "3"]
    assert Field("name", value=[1, 2], field_type="list").value == [1, 2]
    assert Field("name", value=0, field_type="list").value == [0]
    assert Field("name", value="", field_type="list").value == []
    assert Field("name", value=None, field_type="list").value == []
    assert Field("name", value=False, field_type="list").value == [False]
    assert Field("name", value={"foo": "bar"}, field_type="list").value == [
        ("foo", "bar")
    ]
    TestCase().assertCountEqual(
        Field("name", value={"foo", "bar"}, field_type="list").value, ["foo", "bar"]
    )

    # unsupported cases that aren't converted
    assert Field("name", value={"foo": "bar"}, field_type="dict").value == {
        "foo": "bar"
    }
    assert Field("name", value="not a dict", field_type="dict").value == "not a dict"


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


@pytest.mark.parametrize(
    "field_type, required, default_value, value, expected_value",
    [
        # these include examples that would not normally validate
        # that is because `.value` does care about validation, it only returns a value
        # strings
        ("str", True, "default", "input", "input"),
        ("str", True, "default", None, ""),
        ("str", True, "default", "", ""),
        ("str", False, "default", "input", "input"),
        ("str", False, "default", None, "default"),
        ("str", False, "default", "", "default"),
        # integers
        ("int", True, 3, 1, 1),
        ("int", True, 3, None, None),
        ("int", False, 3, 1, 1),
        ("int", False, 3, None, 3),
        # lists
        ("list", True, ["1", "2"], ["3", "4"], ["3", "4"]),
        ("list", True, ["1", "2"], [], []),
        ("list", True, ["1", "2"], [""], [""]),
        ("list", True, ["1", "2"], [None], [None]),
        ("list", True, ["1", "2"], None, []),
        ("list", False, ["1", "2"], ["3", "4"], ["3", "4"]),
        ("list", False, ["1", "2"], [], ["1", "2"]),
        ("list", False, ["1", "2"], [""], ["1", "2"]),
        ("list", False, ["1", "2"], [None], ["1", "2"]),
        ("list", False, ["1", "2"], None, ["1", "2"]),
    ],
)
def test_value_returns_correct_value(
    field_type, required, default_value, value, expected_value
):
    assert (
        Field(
            "name",
            field_type=field_type,
            required=required,
            default_value=default_value,
            value=value,
        ).value
        == expected_value
    )


class MyConnector:
    id = "1"  # noqa A003
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "config",
    [
        (
            # less_than
            {
                "port": {
                    "type": "int",
                    "value": 9,
                    "validations": [
                        {"type": ValidationTypes.LESS_THAN.value, "constraint": 10}
                    ],
                }
            }
        ),
        (
            # greater_than
            {
                "port": {
                    "type": "int",
                    "value": 11,
                    "validations": [
                        {"type": ValidationTypes.GREATER_THAN.value, "constraint": 10}
                    ],
                }
            }
        ),
        (
            # less_than and greater_than with both valid
            {
                "port": {
                    "type": "int",
                    "value": 5,
                    "validations": [
                        {"type": ValidationTypes.GREATER_THAN.value, "constraint": 0},
                        {"type": ValidationTypes.LESS_THAN.value, "constraint": 10},
                    ],
                }
            }
        ),
        (
            # list_type str
            {
                "option_list": {
                    "type": "list",
                    "value": ["option1", "option2", "option3"],
                    "validations": [
                        {"type": ValidationTypes.LIST_TYPE.value, "constraint": "str"},
                    ],
                }
            }
        ),
        (
            # list_type int
            {
                "option_list": {
                    "type": "list",
                    "value": [1, 2, 3],
                    "validations": [
                        {"type": ValidationTypes.LIST_TYPE.value, "constraint": "int"},
                    ],
                }
            }
        ),
        (
            # included_in when list
            {
                "option_list": {
                    "type": "list",
                    "value": ["option1", "option2"],
                    "validations": [
                        {
                            "type": ValidationTypes.INCLUDED_IN.value,
                            "constraint": ["option1", "option2"],
                        },
                    ],
                }
            }
        ),
        (
            # included_in when str
            {
                "option_str": {
                    "type": "str",
                    "value": "option2",
                    "validations": [
                        {
                            "type": ValidationTypes.INCLUDED_IN.value,
                            "constraint": ["option1", "option2"],
                        },
                    ],
                }
            }
        ),
        (
            # included_in when int
            {
                "option_int": {
                    "type": "int",
                    "value": 2,
                    "validations": [
                        {
                            "type": ValidationTypes.INCLUDED_IN.value,
                            "constraint": [1, 2],
                        },
                    ],
                }
            }
        ),
        (
            # regex
            {
                "email": {
                    "type": "str",
                    "value": "real@email.com",
                    "validations": [
                        {
                            "type": ValidationTypes.REGEX.value,
                            "constraint": "([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\\.[A-Z|a-z]{2,})+",
                        },
                    ],
                }
            }
        ),
        (
            # with dependencies met should attempt validation
            {
                "email": {
                    "type": "str",
                    "value": "real@email.com",
                    "validations": [
                        {
                            "type": ValidationTypes.REGEX.value,
                            "constraint": "([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\\.[A-Z|a-z]{2,})+",
                        },
                    ],
                    "depends_on": [{"field": "foo", "value": "bar"}],
                },
                "foo": {
                    "value": "bar",
                },
            }
        ),
        (
            # if dependencies are not met it should skip validation
            {
                "email": {
                    "type": "str",
                    "value": "invalid_email.com",
                    "validations": [
                        {
                            "type": ValidationTypes.REGEX.value,
                            "constraint": "([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\\.[A-Z|a-z]{2,})+",
                        },
                    ],
                    "depends_on": [{"field": "foo", "value": "bar"}],
                },
                "foo": {
                    "value": "not_bar",
                },
            }
        ),
        (
            # when not required and value is empty
            # it should pass validation if no validations exist
            {
                "string_field": {
                    "type": "str",
                    "required": False,
                    "value": "",
                    "validations": [],
                }
            }
        ),
        (
            {
                "string_field": {
                    "type": "str",
                    "required": False,
                    "value": None,
                    "validations": [],
                }
            }
        ),
        (
            {
                "int_field": {
                    "type": "int",
                    "required": False,
                    "value": None,
                    "validations": [],
                }
            }
        ),
        (
            {
                "list_field": {
                    "type": "list",
                    "required": False,
                    "value": [],
                    "validations": [],
                }
            }
        ),
        (
            {
                "list_field": {
                    "type": "list",
                    "required": False,
                    "value": None,
                    "validations": [],
                }
            }
        ),
        (
            {
                "bool_field": {
                    "type": "bool",
                    "required": False,
                    "value": None,
                    "validations": [],
                }
            }
        ),
    ],
)
async def test_check_valid_when_validations_succeed_no_errors_raised(config):
    c = DataSourceConfiguration(config)
    c.check_valid()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "config",
    [
        (
            # less_than
            {
                "port": {
                    "type": "int",
                    "value": 11,
                    "validations": [
                        {"type": ValidationTypes.LESS_THAN.value, "constraint": 10}
                    ],
                }
            }
        ),
        (
            # greater_than
            {
                "port": {
                    "type": "int",
                    "value": 9,
                    "validations": [
                        {"type": ValidationTypes.GREATER_THAN.value, "constraint": 10}
                    ],
                }
            }
        ),
        (
            # less_than and greater_than with one invalid
            {
                "port": {
                    "type": "int",
                    "value": 10,
                    "validations": [
                        {"type": ValidationTypes.GREATER_THAN.value, "constraint": 0},
                        {"type": ValidationTypes.LESS_THAN.value, "constraint": 10},
                    ],
                }
            }
        ),
        (
            # list_type str
            {
                "option_list": {
                    "type": "list",
                    "value": ["option1", "option2", 3],
                    "validations": [
                        {"type": ValidationTypes.LIST_TYPE.value, "constraint": "str"},
                    ],
                }
            }
        ),
        (
            # list_type int
            {
                "option_list": {
                    "type": "list",
                    "value": [1, 2, "option3"],
                    "validations": [
                        {"type": ValidationTypes.LIST_TYPE.value, "constraint": "int"},
                    ],
                }
            }
        ),
        (
            # included_in when list
            {
                "option_list": {
                    "type": "list",
                    "value": ["option1", "option2"],
                    "validations": [
                        {
                            "type": ValidationTypes.INCLUDED_IN.value,
                            "constraint": ["option1", "option3"],
                        },
                    ],
                }
            }
        ),
        (
            # included_in when str
            {
                "option_str": {
                    "type": "str",
                    "value": "option2",
                    "validations": [
                        {
                            "type": ValidationTypes.INCLUDED_IN.value,
                            "constraint": ["option1", "option3"],
                        },
                    ],
                }
            }
        ),
        (
            # included_in when int
            {
                "option_int": {
                    "type": "int",
                    "value": 2,
                    "validations": [
                        {
                            "type": ValidationTypes.INCLUDED_IN.value,
                            "constraint": [1, 3],
                        },
                    ],
                }
            }
        ),
        (
            # regex
            {
                "email": {
                    "type": "str",
                    "value": "not_an_email.com",
                    "validations": [
                        {
                            "type": ValidationTypes.REGEX.value,
                            "constraint": "([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\\.[A-Z|a-z]{2,})+",
                        },
                    ],
                }
            }
        ),
        (
            # with dependencies met should attempt validation
            {
                "email": {
                    "type": "str",
                    "value": "not_an_email.com",
                    "validations": [
                        {
                            "type": ValidationTypes.REGEX.value,
                            "constraint": "([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\\.[A-Z|a-z]{2,})+",
                        },
                    ],
                    "depends_on": [{"field": "foo", "value": "bar"}],
                },
                "foo": {
                    "value": "bar",
                },
            }
        ),
        (
            # when required and value is empty it should fail validation
            {
                "string_field": {
                    "type": "str",
                    "required": True,
                    "value": "",
                    "validations": [],
                }
            }
        ),
        (
            {
                "string_field": {
                    "type": "str",
                    "required": True,
                    "value": None,
                    "validations": [],
                }
            }
        ),
        (
            {
                "int_field": {
                    "type": "int",
                    "required": True,
                    "value": None,
                    "validations": [],
                }
            }
        ),
        (
            {
                "list_field": {
                    "type": "list",
                    "required": True,
                    "value": [],
                    "validations": [],
                }
            }
        ),
        (
            {
                "list_field": {
                    "type": "list",
                    "required": True,
                    "value": None,
                    "validations": [],
                }
            }
        ),
        (
            {
                "bool_field": {
                    "type": "bool",
                    "required": True,
                    "value": None,
                    "validations": [],
                }
            }
        ),
    ],
)
async def test_check_valid_when_validations_fail_raises_error(config):
    c = DataSourceConfiguration(config)
    with pytest.raises(ConfigurableFieldValueError):
        c.check_valid()


@pytest.mark.asyncio
async def test_check_valid_when_dependencies_are_invalid_raises_error():
    config = {
        "port": {
            "type": "int",
            "value": 9,
            "validations": [
                {"type": ValidationTypes.LESS_THAN.value, "constraint": 10}
            ],
            "depends_on": [{"field": "missing_field", "value": "foo"}],
        }
    }

    c = DataSourceConfiguration(config)
    with pytest.raises(ConfigurableFieldDependencyError):
        c.check_valid()


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
                "display": "numeric",
                "value": 3306,
                "label": "Port",
                "type": "int",
            },
            "direct": {
                "display": "toggle",
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
    # we want to make sure all default values are included in the configuration
    # any specified values should override the defaults
    expected = {
        "host": {
            "default_value": None,
            "depends_on": [],
            "display": "text",
            "label": "Host",
            "options": [],
            "order": 1,
            "required": True,
            "sensitive": False,
            "tooltip": None,
            "type": "str",
            "ui_restrictions": [],
            "validations": [],
            "value": "127.0.0.1",
        },
        "port": {
            "default_value": None,
            "depends_on": [],
            "display": "numeric",
            "label": "Port",
            "options": [],
            "order": 1,
            "required": True,
            "sensitive": False,
            "tooltip": None,
            "type": "int",
            "ui_restrictions": [],
            "validations": [],
            "value": 3306,
        },
        "direct": {
            "default_value": None,
            "depends_on": [],
            "display": "toggle",
            "label": "Direct connect",
            "options": [],
            "order": 1,
            "required": True,
            "sensitive": False,
            "tooltip": None,
            "type": "bool",
            "ui_restrictions": [],
            "validations": [],
            "value": True,
        },
        "user": {
            "default_value": None,
            "depends_on": [],
            "display": "text",
            "label": "Username",
            "options": [],
            "order": 1,
            "required": True,
            "sensitive": False,
            "tooltip": None,
            "type": "str",
            "ui_restrictions": [],
            "validations": [],
            "value": "root",
        },
    }
    assert ds.get_simple_configuration() == expected

    with pytest.raises(NotImplementedError):
        await ds.ping()

    await ds.close()

    with pytest.raises(NotImplementedError):
        await ds.get_docs()

    with pytest.raises(NotImplementedError):
        await ds.get_docs_incrementally({})

    with pytest.raises(NotImplementedError):
        await ds.get_access_control()

    with pytest.raises(NotImplementedError):
        await ds.access_control_query([])

    # default rule validators for every data source (order matters)
    assert BaseDataSource.basic_rules_validators() == [
        BasicRuleAgainstSchemaValidator,
        BasicRuleNoMatchAllRegexValidator,
        BasicRulesSetSemanticValidator,
    ]

    # should be empty as advanced rules are specific to a data source
    assert not len(DataSource(configuration=configuration).advanced_rules_validators())

    features = Features()
    ds.set_features(features)
    assert ds._features == features


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

        for serialized_doc_key, expected_doc_key in zip(
            serialized_doc, expected_doc, strict=True
        ):
            assert serialized_doc[serialized_doc_key] == expected_doc[expected_doc_key]


@pytest.mark.asyncio
async def test_validate_config_fields_when_valid_no_errors_raised():
    configuration = {
        "host": {
            "value": "127.0.0.1",
            "type": "str",
        },
        "port": {
            "value": 3306,
            "type": "int",
        },
        "direct": {
            "value": True,
            "type": "bool",
        },
        "user": {
            "value": "root",
            "type": "str",
        },
    }
    ds = DataSource(configuration=DataSourceConfiguration(configuration))
    ds.validate_config_fields()


@pytest.mark.asyncio
async def test_validate_config_fields_when_invalid_raises_error():
    configuration = {
        "host": {
            "value": "127.0.0.1",
            "type": "str",
        },
        "port": {
            "value": 3306,
            "type": "int",
        },
        "direct": {
            "value": True,
            "type": "bool",
        },
    }
    ds = DataSource(configuration=DataSourceConfiguration(configuration))
    with pytest.raises(MalformedConfigurationError):
        ds.validate_config_fields()
