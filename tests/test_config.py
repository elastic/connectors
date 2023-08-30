#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os
from unittest import mock

import pytest

from connectors.config import _handle_config_conflicts, _nest_configs, load_config

HERE = os.path.dirname(__file__)
FIXTURES_DIR = os.path.abspath(os.path.join(HERE, "fixtures"))

CONFIG_FILE = os.path.join(FIXTURES_DIR, "config.yml")
ES_CONFIG_FILE = os.path.join(FIXTURES_DIR, "entsearch.yml")
ES_CONFIG_INVALID_LOG_LEVEL_FILE = os.path.join(
    FIXTURES_DIR, "entsearch_invalid_log_level.yml"
)


def test_bad_config_file():
    with pytest.raises(FileNotFoundError):
        load_config("BEEUUUAH")


def test_config(set_env):
    config = load_config(CONFIG_FILE)
    assert isinstance(config, dict)
    assert config["elasticsearch"]["host"] == "http://nowhere.com:9200"
    assert config["elasticsearch"]["user"] == "elastic"


def test_config_with_ent_search(set_env):
    with mock.patch.dict(os.environ, {"ENT_SEARCH_CONFIG_PATH": ES_CONFIG_FILE}):
        config = load_config(CONFIG_FILE)
        assert config["elasticsearch"]["headers"]["X-Elastic-Auth"] == "SomeYeahValue"
        assert config["service"]["log_level"] == "DEBUG"


def test_config_with_invalid_log_level(set_env):
    with mock.patch.dict(
        os.environ, {"ENT_SEARCH_CONFIG_PATH": ES_CONFIG_INVALID_LOG_LEVEL_FILE}
    ):
        with pytest.raises(ValueError) as e:
            _ = load_config(CONFIG_FILE)

        assert e.match("Unexpected log level.*")


def test_nest_config_when_nested_field_does_not_exist():
    config = {}

    _nest_configs(config, "test.nested.property", 50)

    assert config["test"]["nested"]["property"] == 50


def test_nest_config_when_nested_field_exists():
    config = {"test": {"nested": {"property": 25}}}

    _nest_configs(config, "test.nested.property", 50)

    assert config["test"]["nested"]["property"] == 50


def test_nest_config_when_root_field_does_not_exist():
    config = {}

    _nest_configs(config, "test", 50)

    assert config["test"] == 50


def test_nest_config_when_root_field_does_exists():
    config = {"test": 10}

    _nest_configs(config, "test", 50)

    assert config["test"] == 50


@pytest.mark.parametrize(
    "es_password, es_api_key, connector_api_key, result_api_key",
    [
        ("changeme", None, "123", "123"),
        ("changeme", "456", "123", "456"),
        ("something", None, "123", None),
    ],
)
def test_resolve_single_connector_auth(
    es_password, es_api_key, connector_api_key, result_api_key
):
    config = {
        "elasticsearch": {"username": "elastic", "password": es_password},
        "connectors": [
            {"api_key": connector_api_key, "service_type": "foo", "connector_id": "abc"}
        ],
    }
    if es_api_key:
        config["elasticsearch"]["api_key"] = es_api_key

    output = _handle_config_conflicts(config)
    assert output["elasticsearch"].get("api_key") == result_api_key


def test_do_not_resolve_multiple_connector_auth():
    config = {
        "elasticsearch": {"username": "elastic", "password": "changeme"},
        "connectors": [
            {"api_key": "123", "service_type": "foo", "connector_id": "abc"},
            {"api_key": "456", "service_type": "bar", "connector_id": "def"},
        ],
    }
    output = _handle_config_conflicts(config)
    assert output["elasticsearch"].get("api_key") is None
