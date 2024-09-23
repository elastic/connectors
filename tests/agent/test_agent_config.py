#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import MagicMock, Mock

from connectors.agent.config import ConnectorsAgentConfigurationWrapper


def prepare_unit_mock(fields, log_level):
    if not fields:
        fields = {}
    unit_mock = Mock()
    unit_mock.config = Mock()
    unit_mock.config.source = MagicMock()
    unit_mock.config.source.fields = fields
    unit_mock.config.source.__getitem__.side_effect = fields.__getitem__

    unit_mock.log_level = log_level

    return unit_mock


def test_try_update_without_auth_data():
    config_wrapper = ConnectorsAgentConfigurationWrapper()

    unit_mock = prepare_unit_mock({}, None)

    assert config_wrapper.try_update(unit_mock) is False


def test_try_update_with_api_key_auth_data():
    hosts = ["https://localhost:9200"]
    api_key = "lemme_in"

    config_wrapper = ConnectorsAgentConfigurationWrapper()
    unit_mock = prepare_unit_mock({"hosts": hosts, "api_key": api_key}, None)

    assert config_wrapper.try_update(unit_mock) is True
    assert config_wrapper.get()["elasticsearch"]["host"] == hosts[0]
    assert config_wrapper.get()["elasticsearch"]["api_key"] == api_key


def test_try_update_with_non_encoded_api_key_auth_data():
    hosts = ["https://localhost:9200"]
    api_key = "something:else"
    encoded = "c29tZXRoaW5nOmVsc2U="

    config_wrapper = ConnectorsAgentConfigurationWrapper()
    source_mock = prepare_unit_mock({"hosts": hosts, "api_key": api_key}, None)

    assert config_wrapper.try_update(source_mock) is True
    assert config_wrapper.get()["elasticsearch"]["host"] == hosts[0]
    assert config_wrapper.get()["elasticsearch"]["api_key"] == encoded


def test_try_update_with_basic_auth_auth_data():
    hosts = ["https://localhost:9200"]
    username = "elastic"
    password = "hold the door"

    config_wrapper = ConnectorsAgentConfigurationWrapper()
    unit_mock = prepare_unit_mock(
        {"hosts": hosts, "username": username, "password": password}, None
    )

    assert config_wrapper.try_update(unit_mock) is True
    assert config_wrapper.get()["elasticsearch"]["host"] == hosts[0]
    assert config_wrapper.get()["elasticsearch"]["username"] == username
    assert config_wrapper.get()["elasticsearch"]["password"] == password


def test_try_update_multiple_times_does_not_reset_config_values():
    hosts = ["https://localhost:9200"]
    api_key = "lemme_in"

    log_level = "DEBUG"
    config_wrapper = ConnectorsAgentConfigurationWrapper()

    # First unit comes with elasticsearch data
    first_unit_mock = prepare_unit_mock({"hosts": hosts, "api_key": api_key}, None)

    # Second unit comes only with a log_level
    second_unit_mock = prepare_unit_mock({}, log_level)

    assert config_wrapper.try_update(first_unit_mock) is True
    assert config_wrapper.try_update(second_unit_mock) is True

    assert config_wrapper.get()["elasticsearch"]["host"] == hosts[0]
    assert config_wrapper.get()["elasticsearch"]["api_key"] == api_key
    assert config_wrapper.get()["service"]["log_level"] == log_level


def test_config_changed_when_new_variables_are_passed():
    hosts = ["https://localhost:9200"]
    api_key = "lemme_in_lalala"

    new_config = {"elasticsearch": {"hosts": hosts, "api_key": api_key}}

    config_wrapper = ConnectorsAgentConfigurationWrapper()

    assert config_wrapper.config_changed(new_config) is True


def test_config_changed_when_elasticsearch_config_changed():
    hosts = ["https://localhost:9200"]
    api_key = "lemme_in_lalala"

    starting_config = {
        "elasticsearch": {
            "hosts": hosts,
            "username": "elastic",
            "password": "hey-im-a-password",
        }
    }
    new_config = {"elasticsearch": {"hosts": hosts, "api_key": api_key}}

    config_wrapper = ConnectorsAgentConfigurationWrapper()
    config_wrapper.try_update(prepare_unit_mock(starting_config, None))

    assert config_wrapper.config_changed(new_config) is True


def test_config_changed_when_elasticsearch_config_did_not_change():
    hosts = ["https://localhost:9200"]
    api_key = "lemme_in_lalala"

    new_config = {"elasticsearch": {"hosts": hosts, "api_key": api_key}}

    config_wrapper = ConnectorsAgentConfigurationWrapper()
    config_wrapper.try_update(prepare_unit_mock(new_config, None))

    assert config_wrapper.config_changed(new_config) is True


def test_config_changed_when_log_level_config_changed():
    config_wrapper = ConnectorsAgentConfigurationWrapper()
    config_wrapper.try_update(prepare_unit_mock({}, "INFO"))

    new_config = {"service": {"log_level": "DEBUG"}}

    assert config_wrapper.config_changed(new_config) is True


def test_config_changed_when_log_level_config_did_not_change():
    config_wrapper = ConnectorsAgentConfigurationWrapper()
    config_wrapper.try_update(prepare_unit_mock({}, "INFO"))

    new_config = {"service": {"log_level": "INFO"}}

    assert config_wrapper.config_changed(new_config) is False
