#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import MagicMock

import pytest

from connectors.agent.config import ConnectorsAgentConfigurationWrapper


@pytest.mark.asyncio
async def test_try_update_without_auth_data():
    config_wrapper = ConnectorsAgentConfigurationWrapper()
    source_mock = MagicMock()
    source_mock.fields = {}

    assert config_wrapper.try_update(source_mock) is False


@pytest.mark.asyncio
async def test_try_update_with_api_key_auth_data():
    hosts = ["https://localhost:9200"]
    api_key = "lemme_in"

    config_wrapper = ConnectorsAgentConfigurationWrapper()
    source_mock = MagicMock()
    fields_container = {"hosts": hosts, "api_key": api_key}

    source_mock.fields = fields_container
    source_mock.__getitem__.side_effect = fields_container.__getitem__

    assert config_wrapper.try_update(source_mock) is True
    assert config_wrapper.get()["elasticsearch"]["host"] == hosts[0]
    assert config_wrapper.get()["elasticsearch"]["api_key"] == api_key


@pytest.mark.asyncio
async def test_try_update_with_basic_auth_auth_data():
    hosts = ["https://localhost:9200"]
    username = "elastic"
    password = "hold the door"

    config_wrapper = ConnectorsAgentConfigurationWrapper()
    source_mock = MagicMock()
    fields_container = {"hosts": hosts, "username": username, "password": password}

    source_mock.fields = fields_container
    source_mock.__getitem__.side_effect = fields_container.__getitem__

    assert config_wrapper.try_update(source_mock) is True
    assert config_wrapper.get()["elasticsearch"]["host"] == hosts[0]
    assert config_wrapper.get()["elasticsearch"]["username"] == username
    assert config_wrapper.get()["elasticsearch"]["password"] == password
