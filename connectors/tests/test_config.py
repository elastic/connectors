#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os
from unittest import mock

import pytest
from envyaml import EnvYAML

from connectors.config import load_config

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.yml")
ES_CONFIG_FILE = os.path.join(os.path.dirname(__file__), "entsearch.yml")


def test_bad_config_file():
    with pytest.raises(FileNotFoundError):
        load_config("BEEUUUAH")


def test_config():
    config = load_config(CONFIG_FILE)
    assert isinstance(config, EnvYAML)


def test_config_with_ent_search():
    with mock.patch.dict(os.environ, {"ENT_SEARCH_CONFIG_PATH": ES_CONFIG_FILE}):
        config = load_config(CONFIG_FILE)
        assert config["elasticsearch"]["headers"]["X-Elastic-Auth"] == "SomeYeahValue"
