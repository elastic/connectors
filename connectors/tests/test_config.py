#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os
from contextlib import contextmanager

import pytest
from envyaml import EnvYAML

import connectors.config
from connectors.config import load_config

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.yml")


@contextmanager
def unset_config():
    connectors.config.config = None
    try:
        yield
    finally:
        pass


def test_bad_config_file():
    with unset_config():
        with pytest.raises(FileNotFoundError):
            load_config("BEEUUUAH")


def test_config():
    with unset_config():
        load_config(CONFIG_FILE)
        assert isinstance(connectors.config.config, EnvYAML)
