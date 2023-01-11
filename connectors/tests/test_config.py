#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os
import pytest

from connectors.config import (Config, ConfigNotLoadedError)
from contextlib import contextmanager
from envyaml import EnvYAML

CONFIG = os.path.join(os.path.dirname(__file__), "config.yml")


@contextmanager
def unset_config():
    Config._yaml = None
    try:
        yield
    finally:
        pass


def test_bad_config_file():
    with unset_config():
        with pytest.raises(FileNotFoundError):
            Config.load("BEEUUUAH")


def test_get_without_loading():
    with unset_config():
        with pytest.raises(ConfigNotLoadedError):
            Config.get()


def test_config():
    with unset_config():
        Config.load(CONFIG)
        assert isinstance(Config.get(), EnvYAML)
