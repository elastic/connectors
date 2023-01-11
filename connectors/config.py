#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import copy

from envyaml import EnvYAML


class ConfigNotLoadedError(Exception):
    pass


class Config:
    _yaml = None

    @classmethod
    def load(cls, config_file):
        cls._yaml = EnvYAML(config_file)

    @classmethod
    def get(cls, key=None, default=None):
        if cls._yaml is None:
            raise ConfigNotLoadedError(
                "Config is not loaded yet, make sure to call Config.load(config_file) first."
            )
        yaml = copy.deepcopy(cls._yaml)
        if key is None:
            return yaml
        return yaml.get(key, default)
