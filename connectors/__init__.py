#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os

import yaml

with open(os.path.join(os.path.dirname(__file__), "VERSION")) as f:
    __version__ = f.read().strip()

# This references a file that's built during from .buildkite/publish/publish-common.sh
# See https://github.com/elastic/connectors/pull/3154 for more info
yaml_path = os.path.join(os.path.dirname(__file__), "build.yaml")
if os.path.exists(yaml_path):
    __build_info__ = ""
    with open(yaml_path) as f:
        data = yaml.safe_load(f)
        for key in data:
            __build_info__ += f"{key}: {data[key]}\n"
else:
    __build_info__ = __version__
