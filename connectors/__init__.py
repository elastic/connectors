#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os

with open(os.path.join(os.path.dirname(__file__), "VERSION")) as f:
    __version__: str = f.read().strip()
