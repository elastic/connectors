#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import io
import sys

import pytest

@pytest.fixture
def catch_stdout():
    old = sys.stdout
    new = sys.stdout = io.StringIO()
    try:
        yield new
    finally:
        sys.stdout = old
