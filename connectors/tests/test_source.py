#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import pytest
from connectors.source import Field


def test_field():
    # stupid holder
    f = Field("name")
    assert f.label == "name"
