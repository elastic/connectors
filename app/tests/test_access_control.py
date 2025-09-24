#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import pytest

from connectors.access_control import (
    es_access_control_query,
    prefix_identity,
)


@pytest.mark.asyncio
async def test_access_control_query():
    access_control = ["user_1"]
    access_control_query = es_access_control_query(access_control)

    assert access_control_query == {
        "query": {
            "template": {
                "params": {"access_control": access_control},
                "source": """{
                    "bool": {
                        "should": [
                            {
                                "bool": {
                                    "must_not": {
                                        "exists": {
                                            "field": "_allow_access_control"
                                        }
                                    }
                                }
                            },
                            {
                                "terms": {
                                    "_allow_access_control.enum": {{#toJson}}access_control{{/toJson}}
                                }
                            }
                        ]
                    }
                }""",
            }
        }
    }


def test_prefix_identity():
    prefix = "prefix"
    identity = "identity"

    assert prefix_identity(prefix, identity) == f"{prefix}:{identity}"


def test_prefix_identity_with_prefix_none():
    prefix = None
    identity = "identity"

    assert prefix_identity(prefix, identity) is None


def test_prefix_identity_with_identity_none():
    prefix = "prefix"
    identity = None

    assert prefix_identity(prefix, identity) is None
