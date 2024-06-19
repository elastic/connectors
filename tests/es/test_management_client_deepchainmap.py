#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import pytest

from connectors.es.management_client import _DeepChainMap


def getter(*keys):
    def inner(mapping):
        ret = mapping
        for key in keys:
            ret = ret[key]
        return ret
    return inner


MAPPINGS = (
    {
        "a": {
            "b": {
                "c": "a-b-c",
            }
        },
        "X": {},
        "Y": {
            1: 1,
        },
    },
    {
        "A": {
            "B": {
                "C": "A-B-C",
            }
        },
        "X": "non-mapping",
        "Y": {
            2: 2,
        },
        "Z": {
            1: 1,
        },
    },
    {
        "A": {
            "B'": {
                "C'": "A-B'-C'",
            }
        },
        "Y": {
            2: None,
        },
        "Z": {
            2: 2,
        },
    },
)


class TestDeepChainMap:
    @pytest.mark.parametrize(
        "getter, expect",
        [
            (getter("a", "b", "c"), "a-b-c"),
            (getter("A", "B", "C"), "A-B-C"),
            (getter("A", "B'", "C'"), "A-B'-C'"),
            (getter("X"), {}),
            (getter("Y", 1), 1),
            (getter("Y", 2), 2),
            (getter("Z", 2), 2),
        ],
    )
    def test_get_nested_value(self, getter, expect):
        assert getter(_DeepChainMap(*MAPPINGS)) == expect

    def test_to_dict(self):
        assert _DeepChainMap(*MAPPINGS).to_dict() == {
            "a": {
                "b": {
                    "c": "a-b-c",
                }
            },
            "A": {
                "B": {
                    "C": "A-B-C",
                },
                "B'": {
                    "C'": "A-B'-C'",
                }
            },
            "X": {},
            "Y": {
                1: 1,
                2: 2,
            },
            "Z": {
                1: 1,
                2: 2,
            },
        }
