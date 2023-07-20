#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import pytest

from connectors.access_control import (
    ACCESS_CONTROL,
    _decorate_with_access_control,
    access_control_query,
)

USER_ONE_EMAIL = "user1@mail.com"
USER_TWO_EMAIL = "user2@mail.com"


@pytest.mark.parametrize(
    "_dls_enabled, document, access_control, expected_decorated_document",
    [
        (
            False,
            {},
            [USER_ONE_EMAIL],
            {},
        ),
        (
            True,
            {},
            [USER_ONE_EMAIL],
            {
                ACCESS_CONTROL: [
                    USER_ONE_EMAIL,
                ]
            },
        ),
        (
            True,
            {ACCESS_CONTROL: [USER_ONE_EMAIL]},
            [USER_TWO_EMAIL],
            {
                ACCESS_CONTROL: [
                    USER_ONE_EMAIL,
                    USER_TWO_EMAIL,
                ]
            },
        ),
        (
            True,
            {ACCESS_CONTROL: [USER_ONE_EMAIL]},
            [],
            {
                ACCESS_CONTROL: [
                    USER_ONE_EMAIL,
                ]
            },
        ),
    ],
)
def test_decorate_with_access_control(
    _dls_enabled, document, access_control, expected_decorated_document
):
    decorated_document = _decorate_with_access_control(
        _dls_enabled, document, access_control
    )

    assert (
        decorated_document.get(ACCESS_CONTROL, []).sort()
        == expected_decorated_document.get(ACCESS_CONTROL, []).sort()
    )


def test_access_control_query():
    access_control = ["user_1"]
    query = access_control_query(access_control)

    assert query == {
        "query": {
            "template": {"params": {"access_control": access_control}},
            "source": {
                "bool": {
                    "filter": {
                        "bool": {
                            "should": [
                                {
                                    "bool": {
                                        "must_not": {
                                            "exists": {"field": ACCESS_CONTROL}
                                        }
                                    }
                                },
                                {"terms": {f"{ACCESS_CONTROL}.enum": access_control}},
                            ]
                        }
                    }
                }
            },
        }
    }
