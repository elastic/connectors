#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

ACCESS_CONTROL = "_allow_access_control"


def _decorate_with_access_control(dls_enabled, document, access_control):
    if dls_enabled:
        document[ACCESS_CONTROL] = list(
            set(document.get(ACCESS_CONTROL, []) + access_control)
        )

    return document


def access_control_query(access_control):
    # filter out 'None' values
    filtered_access_control = list(
        filter(
            lambda access_control_entity: access_control_entity is not None,
            access_control,
        )
    )

    return {
        "query": {
            "template": {"params": {"access_control": filtered_access_control}},
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
                                {
                                    "terms": {
                                        f"{ACCESS_CONTROL}.enum": filtered_access_control
                                    }
                                },
                            ]
                        }
                    }
                }
            },
        }
    }
