#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

ACCESS_CONTROL = "_allow_access_control"


def prefix_identity(prefix, identity):
    if prefix is None or identity is None:
        return None

    return f"{prefix}:{identity}"


def es_access_control_query(access_control):
    # filter out 'None' values
    filtered_access_control = list(
        filter(
            lambda access_control_entity: access_control_entity is not None,
            access_control,
        )
    )

    return {
        "query": {
            "template": {
                "params": {"access_control": filtered_access_control},
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
                            },
                        ]
                    }
                }"""
            }
        }
    }
