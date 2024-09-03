#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#


def extract_index_or_alias(response, name):
    index = response.get(name, None)

    if index:
        return index

    for idx_name in response.keys():
        index_definition = response.get(idx_name)

        if not index_definition.get("aliases", None):
            continue

        for alias in index_definition.get("aliases"):
            if name == alias:
                return index_definition

    return None
