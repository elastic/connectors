#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.es.utils import extract_index_or_alias


def test_extract_index_or_alias_with_index():
    response = {
        "shapo-online": {
            "aliases": {"search-shapo-online": {}},
            "mappings": {},
            "settings": {},
        }
    }

    index = extract_index_or_alias(response, "shapo-online")

    assert index == response["shapo-online"]


def test_extract_index_or_alias_with_alias():
    response = {
        "shapo-online": {
            "aliases": {"search-shapo-online": {}},
            "mappings": {},
            "settings": {},
        }
    }

    index = extract_index_or_alias(response, "search-shapo-online")

    assert index == response["shapo-online"]


def test_extract_index_or_alias_when_none_present():
    response = {
        "shapo-online": {
            "aliases": {"search-shapo-online": {}},
            "mappings": {},
            "settings": {},
        }
    }

    index = extract_index_or_alias(response, "nonexistent")

    assert index is None
