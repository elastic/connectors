#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from utils.elasticsearch.index.mappings import IndexMappings

EXPECTED_CONNECTORS_PROPS = ["id", "_subextracted_as_of", "_subextracted_version"]

EXPECTED_CRAWLER_PROPS = [
        "id",
        "additional_urls",
        "body_content",
        "domains",
        "headings",
        "last_crawled_at",
        "links",
        "meta_description",
        "meta_keywords",
        "title",
        "url",
        "url_host",
        "url_path",
        "url_path_dir1",
        "url_path_dir2",
        "url_path_dir3",
        "url_port",
        "url_scheme",
    ]


def test_mappings_default():
    """When the index is not a connectors or crawler index"""
    actual = IndexMappings.default_text_fields_mappings()

    assert isinstance(actual, dict)
    for key in ("dynamic_templates", "dynamic", "properties"):
        assert key in actual

    assert isinstance(actual["dynamic_templates"], list)
    assert actual["dynamic"] == "true"
    assert isinstance(actual["properties"], dict)

    for prop in set(EXPECTED_CRAWLER_PROPS + EXPECTED_CONNECTORS_PROPS):
        assert prop not in actual["properties"]


def test_mappings_connectors_index():
    """When the index is a connectors index"""
    actual = IndexMappings.default_text_fields_mappings(is_connectors_index=True)

    assert isinstance(actual, dict)
    for key in ("dynamic_templates", "dynamic", "properties"):
        assert key in actual

    assert isinstance(actual["dynamic_templates"], list)
    assert actual["dynamic"] == "true"
    assert isinstance(actual["properties"], dict)
    for prop in EXPECTED_CONNECTORS_PROPS:
        assert prop in actual["properties"]
        assert isinstance(actual["properties"][prop], dict)


def test_mappings_crawler_index():
    """When the index is a crawler index"""
    actual = IndexMappings.default_text_fields_mappings(is_crawler_index=True)

    assert isinstance(actual, dict)
    for key in ("dynamic_templates", "dynamic", "properties"):
        assert key in actual

    assert isinstance(actual["dynamic_templates"], list)
    assert actual["dynamic"] == "true"
    assert isinstance(actual["properties"], dict)
    for prop in EXPECTED_CRAWLER_PROPS:
        assert prop in actual["properties"]
        assert isinstance(actual["properties"][prop], dict)
