#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import json

import pytest

from connectors.es.settings import (
    Mappings,
    Settings,
    UnsupportedLanguageCode,
    defaults_for,
)

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

EXPECTED_ANALYZER_KEYS = {
    "i_prefix",
    "q_prefix",
    "iq_text_base",
    "iq_text_stem",
    "iq_text_delimiter",
    "i_text_bigram",
    "q_text_bigram",
}


def test_mappings_default():
    """When the index is not a connectors or crawler index"""
    actual = Mappings.default_text_fields_mappings()

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
    actual = Mappings.default_text_fields_mappings(is_connectors_index=True)

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
    actual = Mappings.default_text_fields_mappings(is_crawler_index=True)

    assert isinstance(actual, dict)
    for key in ("dynamic_templates", "dynamic", "properties"):
        assert key in actual

    assert isinstance(actual["dynamic_templates"], list)
    assert actual["dynamic"] == "true"
    assert isinstance(actual["properties"], dict)
    for prop in EXPECTED_CRAWLER_PROPS:
        assert prop in actual["properties"]
        assert isinstance(actual["properties"][prop], dict)


def test_settings_unsupported_language():
    """When the language_code is not supported"""
    with pytest.raises(UnsupportedLanguageCode):
        _ = Settings(language_code="unsupported_language_code").to_hash()


def test_settings_supported_language():
    """When the language_code is supported"""
    language_code = "fr"
    actual = Settings(language_code=language_code).to_hash()
    assert isinstance(actual, dict)
    assert "analysis" in actual
    assert "filter" in actual["analysis"]
    filter = actual["analysis"]["filter"]

    for k in (
        f"{language_code}-stem-filter",
        f"{language_code}-stop-words-filter",
        f"{language_code}-elision",
    ):
        assert k in filter


def test_defaults_for_is_serializable():
    mappings, settings = defaults_for()
    settings = json.dumps(settings)
    assert "analysis" in settings


def test_settings_none_language():
    """When the language_code is None"""
    actual = Settings(language_code=None).to_hash()
    english = Settings(language_code="en").to_hash()
    assert actual == english
