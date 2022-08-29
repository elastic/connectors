#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import pytest
from templates.elasticsearch.index.settings import (
    IndexSettings,
    UnsupportedLanguageCode,
)

EXPECTED_ANALYZER_KEYS = {
    "i_prefix",
    "q_prefix",
    "iq_text_base",
    "iq_text_stem",
    "iq_text_delimiter",
    "i_text_bigram",
    "q_text_bigram",
}


def test_to_hash_analysis_icu_false():
    """When analysis_icu is false"""
    actual = IndexSettings(
        language_code=IndexSettings.DEFAULT_LANGUAGE, analysis_icu=False
    ).to_hash()
    assert isinstance(actual, dict)
    assert "analysis" in actual
    assert "analyzer" in actual["analysis"]

    analyzer = actual["analysis"]["analyzer"]
    assert EXPECTED_ANALYZER_KEYS.issubset(analyzer.keys())

    non_icu_filters = IndexSettings.NON_ICU_ANALYSIS_SETTINGS["folding_filters"]
    icu_filters = IndexSettings.ICU_ANALYSIS_SETTINGS["folding_filters"]

    filters = {f for item in analyzer.values() for f in item["filter"]}
    # should contain non-icu filters
    assert set(non_icu_filters).issubset(filters)
    # should not contain icu filters
    assert not any(f in filters for f in icu_filters)


def test_to_hash_analysis_icu_true():
    """When analysis_icu is true"""
    actual = IndexSettings(
        language_code=IndexSettings.DEFAULT_LANGUAGE, analysis_icu=True
    ).to_hash()
    assert isinstance(actual, dict)
    assert "analysis" in actual
    assert "analyzer" in actual["analysis"]

    analyzer = actual["analysis"]["analyzer"]
    assert EXPECTED_ANALYZER_KEYS.issubset(analyzer.keys())

    non_icu_filters = IndexSettings.NON_ICU_ANALYSIS_SETTINGS["folding_filters"]
    icu_filters = IndexSettings.ICU_ANALYSIS_SETTINGS["folding_filters"]

    filters = {f for item in analyzer.values() for f in item["filter"]}
    # should contain icu filters
    assert set(icu_filters).issubset(filters)
    # should not contain non-icu filters
    assert not any(f in filters for f in non_icu_filters)


def test_to_hash_unsupported_language():
    """When the language_code is not supported"""
    with pytest.raises(UnsupportedLanguageCode):
        _ = IndexSettings(
            language_code="unsupported_language_code", analysis_icu=False
        ).to_hash()


def test_to_hash_supported_language():
    """When the language_code is supported"""
    language_code = "fr"
    actual = IndexSettings(language_code=language_code, analysis_icu=False).to_hash()
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


def test_to_hash_none_language():
    """When the language_code is None"""
    actual = IndexSettings(language_code=None, analysis_icu=False).to_hash()
    english = IndexSettings(language_code="en", analysis_icu=False).to_hash()
    assert actual == english
