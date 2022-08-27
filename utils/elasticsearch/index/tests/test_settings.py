#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from utils.elasticsearch.index.settings import IndexSettings

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
    actual = IndexSettings(
        language_code=IndexSettings.DEFAULT_LANGUAGE, analysis_icu=False
    ).to_hash()
    assert isinstance(actual, dict)
    assert 'analysis' in actual
    assert 'analyzer' in actual['analysis']
    analyzer = actual['analysis']['analyzer']
    assert EXPECTED_ANALYZER_KEYS.issubset(analyzer.keys())
