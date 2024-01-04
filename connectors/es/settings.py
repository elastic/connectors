#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import copy
from os import path

import yaml

TIMESTAMP_FIELD = "_timestamp"

ENUM_IGNORE_ABOVE = 2048

DATE_FIELD_MAPPING = {"type": "date"}

KEYWORD_FIELD_MAPPING = {"type": "keyword"}

TEXT_FIELD_MAPPING = {
    "type": "text",
    "analyzer": "iq_text_base",
    "index_options": "freqs",
    "fields": {
        "stem": {"type": "text", "analyzer": "iq_text_stem"},
        "prefix": {
            "type": "text",
            "analyzer": "i_prefix",
            "search_analyzer": "q_prefix",
            "index_options": "docs",
        },
        "delimiter": {
            "type": "text",
            "analyzer": "iq_text_delimiter",
            "index_options": "freqs",
        },
        "joined": {
            "type": "text",
            "analyzer": "i_text_bigram",
            "search_analyzer": "q_text_bigram",
            "index_options": "freqs",
        },
        "enum": {"type": "keyword", "ignore_above": ENUM_IGNORE_ABOVE},
    },
}

WORKPLACE_SEARCH_SUBEXTRACTION_STAMP_FIELD_MAPPINGS = {
    "_subextracted_as_of": DATE_FIELD_MAPPING,
    "_subextracted_version": KEYWORD_FIELD_MAPPING,
}

CRAWLER_FIELD_MAPPINGS = {
    "additional_urls": KEYWORD_FIELD_MAPPING,
    "body_content": TEXT_FIELD_MAPPING,
    "domains": KEYWORD_FIELD_MAPPING,
    "headings": TEXT_FIELD_MAPPING,
    "last_crawled_at": DATE_FIELD_MAPPING,
    "links": KEYWORD_FIELD_MAPPING,
    "meta_description": TEXT_FIELD_MAPPING,
    "meta_keywords": KEYWORD_FIELD_MAPPING,
    "title": TEXT_FIELD_MAPPING,
    "url": KEYWORD_FIELD_MAPPING,
    "url_host": KEYWORD_FIELD_MAPPING,
    "url_path": KEYWORD_FIELD_MAPPING,
    "url_path_dir1": KEYWORD_FIELD_MAPPING,
    "url_path_dir2": KEYWORD_FIELD_MAPPING,
    "url_path_dir3": KEYWORD_FIELD_MAPPING,
    "url_port": KEYWORD_FIELD_MAPPING,
    "url_scheme": KEYWORD_FIELD_MAPPING,
}

DEFAULT_LANGUAGE = "en"
FRONT_NGRAM_MAX_GRAM = 12
LANGUAGE_DATA_FILE_PATH = path.join(path.dirname(__file__), "language_data.yml")

GENERIC_FILTERS = {
    "front_ngram": {
        "type": "edge_ngram",
        "min_gram": 1,
        "max_gram": FRONT_NGRAM_MAX_GRAM,
    },
    "delimiter": {
        "type": "word_delimiter_graph",
        "generate_word_parts": "true",
        "generate_number_parts": "true",
        "catenate_words": "true",
        "catenate_numbers": "true",
        "catenate_all": "true",
        "preserve_original": "false",
        "split_on_case_change": "true",
        "split_on_numerics": "true",
        "stem_english_possessive": "true",
    },
    "bigram_joiner": {
        "type": "shingle",
        "token_separator": "",
        "max_shingle_size": 2,
        "output_unigrams": "false",
    },
    "bigram_joiner_unigrams": {
        "type": "shingle",
        "token_separator": "",
        "max_shingle_size": 2,
        "output_unigrams": "true",
    },
    "bigram_max_size": {"type": "length", "min": 0, "max": 16},
}

NON_ICU_ANALYSIS_SETTINGS = {
    "tokenizer_name": "standard",
    "folding_filters": ["cjk_width", "lowercase", "asciifolding"],
}

ICU_ANALYSIS_SETTINGS = {
    "tokenizer_name": "icu_tokenizer",
    "folding_filters": ["icu_folding"],
}


class Mappings:
    """
    Create default mappings to enable relevance tuning
    """

    @classmethod
    def default_text_fields_mappings(
        cls, *, is_connectors_index=False, is_crawler_index=False
    ):
        result = {
            "dynamic": "true",
            "dynamic_templates": [
                {
                    "data": {
                        "match_mapping_type": "string",
                        "mapping": TEXT_FIELD_MAPPING,
                    }
                }
            ],
            "properties": {},
        }
        id_prop = {"id": KEYWORD_FIELD_MAPPING}
        if is_crawler_index:
            result["properties"].update(id_prop)
            result["properties"].update(CRAWLER_FIELD_MAPPINGS)
        if is_connectors_index:
            result["properties"].update(id_prop)
            result["properties"].update(
                WORKPLACE_SEARCH_SUBEXTRACTION_STAMP_FIELD_MAPPINGS
            )
        return result


class UnsupportedLanguageCode(Exception):
    pass


class Settings:
    """Default settings for the Elasticsearch index"""

    @property
    def language_data(self):
        if not self._language_data:
            with open(LANGUAGE_DATA_FILE_PATH, "r") as f:
                self._language_data = yaml.safe_load(f)
        return self._language_data

    def icu_settings(self, analysis_settings):
        return ICU_ANALYSIS_SETTINGS if analysis_settings else NON_ICU_ANALYSIS_SETTINGS

    @property
    def stemmer_name(self):
        return self.language_data[self.language_code].get("stemmer", None)

    @property
    def stop_words_name_or_list(self):
        return self.language_data[self.language_code].get("stop_words", None)

    @property
    def custom_filter_definitions(self):
        return self.language_data[self.language_code].get(
            "custom_filter_definitions", {}
        )

    @property
    def prepended_filters(self):
        return self.language_data[self.language_code].get("prepended_filters", [])

    @property
    def postpended_filters(self):
        return self.language_data[self.language_code].get("postpended_filters", [])

    @property
    def stem_filter_name(self):
        return f"{self.language_code}-stem-filter"

    @property
    def stop_words_filter_name(self):
        return f"{self.language_code}-stop-words-filter"

    @property
    def filter_definitions(self):
        definitions = copy.deepcopy(GENERIC_FILTERS)

        definitions[self.stem_filter_name] = {
            "type": "stemmer",
            "name": self.stemmer_name,
        }

        definitions[self.stop_words_filter_name] = {
            "type": "stop",
            "stopwords": self.stop_words_name_or_list,
        }

        definitions.update(self.custom_filter_definitions)
        return definitions

    @property
    def analyzer_definitions(self):
        definitions = {}

        definitions["i_prefix"] = {
            "tokenizer": self.analysis_settings["tokenizer_name"],
            "filter": [*self.analysis_settings["folding_filters"], "front_ngram"],
        }

        definitions["q_prefix"] = {
            "tokenizer": self.analysis_settings["tokenizer_name"],
            "filter": [*self.analysis_settings["folding_filters"]],
        }

        definitions["iq_text_base"] = {
            "tokenizer": self.analysis_settings["tokenizer_name"],
            "filter": [
                *self.analysis_settings["folding_filters"],
                self.stop_words_filter_name,
            ],
        }

        definitions["iq_text_stem"] = {
            "tokenizer": self.analysis_settings["tokenizer_name"],
            "filter": self.prepended_filters
            + self.analysis_settings["folding_filters"]
            + [
                self.stop_words_filter_name,
                self.stem_filter_name,
            ]
            + self.postpended_filters,
        }

        definitions["iq_text_delimiter"] = {
            "tokenizer": "whitespace",
            "filter": self.prepended_filters
            + ["delimiter"]
            + self.analysis_settings["folding_filters"]
            + [self.stop_words_filter_name, self.stem_filter_name]
            + self.postpended_filters,
        }

        definitions["i_text_bigram"] = {
            "tokenizer": self.analysis_settings["tokenizer_name"],
            "filter": self.analysis_settings["folding_filters"]
            + [
                self.stem_filter_name,
                "bigram_joiner",
                "bigram_max_size",
            ],
        }

        definitions["q_text_bigram"] = {
            "tokenizer": self.analysis_settings["tokenizer_name"],
            "filter": self.analysis_settings["folding_filters"]
            + [
                self.stem_filter_name,
                "bigram_joiner_unigrams",
                "bigram_max_size",
            ],
        }

        return definitions

    @property
    def auto_expand_replicas(self):
        return "0-1"

    @property
    def number_of_shards(self):
        return 2

    def __init__(self, *, language_code=None, analysis_icu=False):
        self._language_data = None
        self.language_code = language_code or DEFAULT_LANGUAGE

        if self.language_code not in self.language_data:
            msg = f"Language '{language_code}' is not supported"
            raise UnsupportedLanguageCode(msg)

        self.analysis_icu = analysis_icu
        self.analysis_settings = self.icu_settings(analysis_icu)

    def to_hash(self):
        return {
            "analysis": {
                "analyzer": self.analyzer_definitions,
                "filter": self.filter_definitions,
            },
            "index": {"similarity": {"default": {"type": "BM25"}}},
        }
