#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from os import path

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
