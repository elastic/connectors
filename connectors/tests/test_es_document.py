#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import pytest

from connectors.es import ESDocument, InvalidDocumentSourceError


@pytest.mark.parametrize(
    "doc_source, exception",
    [
        (None, InvalidDocumentSourceError),
        ("hahaha", InvalidDocumentSourceError),
        ({}, InvalidDocumentSourceError),
        ({"_id": {}}, InvalidDocumentSourceError),
        ({"_id": "1", "_source": "hahaha"}, InvalidDocumentSourceError),
        ({"_id": "1", "_source": {}}, None),
    ],
)
def test_es_document(doc_source, exception, patch_logger):
    try:
        ESDocument(elastic_index=None, doc_source=doc_source)
    except Exception as e:
        assert e.__class__ == exception
    else:
        assert exception is None


def test_es_document_get():
    source = {
        "_id": "test",
        "_source": {
            "string": "string_value",
            "none_value": None,
            "empty_dict": {},
            "nested_dict": {"string": "string_value"},
        },
    }
    default_value = "default"
    es_doc = ESDocument(elastic_index=None, doc_source=source)
    assert es_doc.id == "test"
    assert es_doc.get("string", default=default_value) == "string_value"
    assert es_doc.get("non_existing") is None
    assert es_doc.get("non_existing", default=default_value) == default_value
    assert es_doc.get("empty_dict", default=default_value) == {}
    assert es_doc.get("empty_dict", "string") is None
    assert es_doc.get("empty_dict", "string", default=default_value) == default_value
    assert es_doc.get("nested_dict", "non_existing") is None
    assert (
        es_doc.get("nested_dict", "non_existing", default=default_value)
        == default_value
    )
