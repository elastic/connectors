#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import pytest

from connectors.es import ESDocument, InvalidDocumentSourceError


@pytest.mark.parametrize(
    "doc_source",
    [
        None,
        "hahaha",
        {},
        {"_id": {}},
        {"_id": "1", "_source": "hahaha"},
    ],
)
def test_es_document_raise(doc_source, patch_logger):
    with pytest.raises(InvalidDocumentSourceError):
        ESDocument(elastic_index=None, doc_source=doc_source)


def test_es_document_ok(patch_logger):
    doc_source = {"_id": "1", "_source": {}}
    es_document = ESDocument(elastic_index=None, doc_source=doc_source)
    assert isinstance(es_document, ESDocument)


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
