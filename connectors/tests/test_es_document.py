#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import AsyncMock, Mock

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
def test_es_document_raise(doc_source):
    with pytest.raises(InvalidDocumentSourceError):
        ESDocument(elastic_index=None, doc_source=doc_source)


def test_es_document_ok():
    doc_source = {"_id": "1", "_source": {}}
    es_document = ESDocument(elastic_index=None, doc_source=doc_source)
    assert isinstance(es_document, ESDocument)


def test_es_document_get():
    source = {
        "_id": "test",
        "_seq_no": 1,
        "_primary_term": 2,
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
    assert es_doc._seq_no == 1
    assert es_doc._primary_term == 2
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


@pytest.mark.asyncio
async def test_reload():
    source = {
        "_id": "test",
        "_seq_no": 1,
        "_primary_term": 1,
        "_source": {
            "status": "pending",
        },
    }
    updated_source = {
        "_id": "test",
        "_seq_no": 2,
        "_primary_term": 2,
        "_source": {
            "status": "in_progress",
        },
    }

    index = Mock()
    index.fetch_response_by_id = AsyncMock(return_value=updated_source)
    doc = ESDocument(index, source)
    assert doc.id == "test"
    assert doc._seq_no == 1
    assert doc._primary_term == 1
    assert doc.get("status") == "pending"
    await doc.reload()
    assert doc.id == "test"
    assert doc._seq_no == 2
    assert doc._primary_term == 2
    assert doc.get("status") == "in_progress"
