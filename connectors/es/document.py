#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
class InvalidDocumentSourceError(Exception):
    pass


class ESDocument:
    """
    Represents a document in an Elasticsearch index.
    """

    def __init__(self, elastic_index, doc_source):
        self.index = elastic_index
        if not isinstance(doc_source, dict):
            raise InvalidDocumentSourceError(
                f"Invalid type found for doc_source: {type(doc_source).__name__}, expected: {dict.__name__}"
            )
        self.id = doc_source.get("_id")
        if not isinstance(self.id, str):
            raise InvalidDocumentSourceError(
                f"Invalid type found for id: {type(self.id).__name__}, expected: {str.__name__}"
            )
        self._seq_no = doc_source.get("_seq_no")
        self._primary_term = doc_source.get("_primary_term")
        self._source = doc_source.get("_source", {})
        if not isinstance(self._source, dict):
            raise InvalidDocumentSourceError(
                f"Invalid type found for source: {type(self._source).__name__}, expected: {dict.__name__}"
            )

    def get(self, *keys, default=None):
        value = self._source
        for key in keys:
            if not isinstance(value, dict):
                return default
            value = value.get(key)
        if value is None:
            return default
        return value

    async def reload(self):
        doc_source = await self.index.fetch_response_by_id(self.id)
        self._seq_no = doc_source.get("_seq_no")
        self._primary_term = doc_source.get("_primary_term")
        self._source = doc_source.get("_source", {})
