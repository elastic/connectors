#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from connectors.logger import logger


class InvalidDocumentSourceError(Exception):
    pass


class ESDocument:
    def __init__(self, elastic_index, doc_source):
        self.index = elastic_index
        try:
            self.id = doc_source.get("_id")
            if not isinstance(self.id, str):
                raise InvalidDocumentSourceError(
                    f"Invalid format found for id: {self.id}"
                )
            self._source = doc_source.get("_source", {})
            if not isinstance(self._source, dict):
                raise InvalidDocumentSourceError(
                    f"Invalid format found for source: {self._source}"
                )
        except InvalidDocumentSourceError:
            raise
        except Exception as e:
            logger.critical(e, exc_info=True)
            raise InvalidDocumentSourceError(f"Invalid doc source found: {doc_source}")

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
        return await self.index.fetch_by_id(self.id)
