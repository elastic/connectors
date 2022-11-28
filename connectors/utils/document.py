#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio
from connectors.logger import logger
from elasticsearch.exceptions import ApiError


class Document:
  def __init__(self, elastic_index, id, doc_source):
    self.elastic_index = elastic_index
    self.id = id
    self.doc_source = doc_source

  async def reload(self):
    resp = await self.elastic_client.get(
      index_name=self.index_name,
      id=self.id
    )

    return resp

  async def update(self):
    print("reload placeholder")



