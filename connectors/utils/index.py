#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio
from connectors.logger import logger
from elasticsearch.exceptions import ApiError
from connectors.utils import ESClient

# @TODO move to utils and name it ESIndex and ESDocument

class Index(ESClient):
  def __init__(self, index_name, elastic_config):
    # initialize elasticsearch client
    super().__init__(elastic_config)
    self.index_name = index_name
    self.elastic_config = elastic_config

  def _create_object(self, doc):
    raise NotImplementedError

  async def get_docs(self):
    await self.client.indices.refresh(index=self.index_name)

    try:
        resp = await self.client.search(
            index=self.index_name,
            query={"match_all": {}},
            size=20,
            expand_wildcards="hidden",
        )
    except ApiError as e:
        logger.critical(f"The server returned {e.status_code}")
        logger.critical(e.body, exc_info=True)
        return

    logger.debug(f"Found {len(resp['hits']['hits'])} documents")
    for doc in resp["hits"]["hits"]:
      yield self._create_object(doc)
