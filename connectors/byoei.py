#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Implementation of BYOEI protocol (+some ids collecting)
"""
import time
from collections import defaultdict

from elasticsearch import AsyncElasticsearch, NotFoundError as ElasticNotFoundError
from elasticsearch.helpers import async_scan, async_streaming_bulk
from connectors.logger import logger
from connectors.utils import iso_utc


class ElasticServer:
    def __init__(self, elastic_config):
        logger.debug(f"ElasticServer connecting to {elastic_config['host']}")
        self.host = elastic_config["host"]
        self.auth = elastic_config["user"], elastic_config["password"]
        self.client = AsyncElasticsearch(hosts=[self.host], basic_auth=self.auth)

    async def close(self):
        await self.client.close()

    async def prepare_index(self, index, docs=None, mapping=None, delete_first=False):
        """Creates the index, given a mappingm if it does not exists."""
        # XXX todo update the existing index with the new mapping
        logger.debug(f"Checking index {index}")
        exists = await self.client.indices.exists(
            index=index, expand_wildcards="hidden"
        )
        if exists:
            logger.debug(f"{index} exists")
            if not delete_first:
                return
            logger.debug("Deleting it first")
            await self.client.indices.delete(index=index, expand_wildcards="hidden")

        logger.debug(f"Creating index {index}")
        await self.client.indices.create(index=index)
        if docs is None:
            return
        # XXX bulk
        doc_id = 1
        for doc in docs:
            await self.client.index(index=index, id=doc_id, document=doc)
            doc_id += 1

    async def get_existing_ids(self, index):
        """Returns an iterator on the `id` and `timestamp` fields of all documents in an index."""

        logger.debug(f"Scanning existing index {index}")
        try:
            await self.client.indices.get(index=index)
        except ElasticNotFoundError:
            return

        async for doc in async_scan(
            client=self.client,
            index=index,
            _source=["id", "timestamp"],
        ):
            yield doc["_source"]

    async def async_bulk(self, index, generator):
        start = time.time()
        existing_ids = set()
        existing_timestamps = {}

        async for doc in self.get_existing_ids(index):
            existing_ids.add(doc["id"])
            existing_timestamps[doc["id"]] = doc["timestamp"]

        logger.debug(
            f"Found {len(existing_ids)} docs in {index} (duration "
            f"{int(time.time() - start)} seconds)"
        )
        seen_ids = set()

        async def get_docs():
            async for doc in generator:
                doc_id = doc["id"] = doc.pop("_id")
                seen_ids.add(doc_id)

                # If the doc has a timestamp, we can use it to see if it has
                # been modified. This reduces the bulk size a *lot*
                #
                # Some backends do not know how to do this so it's optional.
                # For them we update the docs in any case.
                if "timestamp" in doc:
                    if existing_timestamps.get(doc_id, "") == doc["timestamp"]:
                        continue
                else:
                    doc["timestamp"] = iso_utc()

                yield {
                    "_op_type": "update",
                    "_index": index,
                    "_id": doc_id,
                    "doc": doc,
                    "doc_as_upsert": True,
                }

            # We delete any document that existed in Elasticsearch that was not
            # returned by the backend.
            for doc_id in existing_ids:
                if doc_id in seen_ids:
                    continue
                yield {"_op_type": "delete", "_index": index, "_id": doc_id}

        res = defaultdict(int)

        # The async_streaming_bulk helper batches bulk requests for us.
        async for ok, result in async_streaming_bulk(self.client, get_docs()):
            action, result = result.popitem()
            if not ok:
                logger.exception(f"Failed to {action} see {result}")
            res[action] += 1

        # we return a number for each operation type.
        return dict(res)
