#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Implementation of BYOEI protocol (+some ids collecting)
"""
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
        logger.debug(f"Scanning existing index {index}")
        try:
            await self.client.indices.get(index=index)
        except ElasticNotFoundError:
            return

        async for doc in async_scan(
            client=self.client, index=index, _source=["id"], size=2000
        ):
            yield doc["_id"]

    async def async_bulk(self, index, generator):
        existing_ids = frozenset(
            [doc_id async for doc_id in self.get_existing_ids(index)]
        )
        logger.debug(f"Found {len(existing_ids)} docs in {index}")
        seen_ids = set()

        async def get_docs():
            async for doc in generator:
                doc_id = doc["_id"]
                seen_ids.add(doc_id)
                doc["id"] = doc_id
                doc["timestamp"] = iso_utc()

                del doc["_id"]
                yield {
                    "_op_type": "update",
                    "_index": index,
                    "_id": doc_id,
                    "doc": doc,
                    "doc_as_upsert": True,
                }

            for doc_id in existing_ids:
                if doc_id in seen_ids:
                    continue
                yield {"_op_type": "delete", "_index": index, "_id": doc_id}

        res = defaultdict(int)

        async for ok, result in async_streaming_bulk(self.client, get_docs()):
            action, result = result.popitem()
            if not ok:
                logger.exception(f"Failed to {action} see {result}")
            res[action] += 1

        return dict(res)
