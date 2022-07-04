from collections import defaultdict
from elasticsearch import AsyncElasticsearch, NotFoundError as ElasticNotFoundError
from elasticsearch.helpers import async_scan, async_streaming_bulk

from connectors.logger import logger
from connectors.connector import Connector


CONNECTORS_INDEX = ".elastic-connectors"


class ElasticServer:
    def __init__(self, config):
        logger.info(f"Connecting to {config['host']}")
        self.host = config["host"]
        self.auth = config["user"], config["password"]
        self.client = AsyncElasticsearch(hosts=[self.host], basic_auth=self.auth)

    async def close(self):
        await self.client.close()

    async def save_connector(self, connector_definition):
        body = dict(connector_definition)
        doc_id = body.pop("id")
        return await self.client.index(index=CONNECTORS_INDEX, id=doc_id, body=body)

    async def get_connectors_definitions(self):
        resp = await self.client.search(
            index=CONNECTORS_INDEX,
            body={"query": {"match_all": {}}},
            size=20,
            expand_wildcards="hidden",
        )
        for hit in resp["hits"]["hits"]:
            source = hit["_source"]
            source["id"] = hit["_id"]
            yield Connector(self, source)

    async def prepare_index(self, index, docs=None, mapping=None, delete_first=False):
        logger.debug(f"Checking index {index}")
        exists = await self.client.indices.exists(
            index=index, expand_wildcards="hidden"
        )
        if exists:
            logger.debug(f"{index} exists")
            if delete_first:
                logger.debug(f"Deleting it first")
                await self.client.indices.delete(index=index, expand_wildcards="hidden")
            else:
                return

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
        existing_ids = [doc_id async for doc_id in self.get_existing_ids(index)]

        logger.debug(f"Found {len(existing_ids)} docs in {index}")

        async def get_docs():
            async for doc in generator:
                if doc["_id"] in existing_ids:
                    continue
                doc_id = doc["_id"]
                doc["id"] = doc_id
                del doc["_id"]
                yield {
                    "_op_type": "update",
                    "_index": index,
                    "_id": doc_id,
                    "doc": doc,
                    "doc_as_upsert": True,
                }

        res = defaultdict(int)

        async for ok, result in async_streaming_bulk(self.client, get_docs()):
            action, result = result.popitem()
            if not ok:
                logger.exception("failed to %s document %s" % ())
            res[action] += 1

        return dict(res)
