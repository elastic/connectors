from collections import defaultdict
from elasticsearch import AsyncElasticsearch, NotFoundError as ElasticNotFoundError
from elasticsearch.helpers import async_scan, async_streaming_bulk

from connectors.logger import logger


CONNECTORS_INDEX = ".elastic-connectors"


class ElasticServer:
    def __init__(self, config):
        logger.info(f"Connecting to {config['host']}")
        self.host = config["host"]
        self.auth = config["user"], config["password"]
        self.client = AsyncElasticsearch(hosts=[self.host], basic_auth=self.auth)

    async def get_connectors_definitions(self):
        resp = await self.client.search(
            index=CONNECTORS_INDEX,
            body={"query": {"match_all": {}}},
            size=20,
        )
        for hit in resp["hits"]["hits"]:
            yield hit["_source"]

    async def prepare_index(self, index, docs=None, mapping=None):
        try:
            await self.client.indices.get(index=index, expand_wildcards="hidden")
        except ElasticNotFoundError:
            await self.client.indices.create(index=CONNECTORS_INDEX)
            if docs is None:
                return
            # XXX bulk
            doc_id = 1
            for doc in docs:
                await self.client.index(index=CONNECTORS_INDEX, id=doc_id, document=doc)
                doc_id += 1

    async def prepare(self):
        doc = {
            "service_type": "mongo",
            "sync_now": True,
            "es_index": "search-airbnb",
            "last_synced": "",
            "scheduling": {"interval": "*"},
        }

        await self.prepare_index(CONNECTORS_INDEX, [doc])

    async def get_existing_ids(self, index):
        logger.debug("get_existing_ids")
        try:
            await self.client.indices.get(index=index)
        except ElasticNotFoundError:
            return

        async for doc in async_scan(
            client=self.client, index=index, _source=["id"], size=2000
        ):
            yield doc["_id"]

    async def async_bulk(self, generator):
        res = defaultdict(int)

        async for ok, result in async_streaming_bulk(self.client, generator):
            action, result = result.popitem()
            if not ok:
                logger.exception("failed to %s document %s" % ())
            res[action] += 1

        return dict(res)
