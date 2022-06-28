import asyncio
import json
from datetime import datetime
from collections import defaultdict

from elasticsearch import AsyncElasticsearch, NotFoundError as ElasticNotFoundError
from elasticsearch.helpers import async_scan, async_streaming_bulk
from bson import Decimal128

from motor.motor_asyncio import AsyncIOMotorClient


IDLING = 10
CONNECTORS_INDEX = ".elastic-connectors"


class ElasticServer:
    def __init__(self):
        self.client = AsyncElasticsearch(
            hosts=["http://localhost:9200"], basic_auth=("elastic", "changeme")
        )

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
        print("get_existing_ids")
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
                print("failed to %s document %s" % ())
            res[action] += 1

        return dict(res)


class MongoConnector:
    def __init__(self, definition):
        self.definition = definition
        self.client = AsyncIOMotorClient(
            "mongodb://127.0.0.1:27021",
            directConnection=True,
            connectTimeoutMS=30,
            socketTimeoutMS=30,
        )
        self.db = self.client.sample_airbnb

    async def ping(self):
        await self.client.admin.command("ping")

    async def get_docs(self):
        async for doc in self.db.listingsAndReviews.find():
            yield doc


def get_connector_instance(definition):
    return MongoConnector(definition)


async def poll():
    es = ElasticServer()
    await es.prepare()

    while True:
        print("poll")
        async for definition in es.get_connectors_definitions():
            connector = get_connector_instance(definition)
            await connector.ping()

            es_index = definition["es_index"]
            await es.prepare_index(es_index)
            existing_ids = [doc_id async for doc_id in es.get_existing_ids(es_index)]

            async def get_docs():
                async for doc in connector.get_docs():
                    if doc["_id"] in existing_ids:
                        continue

                    doc_id = doc["_id"]
                    for review in doc["reviews"]:
                        review["date"] = review["date"].isoformat()
                    for key, value in doc.items():
                        if isinstance(value, datetime):
                            doc[key] = value.isoformat()
                        if isinstance(value, Decimal128):
                            doc[key] = value.to_decimal()
                    doc["id"] = doc_id
                    del doc["_id"]
                    yield {
                        "_op_type": "update",
                        "_index": es_index,
                        "_id": doc_id,
                        "doc": doc,
                        "doc_as_upsert": True,
                    }

            result = await es.async_bulk(get_docs())
            print(result)

        await asyncio.sleep(IDLING)


loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(poll())
except (asyncio.CancelledError, KeyboardInterrupt):
    print("Bye")
