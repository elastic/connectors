from datetime import datetime
from bson import Decimal128
from motor.motor_asyncio import AsyncIOMotorClient
from connectors.elastic import ElasticServer


class MongoConnector:
    def __init__(self, definition):
        self.definition = definition
        self.host = definition.get("host", "mongodb://127.0.0.1:27021")
        self.database = definition["database"]
        self.collection = definition["collection"]
        self.client = AsyncIOMotorClient(
            self.host,
            directConnection=True,
            connectTimeoutMS=60,
            socketTimeoutMS=60,
        )
        self.db = self.client[self.database]

    async def ping(self):
        await self.client.admin.command("ping")

    def serialize(self, doc):
        # make this generic
        for review in doc["reviews"]:
            review["date"] = review["date"].isoformat()
        for key, value in doc.items():
            if isinstance(value, datetime):
                doc[key] = value.isoformat()
            if isinstance(value, Decimal128):
                doc[key] = value.to_decimal()
        return doc

    async def get_docs(self):
        async for doc in self.db[self.collection].find():
            yield self.serialize(doc)
