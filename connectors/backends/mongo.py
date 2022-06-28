from datetime import datetime
from bson import Decimal128
from motor.motor_asyncio import AsyncIOMotorClient
from connectors.elastic import ElasticServer


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
            # make this generic
            for review in doc["reviews"]:
                review["date"] = review["date"].isoformat()
            for key, value in doc.items():
                if isinstance(value, datetime):
                    doc[key] = value.isoformat()
                if isinstance(value, Decimal128):
                    doc[key] = value.to_decimal()

            yield doc
