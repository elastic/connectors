#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime

from bson import Decimal128, ObjectId
from motor.motor_asyncio import AsyncIOMotorClient

from connectors.logger import logger
from connectors.source import BaseDataSource


class MongoDataSource(BaseDataSource):
    """MongoDB"""

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self.client = AsyncIOMotorClient(
            self.configuration["host"],
            directConnection=True,
            connectTimeoutMS=120,
            socketTimeoutMS=120,
        )
        self.db = self.client[self.configuration["database"]]

    @classmethod
    def get_default_configuration(cls):
        return {
            "host": {
                "value": "mongodb://127.0.0.1:27021",
                "label": "MongoDB Host",
                "type": "str",
            },
            "database": {
                "value": "sample_database",
                "label": "MongoDB Database",
                "type": "str",
            },
            "collection": {
                "value": "sample_collection",
                "label": "MongoDB Collection",
                "type": "str",
            },
        }

    async def ping(self):
        await self.client.admin.command("ping")

    # XXX That's a lot of work...
    def serialize(self, doc):
        def _serialize(value):
            if isinstance(value, ObjectId):
                value = str(value)
            if isinstance(value, (list, tuple)):
                value = [_serialize(item) for item in value]
            elif isinstance(value, dict):
                for key, svalue in value.items():
                    value[key] = _serialize(svalue)
            elif isinstance(value, datetime):
                value = value.isoformat()
            elif isinstance(value, Decimal128):
                value = value.to_decimal()
            return value

        for key, value in doc.items():
            doc[key] = _serialize(value)

        return doc

    async def get_docs(self, filtering=None):
        logger.debug("Grabbing collection info")
        collection = self.db[self.configuration["collection"]]

        async for doc in collection.find():
            yield self.serialize(doc), None

        self._dirty = False
