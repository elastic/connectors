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

    name = "MongoDB"
    service_type = "mongodb"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

        client_params = {}

        host = self.configuration["host"]
        user = self.configuration["user"]
        password = self.configuration["password"]

        if self.configuration["direct_connection"]:
            client_params["directConnection"] = True

        if len(user) > 0 or len(password) > 0:
            client_params["username"] = user
            client_params["password"] = password

        self.client = AsyncIOMotorClient(host, **client_params)

        self.db = self.client[self.configuration["database"]]

    @classmethod
    def get_default_configuration(cls):
        return {
            "host": {
                "value": "mongodb://127.0.0.1:27021",
                "label": "Server Hostname",
                "type": "str",
            },
            "database": {
                "value": "sample_database",
                "label": "Database",
                "type": "str",
            },
            "collection": {
                "value": "sample_collection",
                "label": "Collection",
                "type": "str",
            },
            "user": {"label": "Username", "type": "str", "value": ""},
            "password": {"label": "Password", "type": "str", "value": ""},
            "direct_connection": {
                "label": "Direct connection? (true/false)",
                "type": "bool",
                "value": True,
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
