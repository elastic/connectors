import os

import bson
from faker import Faker
from pymongo import MongoClient

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
_SIZES = {"small": 750, "medium": 1500, "large": 3000}
NUMBER_OF_RECORDS_TO_DELETE = 50

fake = Faker()
client = MongoClient("mongodb://admin:justtesting@127.0.0.1:27021")


def setup():
    pass


def load():
    def _random_record():
        return {
            "id": bson.ObjectId(),
            "name": fake.name(),
            "address": fake.address(),
            "birthdate": fake.date(),
            "time": fake.time(),
            "comment": fake.sentence(),
        }

    record_number = _SIZES[DATA_SIZE] + NUMBER_OF_RECORDS_TO_DELETE

    print(f"Generating {record_number} random records")
    db = client.sample_database
    collection = db.sample_collection

    data = []
    for _ in range(record_number):
        data.append(_random_record())
    collection.insert_many(data)


def remove():
    db = client.sample_database
    collection = db.sample_collection

    records = collection.find().limit(NUMBER_OF_RECORDS_TO_DELETE)
    doc_ids = [rec.get("_id") for rec in records]

    query = {"_id": {"$in": doc_ids}}
    collection.delete_many(query)


def teardown():
    pass
