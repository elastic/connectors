import os

import bson
from random import choices
from pymongo import MongoClient
from tests.commons import FakeProvider


NUMBER_OF_RECORDS_TO_DELETE = 50

fake_provider = FakeProvider()
fake = fake_provider.fake
client = MongoClient("mongodb://admin:justtesting@127.0.0.1:27021")

DATA_SIZE = os.environ.get("DATA_SIZE", "medium")

match DATA_SIZE:
    case "small":
        RECORD_COUNT = 1000
    case "medium":
        RECORD_COUNT = 10000
    case "large":
        RECORD_COUNT = 100000

population = [fake_provider.small_text(), fake_provider.medium_text(), fake_provider.large_text()]
weights = [0.65, 0.3, 0.05]

def get_num_docs():
    # 2 is multiplier cause SPACE_OBJECTs will be delivered twice:
    # Test returns SPACE_OBJECT_COUNT objects for each type of content
    # There are 2 types of content:
    # - blogpost
    # - page
    print(RECORD_COUNT - NUMBER_OF_RECORDS_TO_DELETE)

def get_text():
    return choices(population, weights)[0]


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
            "comment": get_text()
        }

    record_number = RECORD_COUNT 

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
