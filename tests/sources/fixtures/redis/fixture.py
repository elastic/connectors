#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
import os
import random

import redis.asyncio as redis

from tests.commons import WeightedFakeProvider

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
_NUM_DB = {"small": 2, "medium": 4, "large": 16}
NUM_DB = _NUM_DB[DATA_SIZE]
RECORDS_TO_DELETE = 10
EACH_ROW_ITEMS = 500
ENDPOINT = "redis://localhost:6379/"

fake_provider = WeightedFakeProvider(weights=[0.65, 0.3, 0.05, 0])


async def inject_lines(redis_client, start, lines):
    text = fake_provider.get_text()
    rows = {}
    for row_id in range(lines):
        key = f"user_{row_id}_{start}"
        rows[key] = text
    await redis_client.mset(rows)


async def load():
    """N databases of 500 rows each. each row is ~ 1024*20 bytes"""
    redis_client = await redis.from_url(f"{ENDPOINT}")
    for db in range(NUM_DB):
        print(f"Adding data in {db}...")
        await redis_client.execute_command("SELECT", db)
        await inject_lines(redis_client, db, EACH_ROW_ITEMS)


async def remove():
    """Removes 10 random items per db"""
    redis_client = await redis.from_url(f"{ENDPOINT}")
    for db in range(NUM_DB):
        print(f"Working on db {db}...")
        await redis_client.execute_command("SELECT", db)
        keys = [
            f"user_{row_id}_{db}"
            for row_id in random.sample(range(1, 100), RECORDS_TO_DELETE)
        ]
        await redis_client.delete(*keys)
