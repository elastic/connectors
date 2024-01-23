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
_NUM_TABLES = {"small": 2, "medium": 4, "large": 6}
NUM_TABLES = _NUM_TABLES[DATA_SIZE]
ENDPOINT = "redis://localhost:6379/"

fake_provider = WeightedFakeProvider(weights=[0.65, 0.3, 0.05, 0])


async def inject_lines(redis_client, table, start, lines):
    rows = {}
    for row_id in range(lines):
        row_id += start
        key = f"user_{row_id}"
        rows[key] = fake_provider.get_text()
    await redis_client.mset(rows)


async def load():
    """N tables of 10001 rows each. each row is ~ 1024*20 bytes"""
    for table in range(NUM_TABLES):
        print(f"Adding data in {table}...")
        redis_client = await redis.from_url(f"{ENDPOINT}{table}")
        for i in range(7):
            await inject_lines(redis_client, table, i * 1000, 100)


async def remove():
    """Removes 10 random items per table"""
    for table in range(NUM_TABLES):
        print(f"Working on table {table}...")
        redis_client = await redis.from_url(f"{ENDPOINT}{table}")
        keys = [f"user_{row_id}" for row_id in random.sample(range(1, 100), 10)]
        print(keys)
        await redis_client.delete(*keys)
