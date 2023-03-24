import os
import random
import string
import aioredis

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
_NUM_TABLES = {"small": 5, "medium": 10, "large": 30}
NUM_TABLES = _NUM_TABLES[DATA_SIZE]


def random_text(k=1024 * 20):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


BIG_TEXT = random_text()


async def inject_lines(redis, table, start, lines):
    rows = {}
    for row_id in range(lines):
        row_id += start
        key = f"user_{row_id}"
        rows[key] = BIG_TEXT
    await redis.mset(rows)


async def load():
    """N tables of 10001 rows each. each row is ~ 1024*20 bytes"""
    for table in range(NUM_TABLES):
        print(f"Adding data in {table}...")
        redis = await aioredis.from_url(f"redis://localhost:6379/{table}")
        for i in range(10):
            await inject_lines(redis, table, i * 1000, 1000)


async def remove():
    """Removes 10 random items per table"""
    for table in range(NUM_TABLES):
        print(f"Working on table {table}...")
        redis = await aioredis.from_url(f"redis://localhost:6379/{table}")
        keys = [f"user_{row_id}" for row_id in random.sample(range(1, 1000), 10)]
        print(keys)
        await redis.delete(*keys)