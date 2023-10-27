#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
import asyncio
import os
import random

import asyncpg

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider(weights=[0.65, 0.3, 0.05, 0])

CONNECTION_STRING = "postgresql://admin:Password_123@127.0.0.1:9090/xe"
BATCH_SIZE = 100
DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

READONLY_USERNAME = "readonly"
READONLY_PASSWORD = "foobar123"

match DATA_SIZE:
    case "small":
        NUM_TABLES = 1
        RECORD_COUNT = 500
    case "medium":
        NUM_TABLES = 3
        RECORD_COUNT = 3000
    case "large":
        NUM_TABLES = 5
        RECORD_COUNT = 7000

RECORDS_TO_DELETE = 10


def get_num_docs():
    print(NUM_TABLES * (RECORD_COUNT - RECORDS_TO_DELETE))


def load():
    """Create a read-only user for use when configuring the connector,
    then create tables and load table data."""

    async def create_readonly_user():
        connect = await asyncpg.connect(CONNECTION_STRING)
        sql_query = (
            f"CREATE USER {READONLY_USERNAME} PASSWORD '{READONLY_PASSWORD}'; "
            f"GRANT pg_read_all_data TO {READONLY_USERNAME};"
        )
        await connect.execute(sql_query)
        await connect.close()

    async def inject_lines(table, connect, lines):
        """Ingest rows in table

        Args:
            table (str): Name of table
            connect (connection): Connection to execute query
            start (int): Starting row
            lines (int): Number of rows
        """
        batch_count = int(lines / BATCH_SIZE)
        inserted = 0
        print(f"Inserting {lines} lines")
        for batch in range(batch_count):
            rows = []
            batch_size = min(BATCH_SIZE, lines - inserted)
            for row_id in range(batch_size):
                rows.append(
                    (fake_provider.fake.name(), row_id, fake_provider.get_text())
                )
            sql_query = (
                f"INSERT INTO customers_{table}"
                + "(name, age, description) VALUES ($1, $2, $3)"
            )
            await connect.executemany(sql_query, rows)
            inserted += batch_size
            print(f"Inserting batch #{batch} of {batch_size} documents.")

    async def load_rows():
        """N tables of 10001 rows each. each row is ~ 1024*20 bytes"""
        connect = await asyncpg.connect(CONNECTION_STRING)
        for table in range(NUM_TABLES):
            print(f"Adding data from table #{table}...")
            sql_query = f"CREATE TABLE IF NOT EXISTS customers_{table} (id SERIAL PRIMARY KEY, name VARCHAR(255), age int, description TEXT)"
            await connect.execute(sql_query)
            await inject_lines(table, connect, RECORD_COUNT)
        await connect.close()

    asyncio.get_event_loop().run_until_complete(create_readonly_user())
    asyncio.get_event_loop().run_until_complete(load_rows())


def remove():
    """Remove documents from tables"""

    async def remove_rows():
        """Removes 10 random items per table"""
        connect = await asyncpg.connect(CONNECTION_STRING)
        for table in range(NUM_TABLES):
            rows = [
                (row_id,)
                for row_id in random.sample(range(1, RECORD_COUNT), RECORDS_TO_DELETE)
            ]
            sql_query = f"DELETE FROM customers_{table} WHERE id IN ($1)"
            await connect.executemany(sql_query, rows)
        await connect.close()

    asyncio.get_event_loop().run_until_complete(remove_rows())
