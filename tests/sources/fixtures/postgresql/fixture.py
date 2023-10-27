#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
import random
import string

import asyncpg

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
CONNECTION_STRING = "postgresql://admin:Password_123@127.0.0.1:9090/xe"
_SIZES = {"small": 5, "medium": 10, "large": 30}
NUM_TABLES = _SIZES[DATA_SIZE]

READONLY_USERNAME = "readonly"
READONLY_PASSWORD = "foobar123"


def random_text(k=1024 * 20):
    """Function to generate random text

    Args:
        k (int, optional): size of data in bytes. Defaults to 1024*20.

    Returns:
        string: random text
    """
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


BIG_TEXT = random_text()


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

    async def inject_lines(table, connect, start, lines):
        """Ingest rows in table

        Args:
            table (str): Name of table
            connect (connection): Connection to execute query
            start (int): Starting row
            lines (int): Number of rows
        """
        rows = []
        for row_id in range(lines):
            row_id += start
            rows.append((f"user_{row_id}", row_id, BIG_TEXT))
        sql_query = (
            f"INSERT INTO customers_{table}"
            + "(name, age, description) VALUES ($1, $2, $3)"
        )
        await connect.executemany(sql_query, rows)

    async def load_rows():
        """N tables of 10001 rows each. each row is ~ 1024*20 bytes"""
        connect = await asyncpg.connect(CONNECTION_STRING)
        for table in range(NUM_TABLES):
            print(f"Adding data from table #{table}...")
            sql_query = f"CREATE TABLE IF NOT EXISTS customers_{table} (name VARCHAR(255), age int, description TEXT, PRIMARY KEY (name))"
            await connect.execute(sql_query)
            for i in range(10):
                await inject_lines(table, connect, i * 1000, 1000)
        await connect.close()

    asyncio.get_event_loop().run_until_complete(create_readonly_user())
    asyncio.get_event_loop().run_until_complete(load_rows())


def remove():
    """Remove documents from tables"""

    async def remove_rows():
        """Removes 10 random items per table"""
        connect = await asyncpg.connect(CONNECTION_STRING)
        for table in range(NUM_TABLES):
            rows = [(f"user_{row_id}",) for row_id in random.sample(range(1, 1000), 10)]
            sql_query = f"DELETE from customers_{table} where name=$1"
            await connect.executemany(sql_query, rows)
        await connect.close()

    asyncio.get_event_loop().run_until_complete(remove_rows())
