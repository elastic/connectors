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
from asyncpg.types import (
    BitString,
    Box,
    Circle,
    Line,
    LineSegment,
    Path,
    Point,
    Polygon,
)

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

event_loop = asyncio.get_event_loop()


def get_num_docs():
    print(NUM_TABLES * (RECORD_COUNT - RECORDS_TO_DELETE) + 3)


async def load():
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
                "(name, age, description) VALUES ($1, $2, $3)"
            )
            await connect.executemany(sql_query, rows)
            inserted += batch_size
            print(f"Inserting batch #{batch} of {batch_size} documents.")

    async def create_special_types_table():
        """Create a table with PostgreSQL special types that require serialization."""
        connect = await asyncpg.connect(CONNECTION_STRING)

        print("Creating special_types table for serialization testing...")
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS special_types (
            id SERIAL PRIMARY KEY,
            ip_inet INET,
            ip_cidr CIDR,
            uuid_col UUID,
            point_col POINT,
            line_col LINE,
            lseg_col LSEG,
            box_col BOX,
            path_col PATH,
            polygon_col POLYGON,
            circle_col CIRCLE,
            bit_col BIT(8),
            varbit_col VARBIT(16),
            inet_array INET[],
            uuid_array UUID[]
        )
        """
        await connect.execute(create_table_sql)

        print("Inserting special type test data...")
        insert_sql = """
        INSERT INTO special_types (
            ip_inet, ip_cidr, uuid_col,
            point_col, line_col, lseg_col, box_col, path_col, polygon_col, circle_col,
            bit_col, varbit_col,
            inet_array, uuid_array
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        """

        test_rows = [
            (
                "192.168.1.1",
                "10.0.0.0/8",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                Point(1.5, 2.5),
                Line(1, -1, 0),
                LineSegment((0, 0), (1, 1)),
                Box((2, 2), (0, 0)),
                Path((0, 0), (1, 1), (2, 0)),
                Polygon((0, 0), (1, 0), (1, 1), (0, 1)),
                Circle((0, 0), 5),
                BitString("10101010"),
                BitString("1101"),
                ["192.168.1.1", "10.0.0.1"],
                [
                    "550e8400-e29b-41d4-a716-446655440000",
                    "f47ac10b-58cc-4372-a567-0e02b2c3d479",
                ],
            ),
            (
                "2001:db8::1",
                "2001:db8::/32",
                "123e4567-e89b-12d3-a456-426614174000",
                Point(-3.14, 2.71),
                Line(2, 3, -6),
                LineSegment((-1, -1), (1, 1)),
                Box((10, 10), (-10, -10)),
                Path((0, 0), (3, 0), (3, 3), (0, 3), is_closed=True),
                Polygon((-1, -1), (1, -1), (1, 1), (-1, 1)),
                Circle((5, 5), 2.5),
                BitString("11110000"),
                BitString("101010101010"),
                ["::1", "fe80::1"],
                ["aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"],
            ),
            (
                "10.30.0.9/24",
                "172.16.0.0/12",
                "00000000-0000-0000-0000-000000000000",
                Point(0, 0),
                Line(0, 1, 0),
                LineSegment((5, 5), (10, 10)),
                Box((100, 100), (50, 50)),
                Path((0, 0), (5, 0), (5, 5)),
                Polygon((0, 0), (4, 0), (4, 3), (0, 3)),
                Circle((0, 0), 10),
                BitString("00000000"),
                BitString("1111111111111111"),
                ["192.0.2.1", "198.51.100.1", "203.0.113.1"],
                [
                    "12345678-1234-5678-1234-567812345678",
                    "87654321-4321-8765-4321-876543218765",
                ],
            ),
        ]

        await connect.executemany(insert_sql, test_rows)
        print(f"Inserted {len(test_rows)} rows with special types")

        await connect.close()

    async def load_rows():
        """N tables of 10001 rows each. each row is ~ 1024*20 bytes"""
        connect = await asyncpg.connect(CONNECTION_STRING)
        for table in range(NUM_TABLES):
            print(f"Adding data from table #{table}...")
            sql_query = f"CREATE TABLE IF NOT EXISTS customers_{table} (id SERIAL PRIMARY KEY, name VARCHAR(255), age int, description TEXT)"
            await connect.execute(sql_query)
            await inject_lines(table, connect, RECORD_COUNT)
        await connect.close()

    await create_readonly_user()
    await load_rows()
    await create_special_types_table()


async def remove():
    """Remove documents from tables"""
    connect = await asyncpg.connect(CONNECTION_STRING)
    for table in range(NUM_TABLES):
        rows = [
            (row_id,)
            for row_id in random.sample(range(1, RECORD_COUNT), RECORDS_TO_DELETE)
        ]
        sql_query = f"DELETE FROM customers_{table} WHERE id IN ($1)"
        await connect.executemany(sql_query, rows)
    await connect.close()
