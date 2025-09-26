#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
import os
import random

from mysql.connector import connect

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider(weights=[0.65, 0.3, 0.05, 0])

DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

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
BATCH_SIZE = 100
DATABASE_NAME = "customerinfo"


def get_num_docs():
    # +1 for the composite key table
    print((NUM_TABLES + 1) * (RECORD_COUNT - RECORDS_TO_DELETE))


def inject_lines(table, cursor, lines):
    batch_count = max(int(lines / BATCH_SIZE), 1)

    inserted = 0
    print(f"Inserting {lines} lines in {batch_count} batches")
    for batch in range(batch_count):
        rows = []
        batch_size = min(BATCH_SIZE, lines - inserted)
        for row_id in range(batch_size):
            rows.append((fake_provider.fake.name(), row_id, fake_provider.get_text()))
        sql_query = f"INSERT INTO customers_{table} (name, age, description) VALUES (%s, %s, %s)"
        cursor.executemany(sql_query, rows)
        inserted += batch_size
        print(f"Inserting batch #{batch} of {batch_size} documents.")


def create_table_with_composite_key(cursor):
    """Create a table with composite primary key"""
    print("Adding table with composite primary key...")
    composite_table_query = """
    CREATE TABLE IF NOT EXISTS orders (
        customer_id INT,
        product_name VARCHAR(255),
        quantity INT,
        order_date DATE,
        order_id INT,
        PRIMARY KEY (customer_id, order_id)
    )
    """
    cursor.execute(composite_table_query)

    rows = []
    for _ in range(RECORD_COUNT):
        rows.append(
            (
                fake_provider.fake.random_int(min=1, max=5000),  # customer_id
                fake_provider.fake.name(),  # product_name
                fake_provider.fake.random_int(min=1, max=10),  # quantity
                fake_provider.fake.date(),  # order_date
                fake_provider.fake.random_int(min=1, max=5000),  # order_id
            )
        )

    composite_query = "INSERT INTO orders (customer_id, product_name, quantity, order_date, order_id) VALUES (%s, %s, %s, %s, %s)"

    batch_count = max(int(RECORD_COUNT / BATCH_SIZE), 1)
    print(f"Inserting {RECORD_COUNT} lines in {batch_count} batches")
    for batch in range(batch_count):
        batch_of_rows = rows[batch * BATCH_SIZE : (batch + 1) * BATCH_SIZE]
        cursor.executemany(composite_query, batch_of_rows)
        print(f"Inserting batch #{batch} of {len(batch_of_rows)} documents.")


async def load():
    """N tables of 10001 rows each. each row is ~ 1024*20 bytes"""
    database = connect(host="127.0.0.1", port=3306, user="root", password="changeme")
    cursor = database.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE_NAME}")
    cursor.execute(f"CREATE DATABASE {DATABASE_NAME}")
    cursor.execute(f"USE {DATABASE_NAME}")
    for table in range(NUM_TABLES):
        print(f"Adding data from table #{table}...")
        sql_query = f"CREATE TABLE IF NOT EXISTS customers_{table} (id INT AUTO_INCREMENT, name VARCHAR(255), age int, description LONGTEXT, PRIMARY KEY (id))"
        cursor.execute(sql_query)
        inject_lines(table, cursor, RECORD_COUNT)

    create_table_with_composite_key(cursor)
    database.commit()


async def remove():
    """Removes 10 random items per table"""
    database = connect(host="127.0.0.1", port=3306, user="root", password="changeme")
    cursor = database.cursor()
    cursor.execute(f"USE {DATABASE_NAME}")
    for table in range(NUM_TABLES):
        print(f"Working on table {table}...")
        rows = [
            (row_id,) for row_id in random.sample(range(1, 1000), RECORDS_TO_DELETE)
        ]
        print(rows)
        sql_query = f"DELETE from customers_{table} WHERE id IN (%s)"
        cursor.executemany(sql_query, rows)
    database.commit()
