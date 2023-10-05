#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import random
import string

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
    print(NUM_TABLES * (RECORD_COUNT - RECORDS_TO_DELETE))


def inject_lines(table, cursor, lines):
    batch_count = max(int(lines / BATCH_SIZE), 1)

    inserted = 0
    print(f"Inserting {lines} lines")
    for batch in range(batch_count):
        rows = []
        batch_size = min(BATCH_SIZE, lines - inserted)
        for row_id in range(batch_size):
            rows.append((fake_provider.fake.name(), row_id, fake_provider.get_text()))
        sql_query = (
            f"INSERT INTO customers_{table} (name, age, description) VALUES (%s, %s, %s)"
        )
        cursor.executemany(sql_query, rows)
        inserted += batch_size
        print(f"Inserting batch #{batch} of {batch_size} documents.")


def load():
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

    database.commit()


def remove():
    """Removes 10 random items per table"""
    database = connect(host="127.0.0.1", port=3306, user="root", password="changeme")
    cursor = database.cursor()
    cursor.execute(f"USE {DATABASE_NAME}")
    for table in range(NUM_TABLES):
        print(f"Working on table {table}...")
        rows = [(row_id,) for row_id in random.sample(range(1, 1000), RECORDS_TO_DELETE)]
        print(rows)
        sql_query = f"DELETE from customers_{table} WHERE id IN (%s)"
        cursor.executemany(sql_query, rows)
    database.commit()
