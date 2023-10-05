#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import random

import pytds

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

DATABASE_NAME = "xe"
HOST = "127.0.0.1"
PORT = 9090
USER = "admin"
PASSWORD = "Password_123"


fake_provider = WeightedFakeProvider(
    weights=[0.65, 0.3, 0.05, 0]
)  # SQL does not like huge blobs

BATCH_SIZE = 1000
DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

match DATA_SIZE:
    case "small":
        NUM_TABLES = 1
        RECORD_COUNT = 800
    case "medium":
        NUM_TABLES = 3
        RECORD_COUNT = 5000
    case "large":
        NUM_TABLES = 5
        RECORD_COUNT = 10000

RECORDS_TO_DELETE = 10


def get_num_docs():
    print(NUM_TABLES * (RECORD_COUNT - RECORDS_TO_DELETE))


def inject_lines(table, cursor, lines):
    """Ingest rows in table

    Args:
        table (str): Name of table
        cursor (cursor): Cursor to execute query
        start (int): Starting row
        lines (int): Number of rows
    """
    batch_count = max(int(lines / BATCH_SIZE), 1)
    inserted = 0
    print(f"Inserting {lines} lines in total {batch_count} batches")
    for batch in range(batch_count):
        rows = []
        batch_size = min(BATCH_SIZE, lines - inserted)
        for row_id in range(batch_size):
            rows.append((fake_provider.fake.name(), row_id, fake_provider.get_text()))
        sql_query = f"INSERT INTO customers_{table} (name, age, description) VALUES (%s, %s, %s)"
        cursor.executemany(sql_query, rows)
        inserted += batch_size
        print(f"Inserted batch #{batch} of {batch_size} documents.")


def load():
    """N tables of 10001 rows each. each row is ~ 1024*20 bytes"""

    database_sa = pytds.connect(
        server=HOST, port=PORT, user="sa", password=PASSWORD, autocommit=True
    )
    cursor = database_sa.cursor()
    cursor.execute("CREATE LOGIN admin WITH PASSWORD = 'Password_123'")
    cursor.execute("ALTER SERVER ROLE [sysadmin] ADD MEMBER [admin]")
    cursor.close()
    database_sa.close()
    database = pytds.connect(server=HOST, port=PORT, user=USER, password=PASSWORD)
    database.autocommit = True
    cursor = database.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE_NAME}")
    cursor.execute(f"CREATE DATABASE {DATABASE_NAME}")
    cursor.execute(f"USE {DATABASE_NAME}")
    database.autocommit = False

    for table in range(NUM_TABLES):
        print(f"Adding data to table customers_{table}...")
        sql_query = f"CREATE TABLE customers_{table} (id INT IDENTITY(1,1), name VARCHAR(255), age int, description TEXT, PRIMARY KEY (id))"
        cursor.execute(sql_query)
        inject_lines(table, cursor, RECORD_COUNT)
    database.commit()


def remove():
    """Removes 10 random items per table"""

    database = pytds.connect(server=HOST, port=PORT, user=USER, password=PASSWORD)
    cursor = database.cursor()
    cursor.execute(f"USE {DATABASE_NAME}")
    for table in range(NUM_TABLES):
        rows = [(row_id,) for row_id in random.sample(range(1, RECORD_COUNT), RECORDS_TO_DELETE)]
        sql_query = f"DELETE from customers_{table} where id in (%s)"
        cursor.executemany(sql_query, rows)
    database.commit()
