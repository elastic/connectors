#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Oracle module responsible to generate records on the Oracle server.
"""
import os
from random import choices
import random

import oracledb

from tests.commons import FakeProvider
from connectors.utils import RetryStrategy, retryable

# If the Oracle Service takes too much time to respond, modify the following retry constants accordingly.
RETRY_INTERVAL = 4
RETRIES = 0
BATCH_SIZE = 100


fake_provider = FakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium")

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

population = [fake_provider.small_text(), fake_provider.medium_text(), fake_provider.large_text()]
weights = [0.65, 0.3, 0.05]

def get_text():
    return choices(population, weights)[0]

USER = "admin"
PASSWORD = "Password_123"
ENCODING = "UTF-8"
DSN = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=9090))(CONNECT_DATA=(SID=xe)))"


def inject_lines(table, cursor, lines):
    batch_count = int(lines / BATCH_SIZE)
    inserted = 0
    print(f"Inserting {lines} lines")
    for batch in range(batch_count):
        rows = []
        batch_size = min(BATCH_SIZE, lines - inserted)
        for row_id in range(batch_size):
            rows.append((fake_provider.fake.name(), row_id, get_text()))
        sql_query = f"INSERT into customers_{table}(name, age, description) VALUES (:1, :2, :3)"
        cursor.executemany(sql_query, rows)
        inserted += batch_size
        print(f"Inserted batch #{batch} of {batch_size} documents.")

def load():
    """Generate tables and loads table data in the oracle server."""
    """N tables of RECORD_COUNT rows each"""
    connection = oracledb.connect(
        user="system", password=PASSWORD, dsn=DSN, encoding=ENCODING
    )
    cursor = connection.cursor()
    cursor.execute("CREATE USER admin IDENTIFIED by Password_123")
    cursor.execute("GRANT CONNECT, RESOURCE, DBA TO admin")
    connection.commit()

    connection = oracledb.connect(
        user=USER, password=PASSWORD, dsn=DSN, encoding=ENCODING
    )
    cursor = connection.cursor()
    for table in range(NUM_TABLES):
        print(f"Adding data for table #{table}...")
        sql_query = f"CREATE TABLE customers_{table} (id NUMBER GENERATED AS IDENTITY, name VARCHAR(255), age int, description long, PRIMARY KEY (id))"
        cursor.execute(sql_query)
        inject_lines(table, cursor, RECORD_COUNT)
    connection.commit()


def remove():
    """Removes 10 random items per table"""
    connection = oracledb.connect(
        user=USER, password=PASSWORD, dsn=DSN, encoding=ENCODING
    )
    cursor = connection.cursor()

    for table in range(NUM_TABLES):
        rows = [(f"user_{row_id}",) for row_id in random.sample(range(1, 1000), 10)]
        sql_query = f"DELETE from customers_{table} where name=:1"
        cursor.executemany(sql_query, rows)
    connection.commit()
