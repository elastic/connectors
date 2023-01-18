#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import random
import string

import psycopg2

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
DATABASE_NAME = "xe"
_SIZES = {"small": 5, "medium": 10, "large": 30}
NUM_TABLES = _SIZES[DATA_SIZE]


def random_text(k=1024 * 20):
    """Function to generate random text

    Args:
        k (int, optional): size of data in bytes. Defaults to 1024*20.

    Returns:
        string: random text
    """
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


BIG_TEXT = random_text()


def inject_lines(table, cursor, start, lines):
    """Ingest rows in table

    Args:
        table (str): Name of table
        cursor (cursor): Cursor to execute query
        start (int): Starting row
        lines (int): Number of rows
    """
    raws = []
    for raw_id in range(lines):
        raw_id += start
        raws.append((f"user_{raw_id}", raw_id, BIG_TEXT))
    sql_query = (
        f"INSERT INTO customers_{table}"
        + "(name, age, description) VALUES (%s, %s, %s)"
    )
    cursor.executemany(sql_query, raws)


def load():
    """N tables of 10001 rows each. each row is ~ 1024*20 bytes"""
    database = psycopg2.connect(
        database=DATABASE_NAME,
        user="admin",
        password="Password_123",
        host="127.0.0.1",
        port="9090",
    )
    cursor = database.cursor()
    for table in range(NUM_TABLES):
        print(f"Adding data in {table}...")
        sql_query = f"CREATE TABLE IF NOT EXISTS customers_{table} (name VARCHAR(255), age int, description TEXT, PRIMARY KEY (name))"
        cursor.execute(sql_query)
        for i in range(10):
            inject_lines(table, cursor, i * 1000, 1000)
    database.commit()


def remove():
    """Removes 10 random items per table"""
    database = psycopg2.connect(
        database=DATABASE_NAME,
        user="admin",
        password="Password_123",
        host="127.0.0.1",
        port="9090",
    )
    cursor = database.cursor()
    for table in range(NUM_TABLES):
        rows = [(f"user_{row_id}",) for row_id in random.sample(range(1, 1000), 10)]
        sql_query = f"DELETE from customers_{table} where name=%s"
        cursor.executemany(sql_query, rows)
    database.commit()
