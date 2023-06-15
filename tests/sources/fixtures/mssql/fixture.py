#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import random
import string

import pytds

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
DATABASE_NAME = "xe"
_SIZES = {"small": 5, "medium": 10, "large": 30}
NUM_TABLES = _SIZES[DATA_SIZE]
HOST = "127.0.0.1"
PORT = 9090
USER = "admin"
PASSWORD = "Password_123"


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
    rows = []
    for row_id in range(lines):
        row_id += start
        rows.append((f"user_{row_id}", row_id, BIG_TEXT))
    sql_query = (
        f"INSERT INTO customers_{table} (name, age, description) VALUES (%s, %s, %s)"
    )
    cursor.executemany(sql_query, rows)


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
        print(f"Adding data from table #{table}...")
        sql_query = f"CREATE TABLE customers_{table} (name VARCHAR(255), age int, description TEXT, PRIMARY KEY (name))"
        cursor.execute(sql_query)
        for i in range(10):
            inject_lines(table, cursor, i * 1000, 1000)
    database.commit()


def remove():
    """Removes 10 random items per table"""

    database = pytds.connect(server=HOST, port=PORT, user=USER, password=PASSWORD)
    cursor = database.cursor()
    cursor.execute(f"USE {DATABASE_NAME}")
    for table in range(NUM_TABLES):
        rows = [(f"user_{row_id}",) for row_id in random.sample(range(1, 1000), 10)]
        sql_query = f"DELETE from customers_{table} where name=%s"
        cursor.executemany(sql_query, rows)
    database.commit()
