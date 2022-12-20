#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import random
import string

from mysql.connector import connect

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
DATABASE_NAME = "customerinfo"
_SIZES = {"small": 5, "medium": 10, "large": 30}
NUM_TABLES = _SIZES[DATA_SIZE]


def random_text(k=1024 * 20):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


BIG_TEXT = random_text()


def main():
    """30 tables of 10001 rows each. each row is ~ 1024*20 bytes"""
    database = connect(host="127.0.0.1", port=3306, user="root", password="changeme")
    cursor = database.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE_NAME}")
    cursor.execute(f"CREATE DATABASE {DATABASE_NAME}")
    cursor.execute(f"USE {DATABASE_NAME}")
    for table in range(NUM_TABLES):
        print(f"Adding data in {table}...")
        sql_query = f"CREATE TABLE IF NOT EXISTS customers_{table} (name VARCHAR(255), age int, description LONGTEXT, PRIMARY KEY (name))"
        cursor.execute(sql_query)
        raws = []
        for raw_id in range(10000):
            raws.append((f"user_{raw_id}", raw_id, BIG_TEXT))
        sql_query = (
            f"INSERT INTO customers_{table}"
            + "(name, age, description) VALUES (%s, %s, %s)"
        )
        cursor.executemany(sql_query, raws)
    database.commit()


if __name__ == "__main__":
    main()
