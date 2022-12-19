#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from mysql.connector import connect
import random
import os


DATABASE_NAME = "customerinfo"
NUM_TABLES = int(os.environ.get('NUM_TABLES', '30'))


def main():
    """Removes 10 random items per table"""
    database = connect(host="127.0.0.1", port=3306, user="root", password="changeme")
    cursor = database.cursor()
    cursor.execute(f"USE {DATABASE_NAME}")
    for table in range(NUM_TABLES):
        print(f"Working on table {table}...")
        rows = [(f"user_{row_id}",) for row_id in random.sample(range(1, 1000), 10)]
        print(rows)
        sql_query = f"DELETE from customers_{table} where name=%s"
        cursor.executemany(sql_query, rows)
    database.commit()


if __name__ == "__main__":
    main()
