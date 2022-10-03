#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from mysql.connector import connect
import random
import string


DATABASE_NAME = "customerinfo"


def random_text():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=4096))


def main():
    """Method for generating 15k document for mysql"""
    database = connect(host="127.0.0.1", port=3306, user="root", password="changeme")
    cursor = database.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE_NAME}")
    cursor.execute(f"CREATE DATABASE {DATABASE_NAME}")
    cursor.execute(f"USE {DATABASE_NAME}")
    for table in range(15):
        print(f"Adding data in {table}...")
        sql_query = f"CREATE TABLE IF NOT EXISTS customers_{table} (name VARCHAR(255), age int, description TEXT, PRIMARY KEY (name))"
        cursor.execute(sql_query)
        raws = []
        for raw_id in range(10000):
            raws.append((f"user_{raw_id}", raw_id, random_text()))
        sql_query = f"INSERT INTO customers_{table}" + "(name, age, description) VALUES (%s, %s, %s)"
        cursor.executemany(sql_query, raws)
    database.commit()


if __name__ == "__main__":
    main()
