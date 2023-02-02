#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import random
import string

from faker import Faker
from mysql.connector import connect

from connectors.logger import logger

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()

DATABASE_NAME = "customerinfo"
TABLE_PREFIX = "customer_"

DROP_DATABASE = f"DROP DATABASE IF EXISTS {DATABASE_NAME}"
CREATE_DATABASE = f"CREATE DATABASE {DATABASE_NAME}"
USE_DATABASE = f"USE {DATABASE_NAME}"

ROWS_REMOVAL_COUNT = 10
NUM_ROW_INSERTION_BATCHES = 10
NUM_ROWS_PER_BATCH = 1000

_SIZES = {"small": 5, "medium": 10, "large": 30}
NUM_TABLES = _SIZES[DATA_SIZE]

fake = Faker()


def random_text(k=1024 * 20):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


BIG_TEXT = random_text()


class Customer:
    def __init__(self, id_):
        self.id_ = id_
        self.name = fake.name()
        self.description = BIG_TEXT

    def to_row(self):
        return self.id_, self.name, self.description


def get_table_name(table_num):
    return TABLE_PREFIX + str(table_num)


def database_connection():
    return connect(host="127.0.0.1", port=3306, user="root", password="changeme")


def add_customers(table, cursor, offset, num_customers):
    rows = []

    for customer_id in range(num_customers):
        customer_id += offset
        rows.append(Customer(customer_id).to_row())

    insert_customer_query = (
        f"INSERT INTO {table}" + "(id, name, description) VALUES (%s, %s, %s)"
    )

    cursor.executemany(insert_customer_query, rows)


def load():
    """N tables of 10001 rows each. each row is ~ 1024*20 bytes"""
    database = database_connection()
    db_cursor = database.cursor()

    db_cursor.execute(DROP_DATABASE)
    logger.info(f"Dropped database '{DATABASE_NAME}' if existed")

    db_cursor.execute(CREATE_DATABASE)
    logger.info(f"Created database '{DATABASE_NAME}'")

    db_cursor.execute(USE_DATABASE)
    logger.info(f"Using database '{DATABASE_NAME}' now")

    for table_num in range(NUM_TABLES):
        table_name = get_table_name(table_num)

        logger.info(
            f"Adding {NUM_ROW_INSERTION_BATCHES * NUM_ROWS_PER_BATCH} rows to table '{table_name}'..."
        )

        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id int NOT NULL, name VARCHAR(255),  description LONGTEXT, PRIMARY KEY (id))"
        db_cursor.execute(create_table_query)

        # insert 10001 rows in 10 batches
        for batch in range(NUM_ROW_INSERTION_BATCHES):
            id_offset = batch * NUM_ROWS_PER_BATCH
            add_customers(table_name, db_cursor, id_offset, NUM_ROWS_PER_BATCH)

    database.commit()


def remove():
    """Removes 10 random items per table"""
    database = database_connection()
    cursor = database.cursor()
    cursor.execute(USE_DATABASE)

    for table_num in range(NUM_TABLES):
        table_name = get_table_name(table_num)
        logger.info(
            f"Removing {ROWS_REMOVAL_COUNT} random rows from table '{table_name}'..."
        )

        ten_random_rows = [
            (row_id,) for row_id in random.sample(range(1, 1000), ROWS_REMOVAL_COUNT)
        ]
        delete_query = f"DELETE from {table_name} where id=%s"
        cursor.executemany(delete_query, ten_random_rows)

        logger.info(f"Removed {ROWS_REMOVAL_COUNT} rows: {ten_random_rows}")

    database.commit()
