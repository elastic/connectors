#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
import base64
import os
import random

import pytds
from faker import Faker

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

faked = Faker()

BATCH_SIZE = 1000
DATA_SIZE: str = os.environ.get("DATA_SIZE", "medium").lower()

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


def get_num_docs() -> None:
    print(NUM_TABLES * (RECORD_COUNT - RECORDS_TO_DELETE))


def inject_lines(table, cursor, lines) -> None:
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
            rows.append(
                (
                    fake_provider.fake.name(),  # name
                    row_id,  # age
                    fake_provider.get_text(),  # description
                    faked.date_time(),  # record_time
                    faked.pydecimal(left_digits=10, right_digits=2),  # balance
                    faked.random_letter(),  # initials
                    faked.boolean(),  # active
                    faked.random_int(),  # points
                    faked.pydecimal(left_digits=10, right_digits=2),  # salary
                    faked.pydecimal(
                        left_digits=8,
                        right_digits=4,
                        min_value=-214748,
                        max_value=214748,
                    ),  # bonus
                    faked.random_int(),  # score
                    faked.pydecimal(left_digits=2, right_digits=1),  # rating
                    faked.pydecimal(left_digits=2, right_digits=2),  # discount
                    faked.date(),  # birthdate
                    faked.date_time(),  # appointment
                    faked.date_time(),  # created_at
                    faked.date_time(),  # updated_at
                    faked.date_time(),  # last_login
                    faked.date_time(),  # expiration
                    faked.random_element(elements=("A", "I")),  # status
                    faked.text(max_nb_chars=100),  # notes
                    faked.text(max_nb_chars=100),  # additional_info
                    faked.uuid4(),  # unique_key
                    faked.json(),  # config
                    faked.random_int(min=1, max=10),  # small_age
                    base64.b64encode(
                        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x10\x00\x00\x00\x10\x08\x06\x00\x00\x00\x1f\xf3\xff\xa6\x00\x00\x00"
                    ).decode("utf-8"),  # profile_pic
                )
            )

        columns = [
            "name",
            "age",
            "description",
            "record_time",
            "balance",
            "initials",
            "active",
            "points",
            "salary",
            "bonus",
            "score",
            "rating",
            "discount",
            "birthdate",
            "appointment",
            "created_at",
            "updated_at",
            "last_login",
            "expiration",
            "status",
            "notes",
            "additional_info",
            "unique_key",
            "config",
            "small_age",
            "profile_pic",
        ]

        placeholders = ", ".join(["%s"] * len(columns))
        column_names = ", ".join(columns)

        sql_query = (
            f"INSERT INTO customers_{table} ({column_names}) VALUES ({placeholders})"
        )

        cursor.executemany(sql_query, rows)
        inserted += batch_size
        print(f"Inserted batch #{batch} of {batch_size} documents.")


async def load() -> None:
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
        sql_query = f"CREATE TABLE customers_{table} (id INT IDENTITY(1,1), name VARCHAR(255), age SMALLINT, description TEXT, record_time TIME, balance DECIMAL(18, 2), initials CHAR(3), active BIT, points BIGINT, salary MONEY, bonus SMALLMONEY, score NUMERIC(10, 2), rating FLOAT, discount REAL, birthdate DATE, appointment TIME, created_at DATETIME2, updated_at DATETIMEOFFSET, last_login DATETIME, expiration SMALLDATETIME, status CHAR(1), notes VARCHAR(1000), additional_info NTEXT, unique_key UNIQUEIDENTIFIER, config XML, small_age TINYINT, profile_pic nvarchar(max), PRIMARY KEY (id))"
        cursor.execute(sql_query)
        inject_lines(table, cursor, RECORD_COUNT)
    database.commit()


async def remove() -> None:
    """Removes 10 random items per table"""

    database = pytds.connect(server=HOST, port=PORT, user=USER, password=PASSWORD)
    cursor = database.cursor()
    cursor.execute(f"USE {DATABASE_NAME}")
    for table in range(NUM_TABLES):
        rows = [
            (row_id,)
            for row_id in random.sample(range(1, RECORD_COUNT), RECORDS_TO_DELETE)
        ]
        sql_query = f"DELETE from customers_{table} where id in (%s)"
        cursor.executemany(sql_query, rows)
    database.commit()
