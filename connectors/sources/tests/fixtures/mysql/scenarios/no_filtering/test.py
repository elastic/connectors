import asyncio
import os
import random
import string
from enum import Enum

from faker import Faker
from mysql.connector import connect

from connectors.logger import logger
from connectors.sources.tests.support import ESIndexAssertionHelper

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()

DATABASE_NAME = "customerinfo"
TABLE_PREFIX = "customer_"

DROP_DATABASE = f"DROP DATABASE IF EXISTS {DATABASE_NAME}"
CREATE_DATABASE = f"CREATE DATABASE {DATABASE_NAME}"
USE_DATABASE = f"USE {DATABASE_NAME}"

ROWS_REMOVAL_COUNT = 10
NUM_ROW_INSERTION_BATCHES = 1
NUM_ROWS_PER_BATCH = 100

_SIZES = {"small": 5, "medium": 10, "large": 30}
NUM_TABLES = _SIZES[DATA_SIZE]


class TestState(Enum):
    SUCCESSFUL = 1
    FAILED = 1


def get_table_name(table_num):
    return TABLE_PREFIX + str(table_num)


def random_text(k=1024 * 20):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


BIG_TEXT = random_text()


class FunctionalTest:
    def __init__(self, index_name, size):
        self.state = None
        self.errors = []
        self.size = size
        self.es_index = ESIndexAssertionHelper(index_name)

    async def load(self):
        raise NotImplementedError

    async def verify_load(self):
        raise NotImplementedError

    async def remove(self):
        raise NotImplementedError

    async def verify_remove(self):
        raise NotImplementedError

    async def update(self):
        raise NotImplementedError

    async def verify_update(self):
        raise NotImplementedError

    async def close(self):
        logger.info("Stopping test case...")
        await self.es_index.close()
        logger.info("Test case stopped successfully.")

    async def run_verifications_concurrently(self, verifications):
        try:
            results = await asyncio.gather(*verifications)
            successful, errors = map(list, zip(*results))

            self.state = (
                TestState.SUCCESSFUL
                if all(successful) and self.state == TestState.SUCCESSFUL and len(errors) == 0
                else TestState.FAILED
            )
            self.errors.append(*errors)

            logger.info(f"Test state: {self.state}")
            logger.info(f"Test errors: {self.errors}")
        except Exception as e:
            logger.error("Error appeared", e)
            self.state = TestState.FAILED
            self.errors.append(e)

            raise e


class Test(FunctionalTest):
    def __init__(self, index_name, size):
        super().__init__(index_name, size)

        self.database = connect(
            host="127.0.0.1", port=3306, user="root", password="changeme"
        )
        self.db_cursor = self.database.cursor()

        logger.info("Instantiated test")

    def add_customers(self, table, offset, num_customers):
        faker = Faker()

        def customer_row(id_):
            return id_, faker.name(), BIG_TEXT

        rows = []

        for customer_id in range(num_customers):
            customer_id += offset
            rows.append(customer_row(customer_id))

        insert_customer_query = (
            f"INSERT INTO {table}" + "(id, name, description) VALUES (%s, %s, %s)"
        )

        self.db_cursor.executemany(insert_customer_query, rows)

    async def load(self):
        """N tables of 10001 rows each. each row is ~ 1024*20 bytes"""
        self.db_cursor.execute(DROP_DATABASE)
        logger.info(f"Dropped database '{DATABASE_NAME}' if existed")

        self.db_cursor.execute(CREATE_DATABASE)
        logger.info(f"Created database '{DATABASE_NAME}'")

        self.db_cursor.execute(USE_DATABASE)
        logger.info(f"Using database '{DATABASE_NAME}' now")

        for table_num in range(NUM_TABLES):
            table_name = get_table_name(table_num)

            logger.info(
                f"Adding {NUM_ROW_INSERTION_BATCHES * NUM_ROWS_PER_BATCH} rows to table '{table_name}'..."
            )

            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id int NOT NULL, name VARCHAR(255),  description LONGTEXT, PRIMARY KEY (id))"
            self.db_cursor.execute(create_table_query)

            # insert 10001 rows in 10 batches
            for batch in range(NUM_ROW_INSERTION_BATCHES):
                id_offset = batch * NUM_ROWS_PER_BATCH
                self.add_customers(table_name, id_offset, NUM_ROWS_PER_BATCH)

        self.database.commit()

    async def remove(self):
        """Removes 10 random items per table"""
        self.db_cursor.execute(USE_DATABASE)

        for table_num in range(NUM_TABLES):
            table_name = get_table_name(table_num)
            logger.info(
                f"Removing {ROWS_REMOVAL_COUNT} random rows from table '{table_name}'..."
            )

            ten_random_rows = [
                (row_id,)
                for row_id in random.sample(range(1, 1000), ROWS_REMOVAL_COUNT)
            ]
            delete_query = f"DELETE from {table_name} where id=%s"
            self.db_cursor.executemany(delete_query, ten_random_rows)

            logger.info(f"Removed {ROWS_REMOVAL_COUNT} rows: {ten_random_rows}")

        self.database.commit()

    async def verify_load(self):
        await super().run_verifications_concurrently(
            [
                self.es_index.assert_has_doc_count(self.size),
                self.es_index.assert_pipeline_ran(),
                self.es_index.assert_content_extraction_successful(),
                self.es_index.assert_first_doc_is_correct(),
            ]
        )

    async def verify_remove(self):
        await super().run_verifications_concurrently(
            [
                self.es_index.assert_has_doc_count(
                    self.size - (NUM_TABLES * ROWS_REMOVAL_COUNT)
                )
            ]
        )

    async def update(self):
        pass

    async def verify_update(self):
        pass
