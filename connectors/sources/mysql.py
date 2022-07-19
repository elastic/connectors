#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime
import asyncio
from sqlite3 import Cursor
from bson import Decimal128
from mysql.connector import connection

from connectors.source import BaseDataSource
from connectors.logger import logger


class MySqlDataSource(BaseDataSource):
    """MYSQL"""

    def __init__(self, connector):
        super().__init__(connector)
        try:
            self.connection = connection.MySQLConnection(
                user=self.configuration["user"],
                password=self.configuration["password"],
                port=self.configuration["port"],
                host=self.configuration["host"],
                database=self.configuration["database"],
        )
        except Exception as exception:
            raise
        self._first_sync = self._dirty = True

    @classmethod
    def get_default_configuration(cls):
        return {
            "host": {
                "value": "127.0.0.1",
                "label": "MySQL Host",
                "type": "str",
            },
            "port": {
                "value": "3306",
                "label": "MySQL Port",
                "type": "int",
            },
            "user": {
                "value": "user",
                "label": "MySQL Username",
                "type": "str",
            },
            "password": {
                "value": "password",
                "label": "MySQL Password",
                "type": "str",
            },
            "database": {
                "value": "sql_db",
                "label": "MySQL Database",
                "type": "str",
            },
        }

    async def ping(self):
        self.connection.ping(reconnect=False, attempts=3, delay=0)
    
    def _execute_query(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        response = cursor.fetchall()
        if not response:
            raise Exception
        return cursor, response

    def get_tables(self, database):
        table_list = []
        query = f"""SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema="{database}" """
        _, tables = self._execute_query(query=query)
        for table in tables:
            table_list.append(table[0])
        return table_list

    def transform(self, keys, values):
        json_doc = {}
        for index in range(len(keys)):
            json_doc[keys[index]] = values[index]
        return json_doc

    async def changed(self):
        return self._dirty

    async def get_rows(self, table):
        query = f"""SELECT * FROM {table}"""
        cursor, rows = self._execute_query(query=query)
        columns = cursor.column_names
        metadata = {"database":self.configuration["database"], "table":table}
        documents = []
        for row in rows:
            document = self.transform(columns, row)
            document["_id"] = hash(frozenset(row))
            document.update(metadata)
            documents.append(document)
        return documents

    async def get_docs(self):
        self.connection.connect()
        async_tasks = []
        for table in self.get_tables(database=self.configuration["database"]):
            task = asyncio.get_event_loop().create_task(self.get_rows(table=table))
            async_tasks.append(task)
        
        for task in async_tasks:
            await task
            for result in task.result():
                yield result
        
        self._dirty = False
