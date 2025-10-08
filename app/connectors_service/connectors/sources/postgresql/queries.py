#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.sources.shared.database.generic_database import Queries


class PostgreSQLQueries(Queries):
    """Class contains methods which return query"""

    def ping(self):
        """Query to ping source"""
        return "SELECT 1+1"

    def all_tables(self, **kwargs):
        """Query to get all tables"""
        return f"SELECT table_name FROM information_schema.tables WHERE table_catalog = '{kwargs['database']}' and table_schema = '{kwargs['schema']}'"

    def table_primary_key(self, **kwargs):
        """Query to get the primary key"""
        return (
            f"SELECT a.attname AS c "
            f"FROM pg_index i "
            f"JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
            f"JOIN pg_class t ON t.oid = i.indrelid "
            f"JOIN pg_constraint c ON c.conindid = i.indexrelid "
            f"WHERE i.indrelid = '\"{kwargs['schema']}\".\"{kwargs['table']}\"'::regclass "
            f"AND t.relkind = 'r' "
            f"AND c.contype = 'p' "
            f"ORDER BY array_position(i.indkey, a.attnum)"
        )

    def table_data(self, **kwargs):
        """Query to get the table data"""
        return f'SELECT * FROM "{kwargs["schema"]}"."{kwargs["table"]}" ORDER BY {kwargs["columns"]} LIMIT {kwargs["limit"]} OFFSET {kwargs["offset"]}'

    def table_last_update_time(self, **kwargs):
        """Query to get the last update time of the table"""
        return f'SELECT MAX(pg_xact_commit_timestamp(xmin)) FROM "{kwargs["schema"]}"."{kwargs["table"]}"'

    def table_data_count(self, **kwargs):
        """Query to get the number of rows in the table"""
        return f'SELECT COUNT(*) FROM "{kwargs["schema"]}"."{kwargs["table"]}"'

    def all_schemas(self):
        """Query to get all schemas of database"""
        pass
