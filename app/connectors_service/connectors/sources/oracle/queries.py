#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from connectors.sources.shared.database.generic_database import Queries


class OracleQueries(Queries):
    """Class contains methods which return query"""

    def ping(self):
        """Query to ping source"""
        return "SELECT 1+1 FROM DUAL"

    def all_tables(self, **kwargs):
        """Query to get all tables"""
        return (
            f"SELECT TABLE_NAME FROM all_tables where OWNER = UPPER('{kwargs['user']}')"
        )

    def table_primary_key(self, **kwargs):
        """Query to get the primary key"""
        return f"SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '{kwargs['table']}' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = UPPER('{kwargs['user']}') AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position"

    def table_data(self, **kwargs):
        """Query to get the table data"""
        return f"SELECT * FROM {kwargs['table']}"

    def table_last_update_time(self, **kwargs):
        """Query to get the last update time of the table"""
        return f"SELECT SCN_TO_TIMESTAMP(MAX(ora_rowscn)) from {kwargs['table']}"

    def table_data_count(self, **kwargs):
        """Query to get the number of rows in the table"""
        return f"SELECT COUNT(*) FROM {kwargs['table']}"

    def all_schemas(self):
        """Query to get all schemas of database"""
        pass  # Multiple schemas not supported in Oracle
