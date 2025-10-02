#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import re
from connectors_sdk.utils import iso_utc

# SPLIT_BY_COMMA_OUTSIDE_BACKTICKS_PATTERN = re.compile(r"`(?:[^`]|``)+`|\w+")

MAX_POOL_SIZE = 10
DEFAULT_FETCH_SIZE = 5000
RETRIES = 3
RETRY_INTERVAL = 2

def generate_id(tables, row, primary_key_columns):
    """Generates an id using table names as prefix in sorted order and primary key values.

    Example:
        tables: table1, table2
        primary key values: 1, 42
        table1_table2_1_42
    """

    if not isinstance(tables, list):
        tables = [tables]

    return (
        f"{'_'.join(sorted(tables))}_"
        f"{'_'.join([str(pk_value) for pk in primary_key_columns if (pk_value := row.get(pk)) is not None])}"
    )

def row2doc(row, column_names, primary_key_columns, table, timestamp):
    row = dict(zip(column_names, row, strict=True))
    row.update(
        {
            "_id": generate_id(table, row, primary_key_columns),
            "_timestamp": timestamp or iso_utc(),
            "Table": table,
        }
    )

    return row

def format_list(list_):
    return ", ".join(list_)
