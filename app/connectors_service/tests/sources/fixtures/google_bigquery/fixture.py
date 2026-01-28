#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201

"""Google Bigquery module that generates fixture data on the local mocked Bigquery container."""

import os
from itertools import islice

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

client_options = ClientOptions(api_endpoint="http://0.0.0.0:9050")

PROJECT = "test"
DATASET = "testdata"
TABLE = "testtable"

DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

match DATA_SIZE:
    case "small":
        RECORD_COUNT = 500
    case "medium":
        RECORD_COUNT = 10000
    case "large":
        RECORD_COUNT = 25000


def partition_all(iterable, chunk_size):
    iterator = iter(iterable)
    while chunk := tuple(islice(iterator, chunk_size)):
        yield chunk


def insert_rows(client, table):
    print(f"Loading BQ fixture data into {table}", end="")
    rows = []
    for n in range(RECORD_COUNT):
        rows.append({"testcolumn": f"row {n}"})
    for chunk in partition_all(rows, 50):
        print(".", end="")
        client.insert_rows_json(table, chunk)
    print("done.")


async def load():
    """Loads DATA_SIZE records into the Bigquery fixture container."""
    client = bigquery.Client(
        PROJECT,
        client_options=client_options,
        credentials=AnonymousCredentials(),
    )

    schema = [bigquery.SchemaField("testcolumn", "STRING", mode="NULLABLE")]

    table_ref = client.dataset(DATASET).table(TABLE)
    table = bigquery.Table(table_ref, schema=schema)
    try:
        table = client.create_table(table)
    except Exception as e:
        print(f"Error creating fixture table {table.full_table_id}: {e}")

    # fetch full created table instance back from BQ using the table_ref
    table = client.get_table(table_ref)

    errors = insert_rows(client, table)
    if errors is not None and errors != []:
        print(f"Errors inserting fixture data: {errors}")
