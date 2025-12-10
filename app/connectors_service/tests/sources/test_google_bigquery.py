#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Google Bigquery source."""

import asyncio
from contextlib import asynccontextmanager
import json
import pytest
from unittest import mock
from unittest.mock import Mock, patch

from connectors.sources.google_bigquery import GoogleBigqueryDataSource
from connectors_sdk.source import ConfigurableFieldValueError, DataSourceConfiguration
from tests.sources.support import create_source

SERVICE_ACCOUNT_CREDENTIALS = '{"project_id": "dummy123"}'

# a very small default table to use, ~496 rows
test_bq_small = {
    "project_id": "bigquery-public-data",
    "dataset": "new_york_subway",
    "table": "stations"
}

# a >10k table to use, 19640 to be exact
test_bq_10k = {
    "project_id": "bigquery-public-data",
    "dataset": "new_york_subway",
    "table": "trips"
}

@asynccontextmanager
async def create_bigquery_source(
        service_account_credentials=SERVICE_ACCOUNT_CREDENTIALS,
        dataset=test_bq_small["dataset"],
        table=test_bq_small["table"]
):
    async with create_source(
            GoogleBigqueryDataSource,
            service_account_credentials=SERVICE_ACCOUNT_CREDENTIALS,
            dataset=dataset,
            table=table
    ) as source:
        yield source

@pytest.mark.asyncio
async def test_empty_configuration():
    """Tests GoogleBigqueryDataSource's validation of the provided configuration."""
    error_configs = [
        # service account credentials must be present and not blank
        DataSourceConfiguration({"service_account_credentials": ""})

    ]

    for error_config in error_configs:
        bq = GoogleBigqueryDataSource(configuration=error_config)
        with pytest.raises(ConfigurableFieldValueError):
            await bq.validate_config()

@pytest.mark.asyncio
async def test_build_query():
    """Tests query building for GoogleBigqueryDataSource."""
    testcreds = json.dumps({"project_id": "testprojectid"})

    # minimum defaults
    config = DataSourceConfiguration(
        {"service_account_credentials": testcreds,
         "dataset": "testdataset",
         "table": "testtable",
         })
    bq = GoogleBigqueryDataSource(configuration=config)
    query = bq.build_query()
    expected = """SELECT * FROM `testprojectid.testdataset.testtable`"""
    assert query == expected

    # custom project
    config = DataSourceConfiguration(
        {"service_account_credentials": testcreds,
         "dataset": "testdataset",
         "table": "testtable",
         "project_id": "different",
         })
    bq = GoogleBigqueryDataSource(configuration=config)
    query = bq.build_query()
    expected = """SELECT * FROM `different.testdataset.testtable`"""
    assert query == expected

    # custom columns
    config = DataSourceConfiguration(
        {"service_account_credentials": testcreds,
         "dataset": "testdataset",
         "table": "testtable",
         "columns": "account_id, timestamp"
         })
    bq = GoogleBigqueryDataSource(configuration=config)
    query = bq.build_query()
    expected = """SELECT account_id, timestamp FROM `testprojectid.testdataset.testtable`"""
    assert query == expected

    # custom predicates
    config = DataSourceConfiguration(
        {"service_account_credentials": testcreds,
         "dataset": "testdataset",
         "table": "testtable",
         "predicates": "WHERE foo=1"
         })
    bq = GoogleBigqueryDataSource(configuration=config)
    query = bq.build_query()
    expected = """SELECT * FROM `testprojectid.testdataset.testtable` WHERE foo=1"""
    assert query == expected
