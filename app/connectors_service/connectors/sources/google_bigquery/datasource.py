#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio
from concurrent.futures import ThreadPoolExecutor
from connectors_sdk.source import (
    BaseDataSource,
    DataSourceConfiguration
)
from connectors.sources.google_bigquery.client import GoogleBigqueryClient
from connectors.sources.shared.database.generic_database import (
    DEFAULT_FETCH_SIZE,
    DEFAULT_RETRY_COUNT
)
from connectors.sources.shared.google import (
    load_service_account_json,
    validate_service_account_json,
)
from connectors.utils import get_pem_format

from functools import cached_property, partial
import os

RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.

executor = ThreadPoolExecutor(1)

class GoogleBigqueryDataSource(BaseDataSource):
    """Google Bigquery"""

    name = "Google Bigquery"
    service_type = "google_bigquery"
    incremental_sync_enabled = False
    advanced_rules_enabled = True
    dls_enabled = False


    def __init__(self, configuration: DataSourceConfiguration):
        """Set up the connection to Google Bigquery.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration.
        """
        super().__init__(configuration=configuration)

    def _set_internal_logger(self):
        self._google_bigquery_client.set_logger(self._logger)

    @cached_property
    def _google_bigquery_client(self) -> GoogleBigqueryClient:
        """Initialize and return an instance of GoogleBigqueryClient

        Returns:
            GoogleBigqueryClient: An instance of GoogleBigqueryClient.
        """
        REQUIRED_CREDENTIAL_KEYS = [
            "type",
            "project_id",
            "private_key_id",
            "private_key",
            "client_email",
            "client_id",
            "auth_uri",
            "token_uri",
        ]

        json_credentials = load_service_account_json(
            self.configuration["service_account_credentials"], "Google Bigquery"
        )

        if (
                json_credentials.get("private_key")
                and "\n" not in json_credentials["private_key"]
        ):
            json_credentials["private_key"] = get_pem_format(
                key=json_credentials["private_key"].strip(),
                postfix="-----END PRIVATE KEY-----",
            )

        required_credentials = {
            key: value
            for key, value in json_credentials.items()
            if key in REQUIRED_CREDENTIAL_KEYS
        }

        return GoogleBigqueryClient(json_credentials=required_credentials)


    @classmethod
    def get_default_configuration(cls) -> dict:
        """Get the default configuration for Google Bigquery.

        Returns:
            dictionary: Default configuration.
        """
        return {
            "service_account_credentials": {
                "display": "textarea",
                "label": "Google Cloud service account JSON",
                "sensitive": True,
                "order": 1,
                "type": "str"
            },
            "dataset": {
                "display": "text",
                "label": "Dataset the table is in.",
                "order": 2,
                "type": "str",
            },
            "table": {
                "display": "text",
                "label": "Table to sync.",
                "order": 3,
                "type": "str",
            },
            "project_id": {
                "display": "text",
                "label": "Google Cloud project.",
                "order": 4,
                "type": "str",
                "required": False,
                "default_value": "",
                "tooltip": "Defaults to the service account project.",
            },
            "index_settings": {
                "display": "textarea",
                "label": "Additional index settings, if any. For example, to set mode to lookup.",
                "order": 5,
                "required": False,
                "default_value": "",
                "type": "str",
                "ui_restrictions": ["advanced"],
            },
            "columns": {
                "display": "textarea",
                "label": "Columns to fetch. Defaults to * if none are set.",
                "order": 6,
                "required": False,
                "default_value": "*",
                "type": "str",
                "ui_restrictions": ["advanced"],
                "tooltip": "Comma-separated, as in a SQL SELECT."
            },
            "predicates": {
                "display": "textarea",
                "label": "Predicates for the query.",
                "order": 7,
                "required": False,
                "default_value": "",
                "type": "str",
                "ui_restrictions": ["advanced"],
                "tooltip": "A SQL WHERE clause. May be required for some partitioned table configurations."
            },
            "fetch_size": {
                "default_value": DEFAULT_FETCH_SIZE,
                "display": "numeric",
                "label": "Rows fetched per request",
                "order": 8,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "retry_count": {
                "default_value": DEFAULT_RETRY_COUNT,
                "display": "numeric",
                "label": "Retries per request",
                "order": 9,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
        }

    async def validate_config(self):
        """Validates whether user inputs are valid for configuration.

        Raises:
            Exception: The format of service account json is invalid.
        """
        await super().validate_config()
        validate_service_account_json(
            self.configuration["service_account_credentials"],
            "Google Bigquery"
        )

        # if they set a project_id and have a wrong service account, a log message
        # will give them a fighting chance :)
        if self.configuration["project_id"] and self.configuration["service_account_credentials"]["project_id"] != self.configuration["project_id"]:
            self._logger.info("A project_id is configured and does not match the project_id for the service_account_credentials block. If authorization fails, this could be why!")
        self.project_id = self.resolve_project()

    async def ping(self):
        """Verify the connection with Google Bigquery"""
        if RUNNING_FTEST:
            return
        try:
            await anext(
                self._google_bigquery_client.api_call(
                    resource="projects",
                    method="serviceAccount",
                    sub_method="get",
                    projectId=self._google_bigquery_client.user_project_id))
        except Exception:
            self._logger.exception("Error while connecting to Google Bigquery.")
            raise


    def resolve_project(self):
        """
        Resolves a project_id, as a string. This should be a Google Cloud project.
        Chooses configured project_id first, or if that is unset, falls back to the
        project_id on the service account.

        Returns:
            string: The project name.

        Raises:
            ConfigurableFieldValueError: service account JSON was invalid/unparseable.
        """

        if self.configuration["project_id"]:
            return self.configuration["project_id"]
        json_credentials = load_service_account_json(
            self.configuration["service_account_credentials"], "Google Cloud Storage"
        )
        return json_credentials["project_id"]


    def resolve_table(self):
        """Inspects the configuration and produces a fully qualified BigQuery table
        identifier.

        Returns:
            string: A BQ table in the form `project.dataset.table`
        """

        return "`%s.%s.%s`" % (self.resolve_project(), self.configuration["dataset"], self.configuration["table"])


    def build_query(self):
        """
        Builds the query that will be run on BigQuery.

        Returns:
            string: The query.
        """
        # create a sub-config because full config contains secrets
        conf = {k: self.configuration[k] for k in ("predicates", "columns")}
        conf["resolved_table"] = self.resolve_table()
        if not conf["columns"]:
            conf["columns"] = "*"

        query = """SELECT %(columns)s FROM %(resolved_table)s""" % conf

        if conf["predicates"]:
            query = query + " " + conf["predicates"]
        return query.strip()


    async def get_docs(self, filtering=None):
        """Returns results as (rowdict,None) on the configured query results. Realizes
        results in fetch_size chunks.

        Args:
            filtering: Unused; part of the BaseDataSource contract.

        Yields:
            dictionary: (rowdict, None) pairs, per the BaseDataSource contract.

        """
        self._logger.info("Connected to Google Bigquery.")
        sql = self.build_query()

        # job is a QueryJob instance
        # https://docs.cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.QueryJob
        job = self._google_bigquery_client.client().query(sql)

        # do not block on the bigquery job completing
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(executor, job.done)

        fetch_size = self.configuration.get("fetch_size") or DEFAULT_FETCH_SIZE

        def get_next_chunk(job_iter):
            try:
                chunk = []
                for _ in range(fetch_size):
                    try:
                        row = next(job_iter)
                        chunk.append(dict(row)) # Row -> dict
                    except StopIteration:
                        break
                return chunk
            except Exception as e:
                # TODO: retry_count support here?
                self._logger.error(f"Error fetching chunk from BigQuery job: {e}")
                raise e

        # we don't use .result() as it is synchronous
        # as of this writing, awaitable jobs are not yet implemented
        # which is why this asyncio machinery is necessary presently
        # but in a future version this should all become awaitable
        # https://github.com/googleapis/python-bigquery/issues/18
        job_iter = iter(job)

        while True:
            chunk = await loop.run_in_executor(executor, get_next_chunk, job_iter)
            if not chunk:
                break
            for row_dict in chunk:
                yield row_dict, None
