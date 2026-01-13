#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Google Bigquery module which fetches rows from a Bigquery table."""

from aiogoogle.auth.creds import ServiceAccountCreds
from connectors_sdk.logger import logger
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import tempfile

RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.

class GoogleBigqueryClient:
    """A client to make api calls to Google Bigquery."""

    def __init__(self, json_credentials):
        """Initialize the ServiceAccountCreds instance.

        Args:
            json_credentials(dict): Service account credentials json."""
        self.json_credentials = json_credentials
        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

    def client(self, project_id=None):
        """Returns an instance of a bigquery client, using the configured credentials,
        optionally to a different project_id (configured creds must have permissions to
        access and create query jobs in it).

        Args:
            project_id (string, optional): If connecting to a project other than the
            service account credentials default, pass this as a string.

        Returns:
            bigquery.Client instance

        """
        credentials = service_account.Credentials.from_service_account_info(self.json_credentials)
        if project_id is not None:
            return bigquery.Client(credentials=credentials, project=project_id)
        else:
            return bigquery.Client(credentials=credentials)
