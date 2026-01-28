#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Google Bigquery module which fetches rows from a Bigquery table."""

from connectors_sdk.logger import logger
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
from google.oauth2 import service_account
import os

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

        if RUNNING_FTEST:
            self._logger.debug("GoogleBigqueryClient: ftest detected, using AnonymousCredentials.")
            credentials = AnonymousCredentials()
            # even AnonymousCredentials will pick up a project_id it finds in the user's
            # ADC config if they have one, so we MUST override that here too.
            project_id = "test"
        else:
            credentials = service_account.Credentials.from_service_account_info(
                self.json_credentials
            )

        # Extend here if configurable BQ client options are required in future,
        # potentially useful to implement private universes for example if needed.
        client_options = None

        # When running local ftest, connect to local container for api.
        if RUNNING_FTEST:
            client_options = ClientOptions(api_endpoint="http://0.0.0.0:9050")


        if project_id is not None:
            self._logger.debug(f"GoogleBigqueryClient setting project_id: {project_id}.")
            if client_options is not None:
                return bigquery.Client(credentials=credentials, project=project_id, client_options=client_options)
            else:
                return bigquery.Client(credentials=credentials, project=project_id)
        else:
            self._logger.debug(f"GoogleBigqueryClient setting default project_id.")
            if client_options is not None:
                return bigquery.Client(credentials=credentials, client_options=client_options)
            else:
                return bigquery.Client(credentials=credentials)
