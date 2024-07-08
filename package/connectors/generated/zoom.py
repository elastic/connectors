# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.zoom import ZoomDataSource
from package.connectors.connector_base import ConnectorBase


class ZoomConnector(ConnectorBase):
    """
    ZoomConnector class generated for connecting to the data source.

    Args:

        account_id (str): Account ID

        client_id (str): Client ID

        client_secret (str): Client secret

        fetch_past_meeting_details (bool): Fetch past meeting details
            - Enable this option to fetch past past meeting details. This setting can increase sync time.

        recording_age (int): Recording Age Limit (Months)
            - How far back in time to request recordings from zoom. Recordings older than this will not be indexed.

    """

    def __init__(
        self,
        account_id=None,
        client_id=None,
        client_secret=None,
        fetch_past_meeting_details=False,
        recording_age=None,
    ):

        configuration = ZoomDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(data_provider=ZoomDataSource(connector_configuration))

        self.account_id = account_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.fetch_past_meeting_details = fetch_past_meeting_details
        self.recording_age = recording_age
