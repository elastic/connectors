# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.confluence import ConfluenceDataSource
from package.connectors.connector_base import ConnectorBase


class ConfluenceConnector(ConnectorBase):
    """
    ConfluenceConnector class generated for connecting to the data source.

    Args:

        data_source (str): Confluence data source

        username (str): Confluence Server username

        password (str): Confluence Server password

        data_center_username (str): Confluence Data Center username

        data_center_password (str): Confluence Data Center password

        account_email (str): Confluence Cloud account email

        api_token (str): Confluence Cloud API token

        confluence_url (str): Confluence URL

        spaces (list): Confluence space keys
            - This configurable field is ignored when Advanced Sync Rules are used.

        index_labels (bool): Enable indexing labels
            - Enabling this will increase the amount of network calls to the source, and may decrease performance

        ssl_enabled (bool): Enable SSL

        ssl_ca (str): SSL certificate

        retry_count (int): Retries per request

        concurrent_downloads (int): Maximum concurrent downloads

    """

    def __init__(
        self,
        data_source="confluence_server",
        username=None,
        password=None,
        data_center_username=None,
        data_center_password=None,
        account_email=None,
        api_token=None,
        confluence_url=None,
        spaces=None,
        index_labels=False,
        ssl_enabled=False,
        ssl_ca=None,
        retry_count=3,
        concurrent_downloads=50,
    ):

        configuration = ConfluenceDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(data_provider=ConfluenceDataSource(connector_configuration))

        self.data_source = data_source
        self.username = username
        self.password = password
        self.data_center_username = data_center_username
        self.data_center_password = data_center_password
        self.account_email = account_email
        self.api_token = api_token
        self.confluence_url = confluence_url
        self.spaces = spaces
        self.index_labels = index_labels
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.retry_count = retry_count
        self.concurrent_downloads = concurrent_downloads
