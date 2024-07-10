# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.box import BoxDataSource
from package.connectors.connector_base import ConnectorBase


class BoxConnector(ConnectorBase):
    """
    BoxConnector class generated for connecting to the data source.

    Args:

        is_enterprise (str): Box Account

        client_id (str): Client ID

        client_secret (str): Client Secret

        refresh_token (str): Refresh Token

        enterprise_id (int): Enterprise ID

        concurrent_downloads (int): Maximum concurrent downloads

    """

    def __init__(
        self,
        is_enterprise="box_free",
        client_id=None,
        client_secret=None,
        refresh_token=None,
        enterprise_id=None,
        concurrent_downloads=15,
        **kwargs
    ):

        configuration = BoxDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(data_provider=BoxDataSource(connector_configuration), **kwargs)

        self.is_enterprise = is_enterprise
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.enterprise_id = enterprise_id
        self.concurrent_downloads = concurrent_downloads
