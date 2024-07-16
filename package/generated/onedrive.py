# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from elastic_connectors.connectors.source import DataSourceConfiguration
from elastic_connectors.connectors.sources.onedrive import OneDriveDataSource
from elastic_connectors.connector_base import ConnectorBase


class OneDriveConnector(ConnectorBase):
    """
    OneDriveConnector class generated for connecting to the data source.

    Args:

        client_id (str): Azure application Client ID

        client_secret (str): Azure application Client Secret

        tenant_id (str): Azure application Tenant ID

        retry_count (int): Maximum retries per request

        concurrent_downloads (int): Maximum concurrent downloads

    """

    def __init__(
        self,
        client_id=None,
        client_secret=None,
        tenant_id=None,
        retry_count=3,
        concurrent_downloads=15,
        **kwargs
    ):

        configuration = OneDriveDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=OneDriveDataSource(connector_configuration), **kwargs
        )

        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.retry_count = retry_count
        self.concurrent_downloads = concurrent_downloads
