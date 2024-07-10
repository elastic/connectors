# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.google_cloud_storage import GoogleCloudStorageDataSource
from package.connectors.connector_base import ConnectorBase


class GoogleCloudStorageConnector(ConnectorBase):
    """
    GoogleCloudStorageConnector class generated for connecting to the data source.

    Args:

        buckets (list): Google Cloud Storage buckets

        service_account_credentials (str): Google Cloud service account JSON

    """

    def __init__(self, buckets=None, service_account_credentials=None, **kwargs):

        configuration = GoogleCloudStorageDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=GoogleCloudStorageDataSource(connector_configuration),
            **kwargs
        )

        self.buckets = buckets
        self.service_account_credentials = service_account_credentials
