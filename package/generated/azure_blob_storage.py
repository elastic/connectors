# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from elastic_connectors.connectors.source import DataSourceConfiguration
from elastic_connectors.connectors.sources.azure_blob_storage import (
    AzureBlobStorageDataSource,
)
from elastic_connectors.connector_base import ConnectorBase


class AzureBlobStorageConnector(ConnectorBase):
    """
    AzureBlobStorageConnector class generated for connecting to the data source.

    Args:

        account_name (str): Azure Blob Storage account name

        account_key (str): Azure Blob Storage account key

        blob_endpoint (str): Azure Blob Storage blob endpoint

        containers (list): Azure Blob Storage containers

        retry_count (int): Retries per request

        concurrent_downloads (int): Maximum concurrent downloads

    """

    def __init__(
        self,
        account_name=None,
        account_key=None,
        blob_endpoint=None,
        containers=None,
        retry_count=3,
        concurrent_downloads=100,
        **kwargs
    ):

        configuration = AzureBlobStorageDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=AzureBlobStorageDataSource(connector_configuration), **kwargs
        )

        self.account_name = account_name
        self.account_key = account_key
        self.blob_endpoint = blob_endpoint
        self.containers = containers
        self.retry_count = retry_count
        self.concurrent_downloads = concurrent_downloads
