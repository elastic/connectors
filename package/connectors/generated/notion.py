# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.notion import NotionDataSource
from package.connectors.connector_base import ConnectorBase


class NotionConnector(ConnectorBase):
    """
    NotionConnector class generated for connecting to the data source.

    Args:

        notion_secret_key (str): Notion Secret Key

        databases (list): List of Databases

        pages (list): List of Pages

        index_comments (bool): Enable indexing comments
            - Enabling this will increase the amount of network calls to the source, and may decrease performance

        concurrent_downloads (int): Maximum concurrent downloads

    """

    def __init__(
        self,
        notion_secret_key=None,
        databases=None,
        pages=None,
        index_comments=False,
        concurrent_downloads=30,
    ):

        configuration = NotionDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(data_provider=NotionDataSource(connector_configuration))

        self.notion_secret_key = notion_secret_key
        self.databases = databases
        self.pages = pages
        self.index_comments = index_comments
        self.concurrent_downloads = concurrent_downloads
