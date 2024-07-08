# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.dropbox import DropboxDataSource
from package.connectors.connector_base import ConnectorBase


class DropboxConnector(ConnectorBase):
    """
    DropboxConnector class generated for connecting to the data source.

    Args:

        path (str): Path to fetch files/folders
            - Path is ignored when Advanced Sync Rules are used.

        app_key (str): App Key

        app_secret (str): App secret

        refresh_token (str): Refresh token

        retry_count (int): Retries per request

        concurrent_downloads (int): Maximum concurrent downloads

        include_inherited_users_and_groups (bool): Include groups and inherited users
            - Include groups and inherited users when indexing permissions. Enabling this configurable field will cause a significant performance degradation.

    """

    def __init__(
        self,
        path=None,
        app_key=None,
        app_secret=None,
        refresh_token=None,
        retry_count=3,
        concurrent_downloads=100,
        include_inherited_users_and_groups=False,
    ):

        configuration = DropboxDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(data_provider=DropboxDataSource(connector_configuration))

        self.path = path
        self.app_key = app_key
        self.app_secret = app_secret
        self.refresh_token = refresh_token
        self.retry_count = retry_count
        self.concurrent_downloads = concurrent_downloads
        self.include_inherited_users_and_groups = include_inherited_users_and_groups
