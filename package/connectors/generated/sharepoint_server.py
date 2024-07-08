# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.sharepoint_server import SharepointServerDataSource
from package.connectors.connector_base import ConnectorBase


class SharepointServerConnector(ConnectorBase):
    """
    SharepointServerConnector class generated for connecting to the data source.

    Args:

        username (str): SharePoint Server username

        password (str): SharePoint Server password

        host_url (str): SharePoint host

        site_collections (list): Comma-separated list of SharePoint site collections to index

        ssl_enabled (bool): Enable SSL

        ssl_ca (str): SSL certificate

        retry_count (int): Retries per request

        fetch_unique_list_permissions (bool): Fetch unique list permissions
            - Enable this option to fetch unique list permissions. This setting can increase sync time. If this setting is disabled a list will inherit permissions from its parent site.

        fetch_unique_list_item_permissions (bool): Fetch unique list item permissions
            - Enable this option to fetch unique list item permissions. This setting can increase sync time. If this setting is disabled a list item will inherit permissions from its parent site.

    """

    def __init__(
        self,
        username=None,
        password=None,
        host_url=None,
        site_collections=None,
        ssl_enabled=False,
        ssl_ca=None,
        retry_count=3,
        fetch_unique_list_permissions=True,
        fetch_unique_list_item_permissions=True,
    ):

        configuration = SharepointServerDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=SharepointServerDataSource(connector_configuration)
        )

        self.username = username
        self.password = password
        self.host_url = host_url
        self.site_collections = site_collections
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.retry_count = retry_count
        self.fetch_unique_list_permissions = fetch_unique_list_permissions
        self.fetch_unique_list_item_permissions = fetch_unique_list_item_permissions
