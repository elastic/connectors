# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.outlook import OutlookDataSource
from package.connectors.connector_base import ConnectorBase


class OutlookConnector(ConnectorBase):
    """
    OutlookConnector class generated for connecting to the data source.

    Args:

        data_source (str): Outlook data source

        tenant_id (str): Tenant ID

        client_id (str): Client ID

        client_secret (str): Client Secret Value

        exchange_server (str): Exchange Server
            - Exchange server's IP address. E.g. 127.0.0.1

        active_directory_server (str): Active Directory Server
            - Active Directory server's IP address. E.g. 127.0.0.1

        username (str): Exchange server username

        password (str): Exchange server password

        domain (str): Exchange server domain name
            - Domain name such as gmail.com, outlook.com

        ssl_enabled (bool): Enable SSL

        ssl_ca (str): SSL certificate

    """

    def __init__(
        self,
        data_source="outlook_cloud",
        tenant_id=None,
        client_id=None,
        client_secret=None,
        exchange_server=None,
        active_directory_server=None,
        username=None,
        password=None,
        domain=None,
        ssl_enabled=False,
        ssl_ca=None,
        **kwargs
    ):

        configuration = OutlookDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=OutlookDataSource(connector_configuration), **kwargs
        )

        self.data_source = data_source
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.exchange_server = exchange_server
        self.active_directory_server = active_directory_server
        self.username = username
        self.password = password
        self.domain = domain
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
