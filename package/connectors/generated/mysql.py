# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.mysql import MySqlDataSource
from package.connectors.connector_base import ConnectorBase


class MySqlConnector(ConnectorBase):
    """
    MySqlConnector class generated for connecting to the data source.

    Args:

        host (str): Host

        port (int): Port

        user (str): Username

        password (str): Password

        database (str): Database

        tables (list): Comma-separated list of tables

        ssl_enabled (bool): Enable SSL

        ssl_ca (str): SSL certificate

        fetch_size (int): Rows fetched per request

        retry_count (int): Retries per request

    """

    def __init__(
        self,
        host=None,
        port=None,
        user=None,
        password=None,
        database=None,
        tables="*",
        ssl_enabled=False,
        ssl_ca=None,
        fetch_size=50,
        retry_count=3,
        **kwargs
    ):

        configuration = MySqlDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=MySqlDataSource(connector_configuration), **kwargs
        )

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.tables = tables
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.fetch_size = fetch_size
        self.retry_count = retry_count
