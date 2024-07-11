# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from elastic_connectors.connectors.source import DataSourceConfiguration
from elastic_connectors.connectors.sources.oracle import OracleDataSource
from elastic_connectors.connector_base import ConnectorBase


class OracleConnector(ConnectorBase):
    """
    OracleConnector class generated for connecting to the data source.

    Args:

        host (str): Host

        port (int): Port

        username (str): Username

        password (str): Password

        connection_source (str): Connection Source
            - Select 'Service Name' option if connecting to a pluggable database

        sid (str): SID

        service_name (str): Service Name

        tables (list): Comma-separated list of tables

        fetch_size (int): Rows fetched per request

        retry_count (int): Retries per request

        oracle_protocol (str): Oracle connection protocol

        oracle_home (str): Path to Oracle Home

        wallet_configuration_path (str): Path to SSL Wallet configuration files

    """

    def __init__(
        self,
        host=None,
        port=None,
        username=None,
        password=None,
        connection_source="sid",
        sid=None,
        service_name=None,
        tables="*",
        fetch_size=50,
        retry_count=3,
        oracle_protocol="TCP",
        oracle_home="",
        wallet_configuration_path="",
        **kwargs
    ):

        configuration = OracleDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=OracleDataSource(connector_configuration), **kwargs
        )

        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection_source = connection_source
        self.sid = sid
        self.service_name = service_name
        self.tables = tables
        self.fetch_size = fetch_size
        self.retry_count = retry_count
        self.oracle_protocol = oracle_protocol
        self.oracle_home = oracle_home
        self.wallet_configuration_path = wallet_configuration_path
