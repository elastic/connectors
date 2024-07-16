# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from elastic_connectors.connectors.source import DataSourceConfiguration
from elastic_connectors.connectors.sources.network_drive import NASDataSource
from elastic_connectors.connector_base import ConnectorBase


class NASConnector(ConnectorBase):
    """
    NASConnector class generated for connecting to the data source.

    Args:

        username (str): Username

        password (str): Password

        server_ip (str): SMB IP

        server_port (int): SMB port

        drive_path (str): SMB path

        drive_type (str): Drive type

        identity_mappings (str): Path of CSV file containing users and groups SID (For Linux Network Drive)

    """

    def __init__(
        self,
        username=None,
        password=None,
        server_ip=None,
        server_port=None,
        drive_path=None,
        drive_type="windows",
        identity_mappings=None,
        **kwargs
    ):

        configuration = NASDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(data_provider=NASDataSource(connector_configuration), **kwargs)

        self.username = username
        self.password = password
        self.server_ip = server_ip
        self.server_port = server_port
        self.drive_path = drive_path
        self.drive_type = drive_type
        self.identity_mappings = identity_mappings
