# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from elastic_connectors.connectors.source import DataSourceConfiguration
from elastic_connectors.connectors.sources.microsoft_teams import (
    MicrosoftTeamsDataSource,
)
from elastic_connectors.connector_base import ConnectorBase


class MicrosoftTeamsConnector(ConnectorBase):
    """
    MicrosoftTeamsConnector class generated for connecting to the data source.

    Args:

        tenant_id (str): Tenant ID

        client_id (str): Client ID

        secret_value (str): Secret value

        username (str): Username

        password (str): Password

    """

    def __init__(
        self,
        tenant_id=None,
        client_id=None,
        secret_value=None,
        username=None,
        password=None,
        **kwargs
    ):

        configuration = MicrosoftTeamsDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=MicrosoftTeamsDataSource(connector_configuration), **kwargs
        )

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.secret_value = secret_value
        self.username = username
        self.password = password
