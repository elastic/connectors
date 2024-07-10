# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.salesforce import SalesforceDataSource
from package.connectors.connector_base import ConnectorBase


class SalesforceConnector(ConnectorBase):
    """
    SalesforceConnector class generated for connecting to the data source.

    Args:

        domain (str): Domain
            - The domain for your Salesforce instance. If your Salesforce URL is 'foo.my.salesforce.com', the domain would be 'foo'.

        client_id (str): Client ID
            - The client id for your OAuth2-enabled connected app. Also called 'consumer key'

        client_secret (str): Client Secret
            - The client secret for your OAuth2-enabled connected app. Also called 'consumer secret'

    """

    def __init__(self, domain=None, client_id=None, client_secret=None, **kwargs):

        configuration = SalesforceDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=SalesforceDataSource(connector_configuration), **kwargs
        )

        self.domain = domain
        self.client_id = client_id
        self.client_secret = client_secret
