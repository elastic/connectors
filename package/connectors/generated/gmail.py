# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.gmail import GMailDataSource
from package.connectors.connector_base import ConnectorBase


class GMailConnector(ConnectorBase):
    """
    GMailConnector class generated for connecting to the data source.

    Args:

        service_account_credentials (str): GMail service account JSON

        subject (str): Google Workspace admin email
            - Admin account email address

        customer_id (str): Google customer id
            - Google admin console -> Account -> Settings -> Customer Id

        include_spam_and_trash (bool): Include spam and trash emails
            - Will include spam and trash emails, when set to true.

    """

    def __init__(
        self,
        service_account_credentials=None,
        subject=None,
        customer_id=None,
        include_spam_and_trash=False,
    ):

        configuration = GMailDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(data_provider=GMailDataSource(connector_configuration))

        self.service_account_credentials = service_account_credentials
        self.subject = subject
        self.customer_id = customer_id
        self.include_spam_and_trash = include_spam_and_trash
