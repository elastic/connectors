# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.servicenow import ServiceNowDataSource
from package.connectors.connector_base import ConnectorBase


class ServiceNowConnector(ConnectorBase):
    """
    ServiceNowConnector class generated for connecting to the data source.

    Args:

        url (str): Service URL

        username (str): Username

        password (str): Password

        services (list): Comma-separated list of services
            - List of services is ignored when Advanced Sync Rules are used.

        retry_count (int): Retries per request

        concurrent_downloads (int): Maximum concurrent downloads

    """

    def __init__(
        self,
        url=None,
        username=None,
        password=None,
        services="*",
        retry_count=3,
        concurrent_downloads=10,
    ):

        configuration = ServiceNowDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(data_provider=ServiceNowDataSource(connector_configuration))

        self.url = url
        self.username = username
        self.password = password
        self.services = services
        self.retry_count = retry_count
        self.concurrent_downloads = concurrent_downloads
