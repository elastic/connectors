# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.jira import JiraDataSource
from package.connectors.connector_base import ConnectorBase


class JiraConnector(ConnectorBase):
    """
    JiraConnector class generated for connecting to the data source.

    Args:

        data_source (str): Jira data source

        username (str): Jira Server username

        password (str): Jira Server password

        data_center_username (str): Jira Data Center username

        data_center_password (str): Jira Data Center password

        account_email (str): Jira Cloud email address
            - Email address associated with Jira Cloud account. E.g. jane.doe@gmail.com

        api_token (str): Jira Cloud API token

        jira_url (str): Jira host url

        projects (list): Jira project keys
            - This configurable field is ignored when Advanced Sync Rules are used.

        ssl_enabled (bool): Enable SSL

        ssl_ca (str): SSL certificate

        retry_count (int): Retries for failed requests

        concurrent_downloads (int): Maximum concurrent downloads

    """

    def __init__(
        self,
        data_source="jira_cloud",
        username=None,
        password=None,
        data_center_username=None,
        data_center_password=None,
        account_email=None,
        api_token=None,
        jira_url=None,
        projects=None,
        ssl_enabled=False,
        ssl_ca=None,
        retry_count=3,
        concurrent_downloads=100,
        **kwargs
    ):

        configuration = JiraDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=JiraDataSource(connector_configuration), **kwargs
        )

        self.data_source = data_source
        self.username = username
        self.password = password
        self.data_center_username = data_center_username
        self.data_center_password = data_center_password
        self.account_email = account_email
        self.api_token = api_token
        self.jira_url = jira_url
        self.projects = projects
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.retry_count = retry_count
        self.concurrent_downloads = concurrent_downloads
