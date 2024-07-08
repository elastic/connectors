# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.github import GitHubDataSource
from package.connectors.connector_base import ConnectorBase


class GitHubConnector(ConnectorBase):
    """
    GitHubConnector class generated for connecting to the data source.

    Args:

        data_source (str): Data source

        host (str): Server URL

        auth_method (str): Authentication method

        token (str): Token

        repo_type (str): Repository Type
            - The Document Level Security feature is not available for the Other Repository Type

        org_name (str): Organization Name

        app_id (int): App ID

        private_key (str): App private key

        repositories (list): List of repositories
            - This configurable field is ignored when Advanced Sync Rules are used.

        ssl_enabled (bool): Enable SSL

        ssl_ca (str): SSL certificate

        retry_count (int): Maximum retries per request

    """

    def __init__(
        self,
        data_source="github_server",
        host=None,
        auth_method="personal_access_token",
        token=None,
        repo_type="other",
        org_name=None,
        app_id=None,
        private_key=None,
        repositories=None,
        ssl_enabled=False,
        ssl_ca=None,
        retry_count=3,
    ):

        configuration = GitHubDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(data_provider=GitHubDataSource(connector_configuration))

        self.data_source = data_source
        self.host = host
        self.auth_method = auth_method
        self.token = token
        self.repo_type = repo_type
        self.org_name = org_name
        self.app_id = app_id
        self.private_key = private_key
        self.repositories = repositories
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.retry_count = retry_count
