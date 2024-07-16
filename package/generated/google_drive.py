# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from elastic_connectors.connectors.source import DataSourceConfiguration
from elastic_connectors.connectors.sources.google_drive import GoogleDriveDataSource
from elastic_connectors.connector_base import ConnectorBase


class GoogleDriveConnector(ConnectorBase):
    """
    GoogleDriveConnector class generated for connecting to the data source.

    Args:

        service_account_credentials (str): Google Drive service account JSON
            - This connectors authenticates as a service account to synchronize content from Google Drive.

        use_domain_wide_delegation_for_sync (bool): Use domain-wide delegation for data sync
            - Enable domain-wide delegation to automatically sync content from all shared and personal drives in the Google workspace. This eliminates the need to manually share Google Drive data with your service account, though it may increase sync time. If disabled, only items and folders manually shared with the service account will be synced. Please refer to the connector documentation to ensure domain-wide delegation is correctly configured and has the appropriate scopes.

        google_workspace_admin_email_for_data_sync (str): Google Workspace admin email
            - Provide the admin email to be used with domain-wide delegation for data sync. This email enables the connector to utilize the Admin Directory API for listing organization users. Please refer to the connector documentation to ensure domain-wide delegation is correctly configured and has the appropriate scopes.

        google_workspace_email_for_shared_drives_sync (str): Google Workspace email for syncing shared drives
            - Provide the Google Workspace user email for discovery and syncing of shared drives. Only the shared drives this user has access to will be synced.

        google_workspace_admin_email (str): Google Workspace admin email
            - In order to use Document Level Security you need to enable Google Workspace domain-wide delegation of authority for your service account. A service account with delegated authority can impersonate admin user with sufficient permissions to fetch all users and their corresponding permissions. Please refer to the connector documentation to ensure domain-wide delegation is correctly configured and has the appropriate scopes.

        max_concurrency (int): Maximum concurrent HTTP requests
            - This setting determines the maximum number of concurrent HTTP requests sent to the Google API to fetch data. Increasing this value can improve data retrieval speed, but it may also place higher demands on system resources and network bandwidth.

    """

    def __init__(
        self,
        service_account_credentials=None,
        use_domain_wide_delegation_for_sync=False,
        google_workspace_admin_email_for_data_sync=None,
        google_workspace_email_for_shared_drives_sync=None,
        google_workspace_admin_email=None,
        max_concurrency=25,
        **kwargs
    ):

        configuration = GoogleDriveDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=GoogleDriveDataSource(connector_configuration), **kwargs
        )

        self.service_account_credentials = service_account_credentials
        self.use_domain_wide_delegation_for_sync = use_domain_wide_delegation_for_sync
        self.google_workspace_admin_email_for_data_sync = (
            google_workspace_admin_email_for_data_sync
        )
        self.google_workspace_email_for_shared_drives_sync = (
            google_workspace_email_for_shared_drives_sync
        )
        self.google_workspace_admin_email = google_workspace_admin_email
        self.max_concurrency = max_concurrency
