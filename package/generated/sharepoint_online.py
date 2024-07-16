# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from elastic_connectors.connectors.source import DataSourceConfiguration
from elastic_connectors.connectors.sources.sharepoint_online import (
    SharepointOnlineDataSource,
)
from elastic_connectors.connector_base import ConnectorBase


class SharepointOnlineConnector(ConnectorBase):
    """
    SharepointOnlineConnector class generated for connecting to the data source.

    Args:

        tenant_id (str): Tenant ID

        tenant_name (str): Tenant name

        client_id (str): Client ID

        secret_value (str): Secret value

        site_collections (list): Comma-separated list of sites
            - A comma-separated list of sites to ingest data from. If enumerating all sites, use * to include all available sites, or specify a list of site names. Otherwise, specify a list of site paths.

        enumerate_all_sites (bool): Enumerate all sites?
            - If enabled, sites will be fetched in bulk, then filtered down to the configured list of sites. This is efficient when syncing many sites. If disabled, each configured site will be fetched with an individual request. This is efficient when syncing fewer sites.

        fetch_subsites (bool): Fetch sub-sites of configured sites?
            - Whether subsites of the configured site(s) should be automatically fetched.

        fetch_drive_item_permissions (bool): Fetch drive item permissions
            - Enable this option to fetch drive item specific permissions. This setting can increase sync time.

        fetch_unique_page_permissions (bool): Fetch unique page permissions
            - Enable this option to fetch unique page permissions. This setting can increase sync time. If this setting is disabled a page will inherit permissions from its parent site.

        fetch_unique_list_permissions (bool): Fetch unique list permissions
            - Enable this option to fetch unique list permissions. This setting can increase sync time. If this setting is disabled a list will inherit permissions from its parent site.

        fetch_unique_list_item_permissions (bool): Fetch unique list item permissions
            - Enable this option to fetch unique list item permissions. This setting can increase sync time. If this setting is disabled a list item will inherit permissions from its parent site.

    """

    def __init__(
        self,
        tenant_id=None,
        tenant_name=None,
        client_id=None,
        secret_value=None,
        site_collections="*",
        enumerate_all_sites=True,
        fetch_subsites=True,
        fetch_drive_item_permissions=True,
        fetch_unique_page_permissions=True,
        fetch_unique_list_permissions=True,
        fetch_unique_list_item_permissions=True,
        **kwargs
    ):

        configuration = SharepointOnlineDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=SharepointOnlineDataSource(connector_configuration), **kwargs
        )

        self.tenant_id = tenant_id
        self.tenant_name = tenant_name
        self.client_id = client_id
        self.secret_value = secret_value
        self.site_collections = site_collections
        self.enumerate_all_sites = enumerate_all_sites
        self.fetch_subsites = fetch_subsites
        self.fetch_drive_item_permissions = fetch_drive_item_permissions
        self.fetch_unique_page_permissions = fetch_unique_page_permissions
        self.fetch_unique_list_permissions = fetch_unique_list_permissions
        self.fetch_unique_list_item_permissions = fetch_unique_list_item_permissions
