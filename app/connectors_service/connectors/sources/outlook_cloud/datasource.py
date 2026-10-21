#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from functools import cached_property

from connectors_sdk.source import BaseDataSource

from connectors.sources.outlook_cloud.client import OutlookCloudClient


class OutlookCloudDataSource(BaseDataSource):
    """Outlook Cloud (Exchange Online) through Microsoft Graph.

    Only the connection, user listing and mailbox checks exist so far. `get_docs` fails
    on purpose, so a sync stops instead of replacing the index with an empty one.
    """

    name = "Outlook Cloud"
    service_type = "outlook_cloud"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self.configuration = configuration

    @cached_property
    def client(self):
        return OutlookCloudClient(configuration=self.configuration)

    def _set_internal_logger(self):
        self.client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        return {
            "tenant_id": {
                "label": "Tenant ID",
                "order": 1,
                "type": "str",
            },
            "client_id": {
                "label": "Client ID",
                "order": 2,
                "type": "str",
            },
            "client_secret": {
                "label": "Client secret value",
                "order": 3,
                "sensitive": True,
                "type": "str",
            },
        }

    async def close(self):
        # `client` is a cached property: only close it if it was created.
        client = self.__dict__.get("client")
        if client is not None:
            await client.close()

    async def ping(self):
        try:
            await self.client.ping()
            self._logger.info("Successfully connected to Microsoft Graph")
        except Exception:
            self._logger.exception("Error while connecting to Microsoft Graph")
            raise

    async def get_docs(self, filtering=None):
        msg = "The Outlook Cloud connector does not sync content yet."
        raise NotImplementedError(msg)
        yield  # unreachable: makes this an async generator, as the sync job expects
