#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from functools import cached_property

from aiohttp.client_exceptions import ClientResponseError
from connectors_sdk.source import BaseDataSource

from connectors.sources.outlook_cloud.client import OutlookCloudClient
from connectors.sources.outlook_cloud.constants import DEFAULT_MAIL_FOLDERS
from connectors.sources.outlook_cloud.formatter import OutlookCloudDocFormatter
from connectors.sources.outlook_cloud.mail_eml import mail_attachment_base64


class OutlookCloudDataSource(BaseDataSource):
    """Outlook Cloud (Exchange Online) through Microsoft Graph.

    Not registered in `connectors/config.py` on purpose (dark launch): the service type is
    added there by the last content-sync PR of #4264, so no release exposes a connector
    that cannot sync yet.
    """

    name = "Outlook Cloud"
    service_type = "outlook_cloud"
    incremental_sync_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self.configuration = configuration
        self.doc_formatter = OutlookCloudDocFormatter()

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
            "sync_all_mail_folders": {
                "display": "toggle",
                "label": "Sync all mail folders",
                "order": 4,
                "tooltip": "When enabled, indexes the user mail folders in each mailbox, not only Inbox, Sent, Junk, and Archive. System folders such as Deleted Items, Drafts, Outbox, and search folders are never indexed. Expect longer syncs, more Microsoft Graph load, and a larger index.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "include_full_raw_message": {
                "display": "toggle",
                "label": "Index full raw email (including headers)",
                "order": 6,
                "tooltip": (
                    "When disabled (default), the email body and a small set of headers "
                    "(such as Subject, From, and To) are indexed. "
                    "Enable to keep the full raw message including routing and "
                    "authentication headers - useful for edge cases where body "
                    "extraction misses content."
                ),
                "type": "bool",
                "value": False,
            },
        }

    async def close(self):
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

    async def _mail_attachment(self, user_id, message):
        include_full_raw = self.configuration["include_full_raw_message"]
        raw_bytes = None
        if include_full_raw:
            try:
                raw_bytes = await self.client.get_raw_message(user_id, message["id"])
            except ClientResponseError as error:
                if error.status != 404:
                    raise
        return mail_attachment_base64(
            message=message,
            include_full_raw_message=include_full_raw,
            raw_mime_bytes=raw_bytes,
            logger=self._logger,
        )

    async def _yield_mail_documents(self, user_id, folder_segment, mail_type):
        async for message in self.client.get_messages(
            user_id, folder_segment, mail_type
        ):
            document = self.doc_formatter.mail_doc_formatter(message, mail_type)
            document["_attachment"] = await self._mail_attachment(user_id, message)
            yield document, None

    async def _yield_user_mail(self, user):
        user_id = user["id"]
        for entry in DEFAULT_MAIL_FOLDERS:
            mail_type = {"constant": entry["constant"]}
            async for document, lazy_download in self._yield_mail_documents(
                user_id, entry["well_known"], mail_type
            ):
                yield document, lazy_download

        if not self.configuration["sync_all_mail_folders"]:
            return

        async for folder in self.client.get_additional_mail_folders(user_id):
            async for document, lazy_download in self._yield_mail_documents(
                user_id, folder["folder_id"], folder["mail_type"]
            ):
                yield document, lazy_download

    async def get_docs(self, filtering=None):
        async for user in self.client.get_accessible_mailboxes():
            async for document, lazy_download in self._yield_user_mail(user):
                yield document, lazy_download
