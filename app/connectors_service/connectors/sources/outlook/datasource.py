#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from functools import cached_property

from connectors_sdk.utils import iso_utc

from connectors.access_control import es_access_control_query
from connectors.sources.exchange_server.client import _extract_ldap_mail
from connectors.sources.exchange_server.datasource import ExchangeServerDataSource
from connectors.sources.exchange_server.utils import (
    _prefix_display_name,
    _prefix_email,
    _prefix_job,
    _prefix_user_id,
)
from connectors.sources.outlook.client import OutlookClient
from connectors.sources.outlook.constants import OUTLOOK_CLOUD, OUTLOOK_SERVER


class OutlookDataSource(ExchangeServerDataSource):
    """Outlook"""

    name = "Outlook"
    service_type = "outlook"

    @cached_property
    def client(self):
        return OutlookClient(configuration=self.configuration)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Outlook

        Returns:
            dictionary: Default configuration.
        """
        return {
            "data_source": {
                "display": "dropdown",
                "label": "Outlook data source",
                "options": [
                    {"label": "Outlook Cloud", "value": OUTLOOK_CLOUD},
                    {"label": "Outlook Server", "value": OUTLOOK_SERVER},
                ],
                "order": 1,
                "type": "str",
                "value": OUTLOOK_CLOUD,
            },
            "tenant_id": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_CLOUD}],
                "label": "Tenant ID",
                "order": 2,
                "type": "str",
            },
            "client_id": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_CLOUD}],
                "label": "Client ID",
                "order": 3,
                "type": "str",
            },
            "client_secret": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_CLOUD}],
                "label": "Client Secret Value",
                "order": 4,
                "sensitive": True,
                "type": "str",
            },
            "exchange_server": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_SERVER}],
                "label": "Exchange Server",
                "order": 5,
                "tooltip": "Exchange server's IP address. E.g. 127.0.0.1",
                "type": "str",
            },
            "active_directory_server": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_SERVER}],
                "label": "Active Directory Server",
                "order": 6,
                "tooltip": "Active Directory server's IP address. E.g. 127.0.0.1",
                "type": "str",
            },
            "username": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_SERVER}],
                "label": "Exchange server username",
                "order": 7,
                "type": "str",
            },
            "password": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_SERVER}],
                "label": "Exchange server password",
                "order": 8,
                "sensitive": True,
                "type": "str",
            },
            "domain": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_SERVER}],
                "label": "Exchange server domain name",
                "order": 9,
                "tooltip": "Domain name such as gmail.com, outlook.com",
                "type": "str",
            },
            "ssl_enabled": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_SERVER}],
                "display": "toggle",
                "label": "Enable SSL",
                "order": 10,
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [
                    {"field": "data_source", "value": OUTLOOK_SERVER},
                    {"field": "ssl_enabled", "value": True},
                ],
                "label": "SSL certificate",
                "order": 11,
                "type": "str",
            },
            "sync_all_mail_folders": {
                "display": "toggle",
                "label": "Sync all mail folders",
                "order": 12,
                "tooltip": "When enabled, indexes the user mail folders in each mailbox, not only Inbox, Sent, Junk, and Archive. System folders such as Deleted Items, Drafts, Outbox, and search folders are never indexed. Expect longer syncs, more Exchange load, and a larger index.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 13,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "include_full_raw_message": {
                "display": "toggle",
                "label": "Index full raw email (including headers)",
                "order": 13,
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
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 14,
                "tooltip": "Document level security ensures identities and permissions set in Outlook are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
        }

    def _validate_ssl_certificate(self):
        if self.configuration["data_source"] != OUTLOOK_SERVER:
            return
        super()._validate_ssl_certificate()

    async def get_access_control(self):
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        async for users in self.client._fetch_all_users():
            if self.configuration["data_source"] == OUTLOOK_CLOUD:
                for user in users.get("value", []):
                    yield await self._user_access_control_doc(user=user)
            elif _extract_ldap_mail(users.get("attributes", {})):
                yield await self._user_access_control_doc_for_server(users=users)

    async def _user_access_control_doc(self, user):
        user_id = user.get("id", "")
        display_name = user.get("displayName", "")
        user_email = user.get("mail", "")
        job_title = user.get("jobTitle", "")

        _prefixed_user_id = _prefix_user_id(user_id=user_id)
        _prefixed_display_name = _prefix_display_name(user=display_name)
        _prefixed_email = _prefix_email(email=user_email)
        _prefixed_job = _prefix_job(job_title=job_title)
        return {
            "_id": user_id,
            "identity": {
                "user_id": _prefixed_user_id,
                "display_name": _prefixed_display_name,
                "email": _prefixed_email,
                "job_title": _prefixed_job,
            },
            "created_at": iso_utc(),
        } | es_access_control_query(
            access_control=[_prefixed_user_id, _prefixed_display_name, _prefixed_email]
        )
