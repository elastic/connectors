#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from copy import copy
from functools import cached_property, partial

from connectors_sdk.source import BaseDataSource
from connectors_sdk.utils import (
    hash_id,
    iso_utc,
)

from connectors.access_control import ACCESS_CONTROL, es_access_control_query
from connectors.sources.outlook.client import OutlookClient
from connectors.sources.outlook.constants import (
    CALENDAR_ATTACHMENT,
    DEFAULT_TIMEZONE,
    MAIL_ATTACHMENT,
    OUTLOOK_CLOUD,
    OUTLOOK_SERVER,
    TASK_ATTACHMENT,
)
from connectors.sources.outlook.utils import (
    _prefix_display_name,
    _prefix_email,
    _prefix_job,
    _prefix_user_id,
    ews_format_to_datetime,
)
from connectors.utils import html_to_text


class OutlookDocFormatter:
    """Format Outlook object documents to Elasticsearch document"""

    def mails_doc_formatter(self, mail, mail_type, timezone):
        return {
            "_id": mail.id,
            "_timestamp": ews_format_to_datetime(
                source_datetime=mail.last_modified_time, timezone=timezone
            ),
            "title": mail.subject,
            "type": mail_type["constant"],
            "sender": mail.sender.email_address,
            "to_recipients": [
                recipient.email_address for recipient in (mail.to_recipients or [])
            ],
            "cc_recipients": [
                recipient.email_address for recipient in (mail.cc_recipients or [])
            ],
            "bcc_recipients": [
                recipient.email_address for recipient in (mail.bcc_recipients or [])
            ],
            "importance": mail.importance,
            "categories": list((mail.categories or [])),
            "message": html_to_text(html=mail.body),
        }

    def calendar_doc_formatter(self, calendar, child_calendar, timezone):
        document = {
            "_id": calendar.id,
            "_timestamp": ews_format_to_datetime(
                source_datetime=calendar.last_modified_time, timezone=timezone
            ),
            "type": "Calendar",
            "title": calendar.subject,
            "meeting_type": "Single"
            if calendar.type == "Single"
            else f"Recurring {calendar.recurrence.pattern}",
            "organizer": calendar.organizer.email_address,
        }

        if child_calendar in ["Folder (Birthdays)", "Birthdays (Birthdays)"]:
            document.update(
                {
                    "date": ews_format_to_datetime(
                        source_datetime=calendar.start, timezone=timezone
                    ).split("T", 1)[0],
                }
            )
        else:
            document.update(
                {
                    "attendees": [
                        attendee.mailbox.email_address
                        for attendee in (calendar.required_attendees or [])
                        if attendee.mailbox.email_address
                    ],
                    "start_date": ews_format_to_datetime(
                        source_datetime=calendar.start, timezone=timezone
                    ),
                    "end_date": ews_format_to_datetime(
                        source_datetime=calendar.end, timezone=timezone
                    ),
                    "location": calendar.location,
                    "content": html_to_text(html=calendar.body),
                }
            )

        return document

    def task_doc_formatter(self, task, timezone):
        return {
            "_id": task.id,
            "_timestamp": ews_format_to_datetime(
                source_datetime=task.last_modified_time, timezone=timezone
            ),
            "type": "Task",
            "title": task.subject,
            "owner": task.owner,
            "start_date": ews_format_to_datetime(
                source_datetime=task.start_date, timezone=timezone
            ),
            "due_date": ews_format_to_datetime(
                source_datetime=task.due_date, timezone=timezone
            ),
            "complete_date": ews_format_to_datetime(
                source_datetime=task.complete_date, timezone=timezone
            ),
            "categories": list((task.categories or [])),
            "importance": task.importance,
            "content": task.text_body,
            "status": task.status,
        }

    def contact_doc_formatter(self, contact, timezone):
        return {
            "_id": contact.id,
            "type": "Contact",
            "_timestamp": ews_format_to_datetime(
                source_datetime=contact.last_modified_time, timezone=timezone
            ),
            "name": contact.display_name,
            "email_addresses": [
                email.email for email in (contact.email_addresses or [])
            ],
            "contact_numbers": [
                number.phone_number
                for number in contact.phone_numbers or []
                if number.phone_number
            ],
            "company_name": contact.company_name,
            "birthday": ews_format_to_datetime(
                source_datetime=contact.birthday, timezone=timezone
            ),
        }

    def attachment_doc_formatter(self, attachment, attachment_type, timezone):
        return {
            "_id": attachment.attachment_id.id,
            "title": attachment.name,
            "type": attachment_type,
            "_timestamp": ews_format_to_datetime(
                source_datetime=attachment.last_modified_time, timezone=timezone
            ),
            "size": attachment.size,
        }


class OutlookDataSource(BaseDataSource):
    """Outlook"""

    name = "Outlook"
    service_type = "outlook"
    incremental_sync_enabled = True
    dls_enabled = True

    def __init__(self, configuration):
        """Setup the connection to the Outlook

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
            logger_ (DocumentLogger): Object of DocumentLogger class.
        """
        super().__init__(configuration=configuration)
        self.configuration = configuration
        self.doc_formatter = OutlookDocFormatter()

    @cached_property
    def client(self):
        return OutlookClient(configuration=self.configuration)

    def _set_internal_logger(self):
        self.client.set_logger(self._logger)

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
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 12,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 13,
                "tooltip": "Document level security ensures identities and permissions set in Outlook are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
        }

    def _dls_enabled(self):
        """Check if document level security is enabled. This method checks whether document level security (DLS) is enabled based on the provided configuration.

        Returns:
            bool: True if document level security is enabled, False otherwise.
        """
        if (
            self._features is None
            or not self._features.document_level_security_enabled()
        ):
            return False

        return self.configuration["use_document_level_security"]

    async def get_access_control(self):
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        async for users in self.client._fetch_all_users():
            if self.configuration["data_source"] == OUTLOOK_CLOUD:
                for user in users.get("value", []):
                    yield await self._user_access_control_doc(user=user)
            elif users.get("attributes", {}).get("mail"):
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

    async def _user_access_control_doc_for_server(self, users):
        name_metadata = users.get("dn", "").split("=", 1)[1]
        display_name = name_metadata.split(",", 1)[0]
        user_email = users.get("attributes", {}).get("mail")
        user_id = hash_id(user_email)

        _prefixed_user_id = _prefix_user_id(user_id=user_id)
        _prefixed_display_name = _prefix_display_name(user=display_name)
        _prefixed_email = _prefix_email(email=user_email)
        return {
            "_id": user_id,
            "identity": {
                "user_id": _prefixed_user_id,
                "display_name": _prefixed_display_name,
                "email": _prefixed_email,
            },
            "created_at": iso_utc(),
        } | es_access_control_query(
            access_control=[_prefixed_user_id, _prefixed_display_name, _prefixed_email]
        )

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )
        return document

    async def close(self):
        await self.client._get_user_instance.close()

    async def get_content(self, attachment, timezone, timestamp=None, doit=False):
        """Extracts the content for allowed file types.

        Args:
            attachment (dictionary): Formatted attachment document.
            timezone (str): User timezone for _timestamp
            timestamp (timestamp, optional): Timestamp of attachment last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and attachment content
        """
        file_size = attachment.size
        if not (doit and file_size > 0):
            return

        filename = attachment.name
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(
            file_extension,
            filename,
            file_size,
        ):
            return

        document = {
            "_id": attachment.attachment_id.id,
            "_timestamp": ews_format_to_datetime(
                source_datetime=attachment.last_modified_time, timezone=timezone
            ),
        }
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(self.download_func, attachment.content),
        )

    async def download_func(self, content):
        """This is a fake-download function
        Its only purpose is to allow attachment content to be
        written to a temp file.
        This is because outlook doesn't download files,
        it instead contains a key with bytes in its response.
        """
        yield content

    async def _fetch_attachments(
        self, attachment_type, outlook_object, timezone, account
    ):
        for attachment in outlook_object.attachments:
            document = self.doc_formatter.attachment_doc_formatter(
                attachment=attachment,
                attachment_type=attachment_type,
                timezone=timezone,
            )
            yield (
                self._decorate_with_access_control(
                    document, [account.primary_smtp_address]
                ),
                partial(
                    self.get_content, attachment=copy(attachment), timezone=timezone
                ),
            )

    async def _fetch_mails(self, account, timezone):
        async for mail, mail_type in self.client.get_mails(account=account):
            document = self.doc_formatter.mails_doc_formatter(
                mail=mail,
                mail_type=mail_type,
                timezone=timezone,
            )
            yield (
                self._decorate_with_access_control(
                    document, [account.primary_smtp_address]
                ),
                None,
            )

            if mail.has_attachments:
                async for doc in self._fetch_attachments(
                    attachment_type=MAIL_ATTACHMENT,
                    outlook_object=mail,
                    timezone=timezone,
                    account=account,
                ):
                    yield doc

    async def _fetch_contacts(self, account, timezone):
        self._logger.debug(f"Fetching contacts for {account.primary_smtp_address}")
        async for contact in self.client.get_contacts(account=account):
            document = self.doc_formatter.contact_doc_formatter(
                contact=contact,
                timezone=timezone,
            )
            yield (
                self._decorate_with_access_control(
                    document, [account.primary_smtp_address]
                ),
                None,
            )

    async def _fetch_tasks(self, account, timezone):
        self._logger.debug(f"Fetching tasks for {account.primary_smtp_address}")
        async for task in self.client.get_tasks(account=account):
            document = self.doc_formatter.task_doc_formatter(
                task=task, timezone=timezone
            )
            yield (
                self._decorate_with_access_control(
                    document, [account.primary_smtp_address]
                ),
                None,
            )

            if task.has_attachments:
                async for doc in self._fetch_attachments(
                    attachment_type=TASK_ATTACHMENT,
                    outlook_object=task,
                    timezone=timezone,
                    account=account,
                ):
                    yield doc

    async def _fetch_calendars(self, account, timezone):
        self._logger.debug(f"Fetching calendars for {account.primary_smtp_address}")
        async for calendar in self.client.get_calendars(account=account):
            async for doc in self._enqueue_calendars(
                calendar=calendar,
                child_calendar=calendar,
                timezone=timezone,
                account=account,
            ):
                yield doc

    async def _fetch_child_calendars(self, account, timezone):
        self._logger.debug(
            f"Fetching child calendars for {account.primary_smtp_address}"
        )
        async for calendar, child_calendar in self.client.get_child_calendars(
            account=account
        ):
            async for doc in self._enqueue_calendars(
                calendar=calendar,
                child_calendar=child_calendar,
                timezone=timezone,
                account=account,
            ):
                yield doc

    async def _enqueue_calendars(self, calendar, child_calendar, timezone, account):
        document = self.doc_formatter.calendar_doc_formatter(
            calendar=calendar,
            child_calendar=str(child_calendar),
            timezone=timezone,
        )
        yield (
            self._decorate_with_access_control(
                document, [account.primary_smtp_address]
            ),
            None,
        )

        if calendar.has_attachments:
            async for doc in self._fetch_attachments(
                attachment_type=CALENDAR_ATTACHMENT,
                outlook_object=calendar,
                timezone=timezone,
                account=account,
            ):
                yield doc

    async def ping(self):
        """Verify the connection with Outlook"""
        await self.client.ping()
        self._logger.info("Successfully connected to Outlook")

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch outlook objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """
        async for account in self.client._get_user_instance.get_user_accounts():
            timezone = account.default_timezone or DEFAULT_TIMEZONE

            async for mail in self._fetch_mails(account=account, timezone=timezone):
                yield mail

            async for contact in self._fetch_contacts(
                account=account, timezone=timezone
            ):
                yield contact

            async for task in self._fetch_tasks(account=account, timezone=timezone):
                yield task

            async for calendar in self._fetch_calendars(
                account=account, timezone=timezone
            ):
                yield calendar

            async for child_calendar in self._fetch_child_calendars(
                account=account, timezone=timezone
            ):
                yield child_calendar
