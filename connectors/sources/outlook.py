#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft Outlook source module is responsible to fetch documents from Outlook server or cloud platforms.
"""
import asyncio
import os
from copy import copy
from datetime import date
from functools import cached_property, partial

import aiofiles
import aiohttp
import exchangelib
import requests.adapters
from aiofiles.os import remove
from exchangelib import (
    IMPERSONATION,
    OAUTH2,
    Account,
    Configuration,
    Credentials,
    FaultTolerance,
    Identity,
    OAuth2Credentials,
)
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
from ldap3 import SAFE_SYNC, Connection, Server

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    get_pem_format,
    hash_id,
    html_to_text,
    iso_utc,
    retryable,
)

RETRIES = 3
RETRY_INTERVAL = 2

QUEUE_MEM_SIZE = 5 * 1024 * 1024  # Size in Megabytes

OUTLOOK_SERVER = "outlook_server"
OUTLOOK_CLOUD = "outlook_cloud"
API_SCOPE = "https://graph.microsoft.com/.default"
EWS_ENDPOINT = "https://outlook.office365.com/EWS/Exchange.asmx"
TOP = 999

DEFAULT_TIMEZONE = "UTC"

INBOX_MAIL_OBJECT = "Inbox Mails"
SENT_MAIL_OBJECT = "Sent Mails"
JUNK_MAIL_OBJECT = "Junk Mails"
ARCHIVE_MAIL_OBJECT = "Archive Mails"
MAIL_ATTACHMENT = "Mail Attachment"
TASK_ATTACHMENT = "Task Attachment"
CALENDAR_ATTACHMENT = "Calendar Attachment"

SEARCH_FILTER_FOR_NORMAL_USERS = (
    "(&(objectCategory=person)(objectClass=user)(givenName=*))"
)
SEARCH_FILTER_FOR_ADMIN = "(&(objectClass=person)(|(cn=*admin*)(cn=*normal*)))"

MAIL_TYPES = [
    {
        "folder": "inbox",
        "constant": INBOX_MAIL_OBJECT,
    },
    {
        "folder": "sent",
        "constant": SENT_MAIL_OBJECT,
    },
    {
        "folder": "junk",
        "constant": JUNK_MAIL_OBJECT,
    },
    {
        "folder": "archive",
        "constant": ARCHIVE_MAIL_OBJECT,
    },
]

MAIL_FIELDS = [
    "sender",
    "to_recipients",
    "cc_recipients",
    "bcc_recipients",
    "last_modified_time",
    "subject",
    "importance",
    "categories",
    "body",
    "has_attachments",
    "attachments",
]
CONTACT_FIELDS = [
    "email_addresses",
    "phone_numbers",
    "last_modified_time",
    "display_name",
    "company_name",
    "birthday",
]
TASK_FIELDS = [
    "last_modified_time",
    "due_date",
    "complete_date",
    "subject",
    "status",
    "owner",
    "start_date",
    "text_body",
    "companies",
    "categories",
    "importance",
    "has_attachments",
    "attachments",
]
CALENDAR_FIELDS = [
    "required_attendees",
    "type",
    "recurrence",
    "last_modified_time",
    "subject",
    "start",
    "end",
    "location",
    "organizer",
    "body",
    "has_attachments",
    "attachments",
]

END_SIGNAL = "FINISHED"
CERT_FILE = "outlook_cert.cer"


def ews_format_to_datetime(source_datetime, timezone):
    """Change datetime format to user account timezone
    Args:
        datetime: Datetime in UTC format
        timezone: User account timezone
    Returns:
        Datetime: Date format as user account timezone
    """
    if isinstance(source_datetime, exchangelib.ewsdatetime.EWSDateTime) and isinstance(
        timezone, exchangelib.ewsdatetime.EWSTimeZone
    ):
        return (source_datetime.astimezone(timezone)).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif isinstance(source_datetime, exchangelib.ewsdatetime.EWSDate) or isinstance(
        source_datetime, date
    ):
        return source_datetime.strftime("%Y-%m-%d")
    else:
        return source_datetime


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_display_name(user):
    return prefix_identity("name", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_job(job_title):
    return prefix_identity("job_title", job_title)


class TokenFetchFailed(Exception):
    """Exception class to notify that connector was unable to fetch authentication token from Graph API"""

    pass


class UsersFetchFailed(Exception):
    """Exception class to notify that connector was unable to fetch users from Active Directory"""

    pass


class UnauthorizedException(Exception):
    """Exception class unauthorized calls"""

    pass


class Forbidden(Exception):
    pass


class NotFound(Exception):
    pass


class SSLFailed(Exception):
    pass


class ManageCertificate:
    async def store_certificate(self, certificate):
        async with aiofiles.open(CERT_FILE, "w") as file:
            await file.write(certificate)

    def get_certificate_path(self):
        return os.path.join(os.getcwd(), CERT_FILE)

    async def remove_certificate_file(self):
        if os.path.exists(CERT_FILE):
            await remove(CERT_FILE)


class RootCAAdapter(requests.adapters.HTTPAdapter):
    """Class to verify SSL Certificate for Exchange Servers"""

    def cert_verify(self, conn, url, verify, cert):
        try:
            super().cert_verify(
                conn=conn,
                url=url,
                verify=ManageCertificate().get_certificate_path(),
                cert=cert,
            )
        except Exception as exception:
            msg = f"Something went wrong while verifying SSL certificate. Error: {exception}"
            raise SSLFailed(msg) from exception


class ExchangeUsers:
    """Fetch users from Exchange Active Directory"""

    def __init__(
        self, ad_server, domain, exchange_server, user, password, ssl_enabled, ssl_ca
    ):
        self.ad_server = Server(host=ad_server)
        self.domain = domain
        self.exchange_server = exchange_server
        self.user = user
        self.password = password
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca

    @cached_property
    def _create_connection(self):
        return Connection(
            server=self.ad_server,
            user=self.user,
            password=self.password,
            client_strategy=SAFE_SYNC,
            auto_bind=True,  # pyright: ignore
        )

    async def close(self):
        await ManageCertificate().remove_certificate_file()

    def _fetch_normal_users(self, search_query):
        try:
            has_value_for_normal_users, _, response, _ = self._create_connection.search(
                search_query,
                SEARCH_FILTER_FOR_NORMAL_USERS,
                attributes=["mail"],
            )

            if not has_value_for_normal_users:
                msg = "Error while fetching users from Exchange Active Directory."
                raise UsersFetchFailed(msg)

            for user in response:
                yield user

        except Exception as e:
            msg = f"Something went wrong while fetching users. Error: {e}"
            raise UsersFetchFailed(msg) from e

    def _fetch_admin_users(self, search_query):
        try:
            (
                has_value_for_admin_users,
                _,
                response_for_admin,
                _,
            ) = self._create_connection.search(
                search_query,
                SEARCH_FILTER_FOR_ADMIN,
                attributes=["mail"],
            )

            if not has_value_for_admin_users:
                msg = "Error while fetching users from Exchange Active Directory."
                raise UsersFetchFailed(msg)

            for user in response_for_admin:
                yield user
        except Exception as e:
            msg = f"Something went wrong while fetching users. Error: {e}"
            raise UsersFetchFailed(msg) from e

    async def get_users(self):
        ldap_domain_name_list = ["DC=" + domain for domain in self.domain.split(".")]
        search_query = ",".join(ldap_domain_name_list)

        for user in self._fetch_normal_users(search_query=search_query):
            yield user

        for user in self._fetch_admin_users(search_query=search_query):
            yield user

    async def get_user_accounts(self):
        await ManageCertificate().store_certificate(certificate=self.ssl_ca)
        BaseProtocol.HTTP_ADAPTER_CLS = (
            RootCAAdapter if self.ssl_enabled else NoVerifyHTTPAdapter
        )

        credentials = Credentials(
            username=self.user,
            password=self.password,
        )
        configuration = Configuration(
            credentials=credentials,
            server=self.exchange_server,
            retry_policy=FaultTolerance(max_wait=120),
        )

        async for user in self.get_users():
            if "searchResRef" in user["type"]:
                continue

            user_account = Account(
                primary_smtp_address=user.get("attributes", {}).get("mail"),
                config=configuration,
                access_type=IMPERSONATION,
            )
            yield user_account


class Office365Users:
    """Fetch users from Office365 Active Directory"""

    def __init__(self, client_id, client_secret, tenant_id):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    @cached_property
    def _get_session(self):
        return aiohttp.ClientSession(
            trust_env=True,
            raise_for_status=True
        )

    async def close(self):
        await self._get_session.close()
        del self._get_session

    def _check_errors(self, response):
        match response.status:
            case 400:
                msg = "Found invalid tenant id or client id value"
                raise UnauthorizedException(msg)
            case 401:
                msg = "Found invalid client secret value"
                raise UnauthorizedException(msg)
            case 403:
                msg = f"Missing permission or something went wrong. Error: {response}"
                raise Forbidden(msg)
            case 404:
                msg = f"Resource Not Found. Error: {response}"
                raise NotFound(msg)
            case _:
                raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=UnauthorizedException,
    )
    async def _fetch_token(self):
        try:
            async with self._get_session.post(
                url=f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "scope": API_SCOPE,
                },
            ) as response:
                token_response = await response.json()
                return token_response["access_token"]
        except Exception as exception:
            self._check_errors(response=exception)

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_users(self):
        access_token = await self._fetch_token()
        url = f"https://graph.microsoft.com/v1.0/users?$top={TOP}"
        while True:
            try:
                async with self._get_session.get(
                    url=url,
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json",
                    },
                ) as response:
                    json_response = await response.json()
                    yield json_response
                    url = json_response.get("@odata.nextLink")
                    if url is None:
                        break
            except Exception:
                raise

    async def get_user_accounts(self):
        async for users in self.get_users():
            for user in users.get("value", []):
                mail = user.get("mail")
                if mail is None:
                    continue

                credentials = OAuth2Credentials(
                    client_id=self.client_id,
                    tenant_id=self.tenant_id,
                    client_secret=self.client_secret,
                    identity=Identity(primary_smtp_address=mail),
                )
                configuration = Configuration(
                    credentials=credentials,
                    auth_type=OAUTH2,
                    service_endpoint=EWS_ENDPOINT,
                    retry_policy=FaultTolerance(max_wait=120),
                )
                user_account = Account(
                    primary_smtp_address=mail,
                    config=configuration,
                    autodiscover=False,
                    access_type=IMPERSONATION,
                )
                yield user_account


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


class OutlookClient:
    """Outlook client to handle API calls made to Outlook"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.is_cloud = self.configuration["data_source"] == OUTLOOK_CLOUD
        self.ssl_enabled = self.configuration.get("ssl_enabled", False)
        self.certificate = self.configuration.get("ssl_ca", None)

        if self.ssl_enabled and self.certificate:
            self.ssl_ca = get_pem_format(self.certificate)
        else:
            self.ssl_ca = ""

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def _get_user_instance(self):
        if self.is_cloud:
            return Office365Users(
                client_id=self.configuration["client_id"],
                client_secret=self.configuration["client_secret"],
                tenant_id=self.configuration["tenant_id"],
            )

        return ExchangeUsers(
            ad_server=self.configuration["active_directory_server"],
            domain=self.configuration["domain"],
            exchange_server=self.configuration["exchange_server"],
            user=self.configuration["username"],
            password=self.configuration["password"],
            ssl_enabled=self.ssl_enabled,
            ssl_ca=self.ssl_ca,
        )

    async def _fetch_all_users(self):
        self._logger.debug("Fetching all users.")
        async for user in self._get_user_instance.get_users():
            yield user

    async def ping(self):
        await anext(self._get_user_instance.get_users())

    async def get_mails(self, account):
        for mail_type in MAIL_TYPES:
            self._logger.debug(
                f"Fetching {mail_type['folder']} mails for {account.primary_smtp_address}"
            )
            if mail_type["folder"] == "archive":
                # If 'Archive' folder is not present, skipping the iteration
                try:
                    folder_object = (
                        account.root / "Top of Information Store" / "Archive"
                    )
                except Exception:  # noqa S112
                    continue
            else:
                folder_object = getattr(account, mail_type["folder"])

            for mail in await asyncio.to_thread(folder_object.all().only, *MAIL_FIELDS):
                yield mail, mail_type

    async def get_calendars(self, account):
        for calendar in await asyncio.to_thread(
            account.calendar.all().only, *CALENDAR_FIELDS
        ):
            yield calendar

    async def get_child_calendars(self, account):
        for child_calendar in account.calendar.children:
            for calendar in await asyncio.to_thread(
                child_calendar.all().only, *CALENDAR_FIELDS
            ):
                yield calendar, child_calendar

    async def get_tasks(self, account):
        for task in await asyncio.to_thread(account.tasks.all().only, *TASK_FIELDS):
            yield task

    async def get_contacts(self, account):
        folder = account.root / "Top of Information Store" / "Contacts"
        for contact in await asyncio.to_thread(folder.all().only, *CONTACT_FIELDS):
            yield contact


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
            yield self._decorate_with_access_control(
                document, [account.primary_smtp_address]
            ), partial(self.get_content, attachment=copy(attachment), timezone=timezone)

    async def _fetch_mails(self, account, timezone):
        async for mail, mail_type in self.client.get_mails(account=account):
            document = self.doc_formatter.mails_doc_formatter(
                mail=mail,
                mail_type=mail_type,
                timezone=timezone,
            )
            yield self._decorate_with_access_control(
                document, [account.primary_smtp_address]
            ), None

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
            yield self._decorate_with_access_control(
                document, [account.primary_smtp_address]
            ), None

    async def _fetch_tasks(self, account, timezone):
        self._logger.debug(f"Fetching tasks for {account.primary_smtp_address}")
        async for task in self.client.get_tasks(account=account):
            document = self.doc_formatter.task_doc_formatter(
                task=task, timezone=timezone
            )
            yield self._decorate_with_access_control(
                document, [account.primary_smtp_address]
            ), None

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
        yield self._decorate_with_access_control(
            document, [account.primary_smtp_address]
        ), None

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
