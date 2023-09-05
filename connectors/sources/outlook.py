#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft Outlook source module is responsible to fetch documents from Outlook server or cloud platforms.
"""
import os
from copy import copy
from datetime import date
from functools import cached_property, partial

import aiofiles
import aiohttp
import exchangelib
import pytz
import requests.adapters
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

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    ConcurrentTasks,
    MemQueue,
    RetryStrategy,
    get_base64_value,
    get_pem_format,
    html_to_text,
    retryable,
)

RETRIES = 3
RETRY_INTERVAL = 2
FILE_SIZE_LIMIT = 10485760

QUEUE_MEM_SIZE = 5 * 1024 * 1024  # Size in Megabytes
MAX_CONCURRENCY = 10

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


def ews_format_to_datetime(source_datetime, timezone):
    """Change datetime format to user account timezone
    Args:
        datetime: Datetime in UTC format
        timezone: User account timezone
    Returns:
        Datetime: Date format as user account timezone
    """
    if isinstance(source_datetime, exchangelib.ewsdatetime.EWSDateTime):
        return (source_datetime.astimezone(pytz.timezone(str(timezone)))).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    elif isinstance(source_datetime, exchangelib.ewsdatetime.EWSDate) or isinstance(
        source_datetime, date
    ):
        return source_datetime.strftime("%Y-%m-%d")
    else:
        return source_datetime


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
    cert_file = "outlook_cert.cer"

    async def store_certificate(self, certificate):
        async with aiofiles.open(self.cert_file, "w") as file:
            await file.write(certificate)

    def get_certificate_path(self):
        return os.path.join(os.getcwd(), self.cert_file)

    def remove_certificate_file(self):
        if os.path.exists(self.cert_file):
            os.remove(self.cert_file)


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
            raise SSLFailed(
                f"Something went wrong while verifying SSL certificate. Error: {exception}"
            ) from exception


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
        ManageCertificate().remove_certificate_file()

    def _fetch_normal_users(self, search_query):
        try:
            has_value_for_normal_users, _, response, _ = self._create_connection.search(
                search_query,
                SEARCH_FILTER_FOR_NORMAL_USERS,
                attributes=["mail"],
            )

            if not has_value_for_normal_users:
                raise UsersFetchFailed(
                    "Error while fetching users from Exchange Active Directory."
                )

            for user in response:
                yield user

        except Exception as e:
            raise UsersFetchFailed(
                f"Something went wrong while fetching users. Error: {e}"
            ) from e

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
                raise UsersFetchFailed(
                    "Error while fetching users from Exchange Active Directory."
                )

            for user in response_for_admin:
                yield user
        except Exception as e:
            raise UsersFetchFailed(
                f"Something went wrong while fetching users. Error: {e}"
            ) from e

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
        return aiohttp.ClientSession(raise_for_status=True)

    async def close(self):
        await self._get_session.close()
        del self._get_session

    def _check_errors(self, response):
        match response.status:
            case 400:
                raise UnauthorizedException("Found invalid tenant id value")
            case 401:
                raise UnauthorizedException(
                    "Found invalid client id or client secret value"
                )
            case 403:
                raise Forbidden(
                    f"Missing permission or something went wrong. Error: {response}"
                )
            case 404:
                raise NotFound(f"Resource Not Found. Error: {response}")
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
            "categories": [category for category in (mail.categories or [])],
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
            "categories": [category for category in (task.categories or [])],
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
                for number in (contact.phone_numbers)
                if number.phone_number
            ],
            "company_name": contact.company_name,
            "birthday": ews_format_to_datetime(
                source_datetime=contact.birthday, timezone=timezone
            ),
        }

    def attachment_doc_formatter(self, attachment, timezone):
        return {
            "_id": attachment.attachment_id.id,
            "title": attachment.name,
            "type": "attachment",
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

        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]

        if self.ssl_enabled and self.certificate:
            ssl_ca = get_pem_format(self.certificate)
        else:
            ssl_ca = ""

        return ExchangeUsers(
            ad_server=self.configuration["active_directory_server"],
            domain=self.configuration["domain"],
            exchange_server=self.configuration["exchange_server"],
            user=self.configuration["username"],
            password=self.configuration["password"],
            ssl_enabled=self.ssl_enabled,
            ssl_ca=ssl_ca,
        )

    async def ping(self):
        await anext(self._get_user_instance.get_users())

    async def get_mails(self, account):
        for mail_type in MAIL_TYPES:
            if mail_type["folder"] == "archive":
                # If 'Archive' folder is not present, skipping the iteration
                try:
                    folder_object = (
                        account.root / "Top of Information Store" / "Archive"
                    )
                except Exception:
                    continue
            else:
                folder_object = getattr(account, mail_type["folder"])

            for mail in folder_object.all().only(*MAIL_FIELDS):
                yield mail, mail_type

    async def get_calendars(self, account):
        for calendar in account.calendar.all().only(*CALENDAR_FIELDS):
            yield calendar

    async def get_child_calendars(self, account):
        for child_calendar in account.calendar.children:
            for calendar in child_calendar.all().only(*CALENDAR_FIELDS):
                yield calendar, child_calendar

    async def get_tasks(self, account):
        for task in account.tasks.all().only(*TASK_FIELDS):
            yield task

    async def get_contacts(self, account):
        folder = account.root / "Top of Information Store" / "Contacts"
        for contact in folder.all().only(*CONTACT_FIELDS):
            if isinstance(contact, exchangelib.items.contact.Contact):
                yield contact


class OutlookDataSource(BaseDataSource):
    """Outlook"""

    name = "Outlook"
    service_type = "outlook"

    def __init__(self, configuration):
        """Setup the connection to the Outlook

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
            logger_ (DocumentLogger): Object of DocumentLogger class.
        """
        super().__init__(configuration=configuration)
        self.configuration = configuration
        self.doc_formatter = OutlookDocFormatter()

        self.tasks = 0
        self.queue = MemQueue(maxmemsize=QUEUE_MEM_SIZE, refresh_timeout=120)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)

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
                "value": "",
            },
            "client_id": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_CLOUD}],
                "label": "Client ID",
                "order": 3,
                "type": "str",
                "value": "",
            },
            "client_secret": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_CLOUD}],
                "label": "Client Secret Value",
                "order": 4,
                "sensitive": True,
                "type": "str",
                "value": "",
            },
            "exchange_server": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_SERVER}],
                "label": "Exchange Server",
                "order": 5,
                "tooltip": "Exchange server's IP address. E.g. 127.0.0.1",
                "type": "str",
                "value": "",
            },
            "active_directory_server": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_SERVER}],
                "label": "Active Directory Server",
                "order": 6,
                "tooltip": "Active Directory server's IP address. E.g. 127.0.0.1",
                "type": "str",
                "value": "",
            },
            "username": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_SERVER}],
                "label": "Exchange server username",
                "order": 7,
                "type": "str",
                "value": "",
            },
            "password": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_SERVER}],
                "label": "Exchange server password",
                "order": 8,
                "sensitive": True,
                "type": "str",
                "value": "",
            },
            "domain": {
                "depends_on": [{"field": "data_source", "value": OUTLOOK_SERVER}],
                "label": "Exchange server domain name",
                "order": 9,
                "tooltip": "Domain name such as gmail.com, outlook.com",
                "type": "str",
                "value": "",
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
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 11,
                "type": "str",
                "value": "",
            },
        }

    async def close(self):
        await self.client._get_user_instance.close()

    def _pre_checks_for_get_content(
        self, attachment_extension, attachment_name, attachment_size
    ):
        if attachment_extension == "":
            self._logger.debug(
                f"Files without extension are not supported, skipping {attachment_name}."
            )
            return

        if attachment_extension.lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.debug(
                f"Files with the extension {attachment_extension} are not supported, skipping {attachment_name}."
            )
            return

        if attachment_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return
        return True

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
        attachment_size = attachment.size
        if not (doit and attachment_size > 0):
            return

        attachment_name = attachment.name
        attachment_extension = (
            attachment_name[attachment_name.rfind(".") :]  # noqa
            if "." in attachment_name
            else ""
        )

        if not self._pre_checks_for_get_content(
            attachment_extension=attachment_extension,
            attachment_name=attachment_name,
            attachment_size=attachment_size,
        ):
            return

        self._logger.debug(f"Downloading {attachment_name}")

        document = {
            "_id": attachment.attachment_id.id,
            "_timestamp": ews_format_to_datetime(
                source_datetime=attachment.last_modified_time, timezone=timezone
            ),
            "_attachment": get_base64_value(attachment.content),
        }

        return document

    async def _fetch_attachments(self, outlook_object, timezone):
        for attachment in outlook_object.attachments:
            await self.queue.put(
                (
                    self.doc_formatter.attachment_doc_formatter(
                        attachment=attachment, timezone=timezone
                    ),
                    partial(
                        self.get_content, attachment=copy(attachment), timezone=timezone
                    ),
                )
            )

        await self.queue.put(END_SIGNAL)

    async def _fetch_mails(self, account, timezone):
        async for mail, mail_type in self.client.get_mails(account=account):
            await self.queue.put(
                (
                    self.doc_formatter.mails_doc_formatter(
                        mail=mail,
                        mail_type=mail_type,
                        timezone=timezone,
                    ),
                    None,
                )
            )

            if mail.has_attachments:
                await self.fetchers.put(
                    partial(
                        self._fetch_attachments, outlook_object=mail, timezone=timezone
                    )
                )
                self.tasks += 1

        await self.queue.put(END_SIGNAL)

    async def _fetch_contacts(self, account, timezone):
        async for contact in self.client.get_contacts(account=account):
            await self.queue.put(
                (
                    self.doc_formatter.contact_doc_formatter(
                        contact=contact,
                        timezone=timezone,
                    ),
                    None,
                )
            )
        await self.queue.put(END_SIGNAL)

    async def _fetch_tasks(self, account, timezone):
        async for task in self.client.get_tasks(account=account):
            await self.queue.put(
                (
                    self.doc_formatter.task_doc_formatter(task=task, timezone=timezone),
                    None,
                )
            )

            if task.has_attachments:
                await self.fetchers.put(
                    partial(
                        self._fetch_attachments, outlook_object=task, timezone=timezone
                    )
                )
                self.tasks += 1

        await self.queue.put(END_SIGNAL)

    async def _fetch_calendars(self, account, timezone):
        async for calendar in self.client.get_calendars(account=account):
            await self._enqueue_calendars(
                calendar=calendar, child_calendar=calendar, timezone=timezone
            )

        await self.queue.put(END_SIGNAL)

    async def _fetch_child_calendars(self, account, timezone):
        async for calendar, child_calendar in self.client.get_child_calendars(
            account=account
        ):
            await self._enqueue_calendars(
                calendar=calendar, child_calendar=child_calendar, timezone=timezone
            )

        await self.queue.put(END_SIGNAL)

    async def _enqueue_calendars(self, calendar, child_calendar, timezone):
        await self.queue.put(
            (
                self.doc_formatter.calendar_doc_formatter(
                    calendar=calendar,
                    child_calendar=str(child_calendar),
                    timezone=timezone,
                ),
                None,
            )
        )

        if calendar.has_attachments:
            await self.fetchers.put(
                partial(
                    self._fetch_attachments, outlook_object=calendar, timezone=timezone
                )
            )
            self.tasks += 1

    async def ping(self):
        """Verify the connection with Outlook"""
        await self.client.ping()
        self._logger.info("Successfully connected to Outlook")

    async def _consumer(self):
        """Async generator to process entries of the queue

        Yields:
            dictionary: Documents from Outlook.
        """
        while self.tasks > 0:
            _, item = await self.queue.get()
            if item == END_SIGNAL:
                self.tasks -= 1
            else:
                yield item

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch outlook objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """
        async for account in self.client._get_user_instance.get_user_accounts():
            timezone = account.default_timezone or DEFAULT_TIMEZONE

            await self.fetchers.put(
                partial(self._fetch_mails, account=account, timezone=timezone)
            )

            await self.fetchers.put(
                partial(self._fetch_contacts, account=account, timezone=timezone)
            )

            await self.fetchers.put(
                partial(self._fetch_tasks, account=account, timezone=timezone)
            )

            await self.fetchers.put(
                partial(self._fetch_calendars, account=account, timezone=timezone)
            )

            await self.fetchers.put(
                partial(self._fetch_child_calendars, account=account, timezone=timezone)
            )

            # Adding 5 tasks for fetching mails, contacts, tasks, calendars and child calendars
            self.tasks += 5

        async for item in self._consumer():
            yield item

        await self.fetchers.join()
