#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Outlook source class methods"""

import asyncio
import ssl
from contextlib import asynccontextmanager
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests.adapters
from aiohttp import StreamReader
from exchangelib import UTC
from exchangelib.attachments import FileAttachment, ItemAttachment
from exchangelib.errors import (
    ErrorAccessDenied,
    ErrorFolderNotFound,
    ErrorManagedFolderNotFound,
    ErrorNonExistentMailbox,
    TransportError,
)
from exchangelib.folders import Calendar, Messages, Tasks
from exchangelib.items import (
    CalendarItem,
    Contact,
    DistributionList,
    MeetingCancellation,
    MeetingRequest,
    MeetingResponse,
    Message,
    Task,
)
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter

from connectors.source import ConfigurableFieldValueError
from connectors.sources.outlook import (
    CALENDAR_ITEM_TYPES,
    INBOX_MAIL_OBJECT,
    MAIL_ATTACHMENT,
    MAIL_ITEM_TYPES,
    OUTLOOK_CLOUD,
    OUTLOOK_SERVER,
    TASK_ITEM_TYPES,
    ExchangeUsers,
    Forbidden,
    InMemoryCAAdapter,
    NotFound,
    OutlookDataSource,
    OutlookDocFormatter,
    SSLCertificateError,
    UnauthorizedException,
    UsersFetchFailed,
)
from connectors.utils import get_pem_format
from tests.commons import AsyncIterator
from tests.sources.support import create_source

RESPONSE_CONTENT = bytes("# This is the dummy file", "utf-8")
EXPECTED_CONTENT = {
    "_id": "attachment_id_1",
    "_timestamp": "2023-12-12T01:01:01Z",
    "_attachment": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl",
}
EXPECTED_CONTENT_EXTRACTED = {
    "_id": "attachment_id_1",
    "_timestamp": "2023-12-12T01:01:01Z",
    "body": str(RESPONSE_CONTENT),
}

TIMEZONE = "Asia/Kolkata"
MAIL = "mail"
TASK = "task"
CALENDAR = "calendar"
CONTACT = "contact"

EXPECTED_RESPONSE = [
    {
        "_id": "mail_1",
        "_timestamp": "2023-12-12T01:01:01Z",
        "title": "Dummy Subject",
        "type": "Inbox Mails",
        "sender": "dummy.user@gmail.com",
        "to_recipients": ["dummy.user@gmail.com"],
        "cc_recipients": ["dummy.user@gmail.com"],
        "bcc_recipients": ["dummy.user@gmail.com"],
        "importance": "High",
        "categories": ["Outlook"],
        "message": "This is a dummy mail",
    },
    {
        "_id": "mail_1",
        "_timestamp": "2023-12-12T01:01:01Z",
        "title": "Dummy Subject",
        "type": "Sent Mails",
        "sender": "dummy.user@gmail.com",
        "to_recipients": ["dummy.user@gmail.com"],
        "cc_recipients": ["dummy.user@gmail.com"],
        "bcc_recipients": ["dummy.user@gmail.com"],
        "importance": "High",
        "categories": ["Outlook"],
        "message": "This is a dummy mail",
    },
    {
        "_id": "mail_1",
        "_timestamp": "2023-12-12T01:01:01Z",
        "title": "Dummy Subject",
        "type": "Junk Mails",
        "sender": "dummy.user@gmail.com",
        "to_recipients": ["dummy.user@gmail.com"],
        "cc_recipients": ["dummy.user@gmail.com"],
        "bcc_recipients": ["dummy.user@gmail.com"],
        "importance": "High",
        "categories": ["Outlook"],
        "message": "This is a dummy mail",
    },
    {
        "_id": "mail_1",
        "_timestamp": "2023-12-12T01:01:01Z",
        "title": "Dummy Subject",
        "type": "Archive Mails",
        "sender": "dummy.user@gmail.com",
        "to_recipients": ["dummy.user@gmail.com"],
        "cc_recipients": ["dummy.user@gmail.com"],
        "bcc_recipients": ["dummy.user@gmail.com"],
        "importance": "High",
        "categories": ["Outlook"],
        "message": "This is a dummy mail",
    },
    {
        "_id": "task_1",
        "_timestamp": "2023-12-12T01:01:01Z",
        "type": "Task",
        "title": "Create Test cases for Outlook",
        "owner": "Test User",
        "start_date": "2023-12-12T01:01:01Z",
        "due_date": "2023-12-12T01:01:01Z",
        "complete_date": "2023-12-12T01:01:01Z",
        "categories": ["Outlook"],
        "importance": "High",
        "content": "This is a dummy task",
        "status": "completed",
    },
    {
        "_id": "calendar_1",
        "_timestamp": "2023-12-12T01:01:01Z",
        "type": "Calendar",
        "title": "TC Review meeting",
        "meeting_type": "Single",
        "organizer": "dummy.user@gmail.com",
        "attendees": ["dummy.user@gmail.com"],
        "start_date": "2023-12-12T01:01:01Z",
        "end_date": "2023-12-12T01:01:01Z",
        "location": "USA",
        "content": "This is a dummy calendar",
    },
    {
        "_id": "contact_1",
        "type": "Contact",
        "_timestamp": "2023-12-12T01:01:01Z",
        "name": "Dummy User",
        "email_addresses": ["dummy.user@gmail.com"],
        "contact_numbers": [99887776655],
        "company_name": "ABC",
        "birthday": "2023-12-12T01:01:01Z",
    },
    {
        "_id": "attachment_id_1",
        "title": "sync.txt",
        "type": "Mail Attachment",
        "_timestamp": "2023-12-12T01:01:01Z",
        "size": 1221,
    },
    {
        "_id": "attachment_id_1",
        "title": "sync.txt",
        "type": "Task Attachment",
        "_timestamp": "2023-12-12T01:01:01Z",
        "size": 1221,
    },
    {
        "_id": "attachment_id_1",
        "title": "sync.txt",
        "type": "Calendar Attachment",
        "_timestamp": "2023-12-12T01:01:01Z",
        "size": 1221,
    },
]


class MockException(Exception):
    def __init__(self, status, message=None):
        super().__init__(message)
        self.status = status


class MockMsgFolderRoot:
    """Mocks account.msg_folder_root, under which "Archive" is resolved by name."""

    def __truediv__(self, path):
        if path == "Archive":
            # A real mail Archive is a Messages folder (the client now verifies this).
            return typed_folder(Messages, MAIL)
        msg = "Unsupported path element"
        raise ValueError(msg)


class MockAttachmentId:
    def __init__(self, id):  # noqa
        self.id = id


def build_mail_document():
    # Spec against Message so the mock passes the isinstance guard in
    # _fetch_mails while still exposing plain attribute values.
    sender = MagicMock()
    sender.email_address = "dummy.user@gmail.com"

    mail = MagicMock(spec=Message)
    mail.id = "mail_1"
    mail.last_modified_time = "2023-12-12T01:01:01Z"
    mail.subject = "Dummy Subject"
    mail.to_recipients = [sender]
    mail.cc_recipients = [sender]
    mail.bcc_recipients = [sender]
    mail.sender = sender
    mail.importance = "High"
    mail.categories = ["Outlook"]
    mail.body = "This is a dummy mail"
    mail.has_attachments = True
    mail.attachments = [MOCK_ATTACHMENT]
    return mail


def build_task_document():
    # Spec against Task so the mock passes the isinstance guard in _fetch_tasks
    # while still exposing plain attribute values.
    task = MagicMock(spec=Task)
    task.id = "task_1"
    task.last_modified_time = "2023-12-12T01:01:01Z"
    task.subject = "Create Test cases for Outlook"
    task.owner = "Test User"
    task.start_date = "2023-12-12T01:01:01Z"
    task.due_date = "2023-12-12T01:01:01Z"
    task.complete_date = "2023-12-12T01:01:01Z"
    task.importance = "High"
    task.text_body = "This is a dummy task"
    task.status = "completed"
    task.categories = ["Outlook"]
    task.has_attachments = True
    task.attachments = [MOCK_ATTACHMENT]
    return task


def ContactDocument():
    # A real Contact instance so _fetch_contacts' isinstance dispatch routes it
    # to the contact formatter (spec mock keeps isinstance(..., Contact) True).
    contact = MagicMock()
    contact.email = "dummy.user@gmail.com"
    contact.phone_number = 99887776655

    document = MagicMock(spec=Contact)
    document.id = "contact_1"
    document.last_modified_time = "2023-12-12T01:01:01Z"
    document.display_name = "Dummy User"
    document.email_addresses = [contact]
    document.phone_numbers = [contact]
    document.company_name = "ABC"
    document.birthday = "2023-12-12T01:01:01Z"
    return document


class DistributionListDocument:
    """Mimics a DistributionList (contact group) item that Exchange can return
    from the Contacts folder. It carries the shared item fields plus its members,
    not the per-contact fields (email_addresses, phone_numbers, company_name,
    birthday)."""

    def __init__(self):
        member = MagicMock()
        member.mailbox.email_address = "group.member@gmail.com"

        self.id = "distribution_list_1"
        self.last_modified_time = "2023-12-12T01:01:01Z"
        self.display_name = "Dummy Group"
        self.members = [member]


def build_calendar_document():
    # Spec against CalendarItem so the mock passes the isinstance guard in
    # _enqueue_calendars while still exposing plain attribute values.
    organizer = MagicMock()
    organizer.email_address = "dummy.user@gmail.com"
    organizer.mailbox.email_address = "dummy.user@gmail.com"

    calendar = MagicMock(spec=CalendarItem)
    calendar.id = "calendar_1"
    calendar.organizer = organizer
    calendar.last_modified_time = "2023-12-12T01:01:01Z"
    calendar.subject = "TC Review meeting"
    calendar.type = "Single"
    calendar.start = "2023-12-12T01:01:01Z"
    calendar.end = "2023-12-12T01:01:01Z"
    calendar.location = "USA"
    calendar.required_attendees = [organizer]
    calendar.body = "This is a dummy calendar"
    calendar.has_attachments = True
    calendar.attachments = [MOCK_ATTACHMENT]
    return calendar


class AllObjects:
    def __init__(self, object_type):
        self.object_type = object_type

    def only(self, *args):
        match self.object_type:
            case "mail":
                return [build_mail_document()]
            case "task":
                return [build_task_document()]
            case "contact":
                return [ContactDocument()]
            case "calendar":
                return [build_calendar_document()]


class MockOutlookObject:
    def __init__(self, object_type):
        self.object_type = object_type
        self.children = [self]

    def all(self):  # noqa
        return AllObjects(object_type=self.object_type)


def typed_folder(folder_cls, object_type):
    """A folder mock that passes isinstance(folder_cls) (as the client now checks)
    while keeping MockOutlookObject's .all().only() dispatch."""
    folder = MagicMock()
    folder.__class__ = folder_cls
    folder.object_type = object_type
    folder.all.return_value = AllObjects(object_type=object_type)
    return folder


class MockAccount:
    def __init__(self):
        self.default_timezone = "UTC"

        self.inbox = MockOutlookObject(object_type=MAIL)
        self.sent = MockOutlookObject(object_type=MAIL)
        self.junk = MockOutlookObject(object_type=MAIL)
        self.tasks = MockOutlookObject(object_type=TASK)
        self.calendar = MockOutlookObject(object_type=CALENDAR)
        # Child folders under Calendar are real Calendar folders (the client only
        # descends into folders it identifies as calendars).
        self.calendar.children = [typed_folder(Calendar, CALENDAR)]
        # Accessed via distinguished folder IDs.
        self.contacts = MockOutlookObject(object_type=CONTACT)
        self.msg_folder_root = MockMsgFolderRoot()
        self.primary_smtp_address = "alex.wilber@gmail.com"


def build_attachment(attachment_id, name, size, last_modified_time, content):
    # Spec against FileAttachment so it passes the isinstance guard in get_content.
    attachment = MagicMock(spec=FileAttachment)
    attachment.attachment_id = attachment_id
    attachment.name = name
    attachment.size = size
    attachment.last_modified_time = last_modified_time
    attachment.content = content
    return attachment


MOCK_ATTACHMENT = build_attachment(
    attachment_id=MockAttachmentId(
        id="attachment_id_1",
    ),
    name="sync.txt",
    size=1221,
    last_modified_time="2023-12-12T01:01:01Z",
    content=RESPONSE_CONTENT,
)

MOCK_ATTACHMENT_WITHOUT_EXTENSION = build_attachment(
    attachment_id=MockAttachmentId(
        id="attachment_id_2",
    ),
    name="sync",
    size=200,
    last_modified_time="2023-12-12T01:01:01Z",
    content=RESPONSE_CONTENT,
)

MOCK_ATTACHMENT_WITH_SIZE_ZERO = build_attachment(
    attachment_id=MockAttachmentId(
        id="attachment_id_3",
    ),
    name="test.py",
    size=0,
    last_modified_time="2023-12-12T01:01:01Z",
    content=RESPONSE_CONTENT,
)

MOCK_ATTACHMENT_WITH_LARGER_SIZE = build_attachment(
    attachment_id=MockAttachmentId(
        id="attachment_id_3",
    ),
    name="test.py",
    size=23000000,
    last_modified_time="2023-12-12T01:01:01Z",
    content=RESPONSE_CONTENT,
)

MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION = build_attachment(
    attachment_id=MockAttachmentId(
        id="attachment_id_3",
    ),
    name="test.png",
    size=23000000,
    last_modified_time="2023-12-12T01:01:01Z",
    content=RESPONSE_CONTENT,
)


class JSONAsyncMock(AsyncMock):
    def __init__(self, json, status, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._json = json
        self.status = status

    async def json(self):
        return self._json


class StreamReaderAsyncMock(AsyncMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content = StreamReader


@asynccontextmanager
async def create_outlook_source(
    data_source=OUTLOOK_CLOUD,
    tenant_id="foo",
    client_id="bar",
    client_secret="faa",
    exchange_server="127.0.0.1",
    active_directory_server="127.0.0.1",
    username="fee",
    password="fuu",
    domain="outlook.com",
    ssl_enabled=False,
    ssl_ca="",
    use_text_extraction_service=False,
):
    async with create_source(
        OutlookDataSource,
        data_source=data_source,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        exchange_server=exchange_server,
        active_directory_server=active_directory_server,
        username=username,
        password=password,
        domain=domain,
        ssl_enabled=ssl_enabled,
        ssl_ca=ssl_ca,
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source


def get_json_mock(mock_response, status):
    async_mock = AsyncMock()
    async_mock.__aenter__ = AsyncMock(
        return_value=JSONAsyncMock(json=mock_response, status=status)
    )
    return async_mock


def get_stream_reader():
    async_mock = AsyncMock()
    async_mock.__aenter__ = AsyncMock(return_value=StreamReaderAsyncMock())
    return async_mock


def side_effect_function(url, headers):
    """Dynamically changing return values for API calls
    Args:
        url, ssl: Params required for get call
    """
    if (
        url
        == "https://graph.microsoft.com/v1.0/users?$top=999&$filter=accountEnabled%20eq%20true"
    ):
        return get_json_mock(
            mock_response={
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/users?$top=999&$skipToken=fake-skip-token",
                "value": [{"mail": "test.user@gmail.com"}],
            },
            status=200,
        )
    elif (
        url
        == "https://graph.microsoft.com/v1.0/users?$top=999&$skipToken=fake-skip-token"
    ):
        return get_json_mock(
            mock_response={"value": [{"mail": "dummy.user@gmail.com"}]}, status=200
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extras",
    [
        (
            # Outlook Server with blank dependent fields
            {
                "data_source": OUTLOOK_SERVER,
                "username": "foo",
                "password": "bar",
                "exchange_server": "",
                "active_directory_server": "",
                "domain": "",
            }
        ),
        (
            # Outlook Cloud with blank dependent fields
            {
                "data_source": OUTLOOK_CLOUD,
                "tenant_id": "foo",
                "client_id": "bar",
                "client_secret": "",
            }
        ),
    ],
)
async def test_validate_configuration_with_invalid_dependency_fields_raises_error(
    extras,
):
    # Setup
    async with create_outlook_source(**extras) as source:
        # Execute
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extras",
    [
        (
            # Outlook Server with blank non-dependent fields
            {
                "data_source": OUTLOOK_SERVER,
                "username": "foo.bar@gmail.com",
                "password": "abc@123",
                "exchange_server": "127.0.0.1",
                "domain": "gmail.com",
                "active_directory_server": "127.0.0.1",
                "ssl_enabled": False,
                "ssl_ca": "",
            }
        ),
        (
            # Outlook Cloud with blank dependent fields
            {
                "data_source": OUTLOOK_CLOUD,
                "tenant_id": "foo",
                "client_id": "bar",
                "client_secret": "foo.bar",
            }
        ),
    ],
)
async def test_validate_config_with_valid_dependency_fields_does_not_raise_error(
    extras,
):
    async with create_outlook_source(**extras) as source:
        await source.validate_config()


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Connection")
async def test_ping_for_server(mock_connection):
    mock_connection_instance = mock_connection.return_value
    mock_connection_instance.search.return_value = (
        True,
        None,
        [{"mail": "alex.wilber@gmail.com"}, {"mail": "alex.kite@gmail.com"}],
        None,
    )

    async with create_outlook_source() as source:
        source.client.is_cloud = False
        await source.ping()


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Connection")
async def test_ping_for_server_for_failed_connection(mock_connection):
    mock_connection_instance = mock_connection.return_value
    mock_connection_instance.search.return_value = (
        False,
        None,
        [],
        None,
    )

    async with create_outlook_source() as source:
        source.client.is_cloud = False
        with pytest.raises(UsersFetchFailed):
            await source.ping()


@pytest.mark.asyncio
async def test_ping_for_cloud():
    async with create_outlook_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=get_json_mock(
                mock_response={"access_token": "fake-token"}, status=200
            ),
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                side_effect=side_effect_function,
            ):
                await source.ping()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "side_effect_exception, raised_exception",
    [
        (MockException(status=400), UnauthorizedException),
        (MockException(status=401), UnauthorizedException),
        (MockException(status=403), Forbidden),
        (MockException(status=404), NotFound),
    ],
)
@mock.patch("connectors.utils.time_to_sleep_between_retries")
async def test_ping_for_cloud_for_failed_connection(
    mock_time_to_sleep_between_retries, raised_exception, side_effect_exception
):
    mock_time_to_sleep_between_retries.return_value = 0
    async with create_outlook_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            side_effect=side_effect_exception,
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                return_value=AsyncMock(),
            ):
                with pytest.raises(raised_exception):
                    await source.ping()


@pytest.mark.asyncio
async def test_get_users_for_cloud():
    async with create_outlook_source() as source:
        users = []
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=get_json_mock(
                mock_response={"access_token": "fake-token"}, status=200
            ),
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                side_effect=side_effect_function,
            ):
                async for response in source.client._get_user_instance.get_users():
                    user_mails = [user["mail"] for user in response["value"]]
                    users.extend(user_mails)
                assert users == ["test.user@gmail.com", "dummy.user@gmail.com"]


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Connection")
async def test_fetch_admin_users_negative(mock_connection):
    async with create_outlook_source() as source:
        mock_connection_instance = mock_connection.return_value
        mock_connection_instance.search.return_value = (
            False,
            None,
            [],
            None,
        )
        source.client.is_cloud = False
        with pytest.raises(Exception):
            for _response in source.client._get_user_instance._fetch_admin_users(
                search_query="search_query"
            ):
                pass


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Connection")
async def test_fetch_admin_users(mock_connection):
    async with create_outlook_source() as source:
        users = []
        mock_connection_instance = mock_connection.return_value
        mock_connection_instance.search.return_value = (
            True,
            None,
            ["test.user@gmail.com", "dummy.user@gmail.com"],
            None,
        )
        source.client.is_cloud = False
        for response in source.client._get_user_instance._fetch_admin_users(
            search_query="search_query"
        ):
            users.append(response)
        assert users == ["test.user@gmail.com", "dummy.user@gmail.com"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attachment, expected_content",
    [
        (MOCK_ATTACHMENT, EXPECTED_CONTENT),
        (MOCK_ATTACHMENT_WITHOUT_EXTENSION, None),
        (MOCK_ATTACHMENT_WITH_SIZE_ZERO, None),
        (MOCK_ATTACHMENT_WITH_LARGER_SIZE, None),
        (MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION, None),
    ],
)
async def test_get_content(attachment, expected_content):
    async with create_outlook_source() as source:
        response = await source.get_content(
            attachment=attachment,
            timezone=TIMEZONE,
            doit=True,
        )
        assert response == expected_content


@pytest.mark.asyncio
async def test_get_content_with_extraction_service():
    with (
        patch(
            "connectors.content_extraction.ContentExtraction.extract_text",
            return_value=str(RESPONSE_CONTENT),
        ),
        patch(
            "connectors.content_extraction.ContentExtraction.get_extraction_config",
            return_value={"host": "http://localhost:8090"},
        ),
    ):
        async with create_outlook_source(use_text_extraction_service=True) as source:
            response = await source.get_content(
                attachment=MOCK_ATTACHMENT,
                timezone=TIMEZONE,
                doit=True,
            )
            assert response == EXPECTED_CONTENT_EXTRACTED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "is_cloud, user_response",
    [
        (True, {"value": [{"mail": "dummy.user@gmail.com"}]}),
        (False, {"type": "user", "attributes": {"mail": ["account@example.com"]}}),
    ],
)
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_get_user_accounts_for_cloud(
    account, is_cloud, user_response, reset_http_adapter_cls
):
    async with create_outlook_source() as source:
        source.client.is_cloud = is_cloud
        source.client._get_user_instance.get_users = AsyncIterator([user_response])

        async for (
            source_account
        ) in source.client._get_user_instance.get_user_accounts():
            assert source_account == "account"


@pytest.mark.asyncio
@patch("connectors.sources.outlook.logger.warning")
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_exchange_get_user_accounts_skips_empty_mail(
    mock_account, mock_warning, reset_http_adapter_cls
):
    valid_user = {
        "type": "user",
        "attributes": {"mail": ["valid.user@example.com"]},
    }
    user_without_mail = {
        "type": "user",
        "dn": "CN=NoMail,CN=Users,DC=example,DC=local",
        "attributes": {"mail": []},
    }
    async with create_outlook_source() as source:
        source.client.is_cloud = False
        source.client._get_user_instance.get_users = AsyncIterator(
            [user_without_mail, valid_user]
        )

        accounts = [
            account
            async for account in source.client._get_user_instance.get_user_accounts()
        ]

        assert accounts == ["account"]
        mock_account.assert_called_once()
        assert mock_account.call_args.kwargs["primary_smtp_address"] == (
            "valid.user@example.com"
        )
        mock_warning.assert_called_once()
        warning_message = mock_warning.call_args.args[0]
        assert "Skipping Active Directory user without a valid mail attribute" in (
            warning_message
        )
        assert "CN=NoMail,CN=Users,DC=example,DC=local" in warning_message


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_exchange_get_user_accounts_normalizes_ldap_mail_list(
    mock_account, reset_http_adapter_cls
):
    user = {
        "type": "user",
        "attributes": {"mail": ["user@example.com"]},
    }
    async with create_outlook_source() as source:
        source.client.is_cloud = False
        source.client._get_user_instance.get_users = AsyncIterator([user])

        accounts = [
            account
            async for account in source.client._get_user_instance.get_user_accounts()
        ]

        assert accounts == ["account"]
        mock_account.assert_called_once_with(
            primary_smtp_address="user@example.com",
            config=mock_account.call_args.kwargs["config"],
            access_type=mock_account.call_args.kwargs["access_type"],
        )


# Real self-signed certificate in the single-line form the connector receives.
# load_verify_locations ignores validity dates, so it never expires for tests.
VALID_SSL_CERTIFICATE = (
    "-----BEGIN CERTIFICATE----- "
    "MIICsjCCAZqgAwIBAgIUDznyN9v5Tk8muCxnL/Z2EFwtcBwwDQYJKoZIhvcNAQELBQAwEjEQMA4GA1UEAwwHdGVzdC1jYTAgFw0wMDAxMDEwMDAwMDBaGA8yMDk5MDEwMTAwMDAwMFowEjEQMA4GA1UEAwwHdGVzdC1jYTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKmQAqm3+hDpc9+OzTjhY4W/AASWa41qyeuKNL+K8kA6oh9TmT20YhPikxPzQCUxp/prm9pi9eym5VLh2GhNCCE8LR+TsrwZr2MpYGZph1Y/y4U5PVNZCOboCee44F/6f8huYtHRPSrOC1OHehvMwdfAC63MueN6oBtxIIOwktxlkuBbK5wY97QlY/utxMa72APdUh3TAyzA6GWum7rLvEafj1v7WRpJWkTpklFXhaGVm4u/SWeFiMfgIK+ciJgT04k0qbk8APwuPmLR5VmUNyMDOgLMtSLu9sbntVv+eLoAAiOFLk5ZpHs0Q8UPANdNMV03tgxaDnvtgzh7W0Qgvo8CAwEAATANBgkqhkiG9w0BAQsFAAOCAQEAGPI/7KUIjHsNuRHUALIRtNVlhD80gdzKN27IEFLTu/jbiNEIGY59oV0qvx+iCPrLTLDnJkxHlnwApwB2WulXNg7+nYGHPP03jSLXKA+61GAN/ghPULl1DcA5Q+gunhPA4ITyqOr70i/3fphSXWjWfcX8hcym3pDcKzPIY3wV+dVeVdRdi9C1cTRuZ7zh2Chm7e4vM1SagLybMA4F8yckPJsRdVV5hZ+W6cI1H9fhjq/G1N0TyH4wG3FffRVniYVgAxY9m9RgMiQ5qCuc2PdktO7ovmNybijVG1aLVcHcYAS285f4JnZPIAJJMvCvW0NXDDphBNQPG5Nt1PHVgzUiNA== "
    "-----END CERTIFICATE-----"
)

EXCHANGE_SERVER_CONFIG = {
    "data_source": OUTLOOK_SERVER,
    "username": "foo.bar@gmail.com",
    "password": "abc@123",
    "exchange_server": "127.0.0.1",
    "domain": "gmail.com",
    "active_directory_server": "127.0.0.1",
}

EXCHANGE_LDAP_USER = {
    "type": "user",
    "attributes": {"mail": ["user@example.com"]},
}


@pytest.fixture
def reset_http_adapter_cls():
    original_adapter_cls = BaseProtocol.HTTP_ADAPTER_CLS
    original_ssl_context = InMemoryCAAdapter.ssl_context
    yield
    BaseProtocol.HTTP_ADAPTER_CLS = original_adapter_cls
    InMemoryCAAdapter.ssl_context = original_ssl_context


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_exchange_get_user_accounts_uses_in_memory_ssl_adapter(
    mock_account, reset_http_adapter_cls
):
    async with create_outlook_source(
        ssl_enabled=True,
        ssl_ca=VALID_SSL_CERTIFICATE,
        **EXCHANGE_SERVER_CONFIG,
    ) as source:
        source.client._get_user_instance.get_users = AsyncIterator([EXCHANGE_LDAP_USER])

        async for _ in source.client._get_user_instance.get_user_accounts():
            pass

        assert BaseProtocol.HTTP_ADAPTER_CLS is InMemoryCAAdapter
        assert isinstance(InMemoryCAAdapter.ssl_context, ssl.SSLContext)
        assert InMemoryCAAdapter.ssl_context.verify_mode == ssl.CERT_REQUIRED
        assert InMemoryCAAdapter.ssl_context.check_hostname is True


@pytest.mark.parametrize("manager_method", ["init_poolmanager", "proxy_manager_for"])
def test_in_memory_ca_adapter_injects_ssl_context(
    manager_method, reset_http_adapter_cls
):
    sentinel_context = MagicMock(spec=ssl.SSLContext)
    InMemoryCAAdapter.ssl_context = sentinel_context

    with patch.object(requests.adapters.HTTPAdapter, manager_method) as mock_super:
        adapter = InMemoryCAAdapter()
        getattr(adapter, manager_method)()

        # The configured in-memory context must be forwarded to urllib3.
        assert mock_super.call_args.kwargs["ssl_context"] is sentinel_context


def test_in_memory_ca_adapter_omits_ssl_context_when_unset(reset_http_adapter_cls):
    InMemoryCAAdapter.ssl_context = None

    with patch.object(requests.adapters.HTTPAdapter, "init_poolmanager") as mock_super:
        adapter = InMemoryCAAdapter()
        adapter.init_poolmanager()

        assert "ssl_context" not in mock_super.call_args.kwargs


@pytest.mark.asyncio
@patch("connectors.sources.outlook.ssl.create_default_context")
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_exchange_get_user_accounts_builds_ssl_context_from_pem(
    mock_account, mock_create_default_context, reset_http_adapter_cls
):
    mock_context = MagicMock(spec=ssl.SSLContext)
    mock_create_default_context.return_value = mock_context
    pem_certificate = get_pem_format(VALID_SSL_CERTIFICATE)

    async with create_outlook_source(
        ssl_enabled=True,
        ssl_ca=VALID_SSL_CERTIFICATE,
        **EXCHANGE_SERVER_CONFIG,
    ) as source:
        source.client._get_user_instance.get_users = AsyncIterator([EXCHANGE_LDAP_USER])

        async for _ in source.client._get_user_instance.get_user_accounts():
            pass

        mock_create_default_context.assert_called_once_with(cadata=pem_certificate)
        assert InMemoryCAAdapter.ssl_context is mock_context


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_exchange_get_user_accounts_raises_when_ssl_enabled_without_cert(
    mock_account, reset_http_adapter_cls
):
    original_adapter_cls = BaseProtocol.HTTP_ADAPTER_CLS
    exchange_users = ExchangeUsers(
        ad_server="127.0.0.1",
        domain="example.com",
        exchange_server="127.0.0.1",
        user="user",
        password="pass",
        ssl_enabled=True,
        ssl_ca="",
    )
    exchange_users.get_users = AsyncIterator([EXCHANGE_LDAP_USER])

    # SSL on without a cert must fail loudly, not fall back to no verification.
    with pytest.raises(SSLCertificateError):
        async for _ in exchange_users.get_user_accounts():
            pass

    assert BaseProtocol.HTTP_ADAPTER_CLS is original_adapter_cls


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_exchange_get_user_accounts_raises_when_ssl_enabled_with_bad_cert(
    mock_account, reset_http_adapter_cls
):
    original_adapter_cls = BaseProtocol.HTTP_ADAPTER_CLS
    exchange_users = ExchangeUsers(
        ad_server="127.0.0.1",
        domain="example.com",
        exchange_server="127.0.0.1",
        user="user",
        password="pass",
        ssl_enabled=True,
        ssl_ca="not-a-valid-certificate",
    )
    exchange_users.get_users = AsyncIterator([EXCHANGE_LDAP_USER])

    with pytest.raises(SSLCertificateError):
        async for _ in exchange_users.get_user_accounts():
            pass

    assert BaseProtocol.HTTP_ADAPTER_CLS is original_adapter_cls


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_exchange_get_user_accounts_raises_on_markerless_cert_via_client(
    mock_account, reset_http_adapter_cls
):
    # get_pem_format reduces marker-less junk to "", which must hit the
    # "no CA certificate" guard rather than silently using system CAs.
    async with create_outlook_source(
        ssl_enabled=True,
        ssl_ca="this is not a certificate",
        **EXCHANGE_SERVER_CONFIG,
    ) as source:
        source.client._get_user_instance.get_users = AsyncIterator([EXCHANGE_LDAP_USER])

        with pytest.raises(SSLCertificateError):
            async for _ in source.client._get_user_instance.get_user_accounts():
                pass


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_exchange_get_user_accounts_raises_on_unloadable_pem_via_client(
    mock_account, reset_http_adapter_cls
):
    # A non-empty but bogus PEM must hit the "could not be loaded" guard.
    async with create_outlook_source(
        ssl_enabled=True,
        ssl_ca="-----BEGIN CERTIFICATE----- notbase64 -----END CERTIFICATE-----",
        **EXCHANGE_SERVER_CONFIG,
    ) as source:
        source.client._get_user_instance.get_users = AsyncIterator([EXCHANGE_LDAP_USER])

        with pytest.raises(SSLCertificateError):
            async for _ in source.client._get_user_instance.get_user_accounts():
                pass


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_exchange_get_user_accounts_uses_no_verify_when_ssl_disabled(
    mock_account, reset_http_adapter_cls
):
    exchange_users = ExchangeUsers(
        ad_server="127.0.0.1",
        domain="example.com",
        exchange_server="127.0.0.1",
        user="user",
        password="pass",
        ssl_enabled=False,
        ssl_ca="",
    )
    exchange_users.get_users = AsyncIterator([EXCHANGE_LDAP_USER])

    async for _ in exchange_users.get_user_accounts():
        pass

    assert BaseProtocol.HTTP_ADAPTER_CLS is NoVerifyHTTPAdapter


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_exchange_get_user_accounts_does_not_write_cert_file(
    mock_account, reset_http_adapter_cls
):
    with patch("aiofiles.open", create=True) as mock_open:
        async with create_outlook_source(
            ssl_enabled=True,
            ssl_ca=VALID_SSL_CERTIFICATE,
            **EXCHANGE_SERVER_CONFIG,
        ) as source:
            source.client._get_user_instance.get_users = AsyncIterator(
                [EXCHANGE_LDAP_USER]
            )

            async for _ in source.client._get_user_instance.get_user_accounts():
                pass

        mock_open.assert_not_called()


@pytest.mark.asyncio
async def test_validate_config_raises_when_ssl_enabled_without_certificate():
    # SSL enabled without a certificate must be rejected up front, not silently
    # downgraded or failed mid-sync.
    async with create_outlook_source(
        data_source=OUTLOOK_SERVER,
        username="foo.bar@gmail.com",
        password="abc@123",
        exchange_server="127.0.0.1",
        domain="gmail.com",
        active_directory_server="127.0.0.1",
        ssl_enabled=True,
        ssl_ca="",
    ) as source:
        with pytest.raises(ConfigurableFieldValueError) as exc_info:
            await source.validate_config()

        assert "SSL certificate" in str(exc_info.value)


@pytest.mark.asyncio
async def test_validate_config_raises_when_certificate_is_invalid():
    # A present but unloadable certificate would otherwise crash mid-sync with an
    # opaque X509 error.
    async with create_outlook_source(
        data_source=OUTLOOK_SERVER,
        username="foo.bar@gmail.com",
        password="abc@123",
        exchange_server="127.0.0.1",
        domain="gmail.com",
        active_directory_server="127.0.0.1",
        ssl_enabled=True,
        ssl_ca="this is not a certificate",
    ) as source:
        with pytest.raises(ConfigurableFieldValueError) as exc_info:
            await source.validate_config()

        assert "SSL certificate is not valid" in str(exc_info.value)


@pytest.mark.asyncio
async def test_validate_config_passes_when_ssl_enabled_with_valid_certificate():
    async with create_outlook_source(
        data_source=OUTLOOK_SERVER,
        username="foo.bar@gmail.com",
        password="abc@123",
        exchange_server="127.0.0.1",
        domain="gmail.com",
        active_directory_server="127.0.0.1",
        ssl_enabled=True,
        ssl_ca=VALID_SSL_CERTIFICATE,
    ) as source:
        await source.validate_config()


@pytest.mark.asyncio
async def test_get_docs():
    async with create_outlook_source() as source:
        source.client._get_user_instance.get_user_accounts = AsyncIterator(
            [MockAccount()]
        )
        async for document, _ in source.get_docs():
            assert document in EXPECTED_RESPONSE


def _account_raising_on_inbox(exception, smtp):
    """Build a mock account whose first folder access (inbox) raises."""
    account = MagicMock()
    account.default_timezone = "UTC"
    account.primary_smtp_address = smtp
    type(account).inbox = mock.PropertyMock(side_effect=exception)
    return account


@pytest.mark.asyncio
async def test_get_docs_skips_account_without_mailbox_and_continues():
    async with create_outlook_source() as source:
        bad_account = _account_raising_on_inbox(
            ErrorNonExistentMailbox("no mailbox"), smtp="no.mailbox@example.com"
        )
        source.client._get_user_instance.get_user_accounts = AsyncIterator(
            [bad_account, MockAccount()]
        )
        source._logger = MagicMock()

        documents = [document async for document, _ in source.get_docs()]

        # The healthy account is still fully synced past the mail stage.
        assert all(document in EXPECTED_RESPONSE for document in documents)
        assert any(document["_id"] == "contact_1" for document in documents)

        source._logger.warning.assert_called_once()
        warning_message = source._logger.warning.call_args.args[0]
        assert "no.mailbox@example.com" in warning_message
        assert "ErrorNonExistentMailbox" in warning_message


@pytest.mark.asyncio
async def test_get_docs_skips_account_on_access_denied_and_continues():
    async with create_outlook_source() as source:
        bad_account = _account_raising_on_inbox(
            ErrorAccessDenied("access denied"), smtp="denied@example.com"
        )
        source.client._get_user_instance.get_user_accounts = AsyncIterator(
            [bad_account, MockAccount()]
        )
        source._logger = MagicMock()

        documents = [document async for document, _ in source.get_docs()]

        # The healthy account is still fully synced.
        assert all(document in EXPECTED_RESPONSE for document in documents)
        assert any(document["_id"] == "contact_1" for document in documents)

        source._logger.warning.assert_called_once()
        warning_message = source._logger.warning.call_args.args[0]
        assert "denied@example.com" in warning_message
        assert "ErrorAccessDenied" in warning_message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception",
    [
        # Connection-wide failures must abort the sync, not be skipped
        # (which would empty the index).
        TransportError("TLS verification failed"),
        RuntimeError("boom"),
    ],
)
async def test_get_docs_reraises_connection_wide_error(exception):
    async with create_outlook_source() as source:
        bad_account = _account_raising_on_inbox(exception, smtp="broken@example.com")
        source.client._get_user_instance.get_user_accounts = AsyncIterator(
            [bad_account, MockAccount()]
        )

        with pytest.raises(type(exception)):
            async for _document, _ in source.get_docs():
                pass


@pytest.mark.asyncio
async def test_get_docs_falls_back_to_utc_timezone(monkeypatch):
    async with create_outlook_source() as source:
        # An account without a default timezone must fall back to the
        # exchangelib UTC (an EWSTimeZone), never the bare string "UTC".
        account = MockAccount()
        account.default_timezone = None
        source.client._get_user_instance.get_user_accounts = AsyncIterator([account])

        captured = {}

        def fake_fetch_mails(account, timezone):
            captured["timezone"] = timezone
            return AsyncIterator([])

        monkeypatch.setattr(source, "_fetch_mails", fake_fetch_mails)

        _ = [document async for document, _ in source.get_docs()]

        assert captured["timezone"] is UTC


@pytest.mark.asyncio
async def test_get_contacts_resolves_via_distinguished_folder_id():
    async with create_outlook_source() as source:
        account = MockAccount()
        contacts = [contact async for contact in source.client.get_contacts(account)]
        assert [contact.id for contact in contacts] == ["contact_1"]


# Both errors mean a folder is absent and must be skipped (FOLDER_SKIP_ERRORS).
FOLDER_SKIP_EXCEPTIONS = [
    pytest.param(ErrorFolderNotFound("no"), id="ErrorFolderNotFound"),
    pytest.param(ErrorManagedFolderNotFound("no"), id="ErrorManagedFolderNotFound"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("exception", FOLDER_SKIP_EXCEPTIONS)
async def test_get_contacts_skips_when_folder_not_found(exception):
    async with create_outlook_source() as source:
        account = MagicMock()
        account.primary_smtp_address = "alex.wilber@gmail.com"
        type(account).contacts = mock.PropertyMock(side_effect=exception)

        contacts = [contact async for contact in source.client.get_contacts(account)]
        assert contacts == []


@pytest.mark.asyncio
@pytest.mark.parametrize("exception", FOLDER_SKIP_EXCEPTIONS)
async def test_get_calendars_skips_when_folder_not_found(exception):
    async with create_outlook_source() as source:
        account = MagicMock()
        account.primary_smtp_address = "alex.wilber@gmail.com"
        type(account).calendar = mock.PropertyMock(side_effect=exception)

        calendars = [
            calendar async for calendar in source.client.get_calendars(account)
        ]
        assert calendars == []


@pytest.mark.asyncio
@pytest.mark.parametrize("exception", FOLDER_SKIP_EXCEPTIONS)
async def test_get_child_calendars_skips_when_folder_not_found(exception):
    async with create_outlook_source() as source:
        account = MagicMock()
        account.primary_smtp_address = "alex.wilber@gmail.com"
        type(account).calendar = mock.PropertyMock(side_effect=exception)

        calendars = [
            calendar async for calendar in source.client.get_child_calendars(account)
        ]
        assert calendars == []


@pytest.mark.asyncio
async def test_get_child_calendars_skips_non_calendar_child():
    async with create_outlook_source() as source:
        source.client._logger = MagicMock()
        account = MagicMock()
        account.primary_smtp_address = "alex.wilber@gmail.com"
        # A child folder under Calendar that isn't itself a calendar (e.g. a mail
        # folder) is ignored by design; a real Calendar child still yields.
        non_calendar = MagicMock(spec=Messages)
        non_calendar.name = "Notes"
        calendar_child = typed_folder(Calendar, CALENDAR)
        calendar_child.name = "Team Calendar"
        account.calendar.children = [non_calendar, calendar_child]

        calendars = [
            (calendar, child)
            async for calendar, child in source.client.get_child_calendars(account)
        ]

        assert [child for _, child in calendars] == [calendar_child]
        source.client._logger.debug.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("exception", FOLDER_SKIP_EXCEPTIONS)
async def test_get_tasks_skips_when_folder_not_found(exception):
    async with create_outlook_source() as source:
        account = MagicMock()
        account.primary_smtp_address = "alex.wilber@gmail.com"
        type(account).tasks = mock.PropertyMock(side_effect=exception)

        tasks = [task async for task in source.client.get_tasks(account)]
        assert tasks == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method_name, folder_attr",
    [
        ("get_calendars", "calendar"),
        ("get_tasks", "tasks"),
        ("get_contacts", "contacts"),
    ],
)
async def test_get_methods_resolve_folder_off_event_loop(method_name, folder_attr):
    # exchangelib is synchronous, so resolving the distinguished folder must be
    # offloaded via asyncio.to_thread instead of blocking the event loop.
    async with create_outlook_source() as source:
        account = MockAccount()
        method = getattr(source.client, method_name)
        with patch(
            "connectors.sources.outlook.asyncio.to_thread",
            wraps=asyncio.to_thread,
        ) as to_thread:
            _ = [item async for item in method(account)]

        assert any(
            call.args[:3] == (getattr, account, folder_attr)
            for call in to_thread.call_args_list
        )


@pytest.mark.asyncio
async def test_get_mails_resolve_folder_off_event_loop():
    async with create_outlook_source() as source:
        account = MockAccount()
        with patch(
            "connectors.sources.outlook.asyncio.to_thread",
            wraps=asyncio.to_thread,
        ) as to_thread:
            _ = [item async for item in source.client.get_mails(account)]

        # Named folders resolve via getattr, off the loop.
        assert any(
            call.args[:3] == (getattr, account, "inbox")
            for call in to_thread.call_args_list
        )
        # The Archive leaf resolves via a lambda, also off the loop. Queryset
        # materialization also runs via single-arg lambdas, so pick the lambda
        # that resolves to the mail (Messages) folder.
        lambda_calls = [c for c in to_thread.call_args_list if len(c.args) == 1]
        assert lambda_calls
        assert any(
            getattr(call.args[0](), "object_type", None) == MAIL
            for call in lambda_calls
        )


@pytest.mark.asyncio
async def test_get_child_calendars_resolve_folder_off_event_loop():
    async with create_outlook_source() as source:
        account = MockAccount()
        with patch(
            "connectors.sources.outlook.asyncio.to_thread",
            wraps=asyncio.to_thread,
        ) as to_thread:
            _ = [item async for item in source.client.get_child_calendars(account)]

        # The first offloaded call resolves the child-calendar list.
        resolved = to_thread.call_args_list[0].args[0]()
        assert resolved == list(account.calendar.children)


@pytest.mark.asyncio
async def test_get_contacts_queries_distribution_list_fields():
    # The Contacts folder query must include DistributionList fields (members),
    # or contact groups come back with no email addresses. The fields are now
    # applied inside the thread lambda, so spy on the folder's .only() call.
    async with create_outlook_source() as source:
        account = MockAccount()
        only_spy = MagicMock(return_value=[])
        folder = MagicMock()
        folder.all.return_value.only = only_spy
        account.contacts = folder

        _ = [contact async for contact in source.client.get_contacts(account)]

        only_spy.assert_called_once()
        assert "members" in only_spy.call_args.args


@pytest.mark.asyncio
async def test_fetch_contacts_skips_unexpected_item_type():
    # The Contacts folder should only yield Contact or DistributionList items;
    # anything else is skipped with a warning instead of aborting the sync.
    async with create_outlook_source() as source:
        unexpected = MagicMock(spec=Message)
        source.client.get_contacts = AsyncIterator([unexpected])
        source._logger = MagicMock()
        account = MockAccount()

        documents = [
            document
            async for document, _ in source._fetch_contacts(
                account=account, timezone=TIMEZONE
            )
        ]

        assert documents == []
        source._logger.warning.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_contacts_routes_distribution_list_to_group_formatter():
    # DistributionList routes to the group formatter; a contact to the contact one.
    async with create_outlook_source() as source:
        member = MagicMock()
        member.mailbox.email_address = "group.member@gmail.com"
        distribution_list = MagicMock(spec=DistributionList)
        distribution_list.id = "distribution_list_1"
        distribution_list.last_modified_time = "2023-12-12T01:01:01Z"
        distribution_list.display_name = "Dummy Group"
        distribution_list.members = [member]

        source.client.get_contacts = AsyncIterator(
            [distribution_list, ContactDocument()]
        )
        account = MockAccount()

        documents = [
            document
            async for document, _ in source._fetch_contacts(
                account=account, timezone=TIMEZONE
            )
        ]

        by_id = {document["_id"]: document for document in documents}
        assert by_id["distribution_list_1"]["type"] == "Distribution List"
        assert by_id["distribution_list_1"]["email_addresses"] == [
            "group.member@gmail.com"
        ]
        assert by_id["contact_1"]["type"] == "Contact"


@pytest.mark.asyncio
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_exchange_get_user_accounts_handles_missing_type_key(
    mock_account, reset_http_adapter_cls
):
    # Some LDAP entries may not carry a "type" key; that must not raise KeyError.
    user_without_type = {"attributes": {"mail": ["user@example.com"]}}
    async with create_outlook_source() as source:
        source.client.is_cloud = False
        source.client._get_user_instance.get_users = AsyncIterator([user_without_type])

        accounts = [
            account
            async for account in source.client._get_user_instance.get_user_accounts()
        ]

        assert accounts == ["account"]


def test_mails_doc_formatter_handles_missing_sender():
    mail = build_mail_document()
    mail.sender = None

    document = OutlookDocFormatter().mails_doc_formatter(
        mail=mail,
        mail_type={"constant": INBOX_MAIL_OBJECT},
        timezone=TIMEZONE,
    )

    assert document["sender"] is None
    assert document["to_recipients"] == ["dummy.user@gmail.com"]


def test_calendar_doc_formatter_handles_missing_organizer():
    calendar = build_calendar_document()
    calendar.organizer = None

    document = OutlookDocFormatter().calendar_doc_formatter(
        calendar=calendar,
        child_calendar="Calendar",
        timezone=TIMEZONE,
    )

    assert document["organizer"] is None


def test_calendar_doc_formatter_handles_occurrence_without_recurrence():
    calendar = build_calendar_document()
    calendar.type = "Occurrence"
    calendar.recurrence = None

    document = OutlookDocFormatter().calendar_doc_formatter(
        calendar=calendar,
        child_calendar="Calendar",
        timezone=TIMEZONE,
    )

    assert document["meeting_type"] == "Occurrence"


def test_calendar_doc_formatter_handles_birthday_without_start():
    calendar = build_calendar_document()
    calendar.start = None

    document = OutlookDocFormatter().calendar_doc_formatter(
        calendar=calendar,
        child_calendar="Birthdays (Birthdays)",
        timezone=TIMEZONE,
    )

    assert document["date"] is None


def test_calendar_doc_formatter_skips_attendees_without_mailbox():
    calendar = build_calendar_document()
    attendee_without_mailbox = MagicMock()
    attendee_without_mailbox.mailbox = None
    calendar.required_attendees = [attendee_without_mailbox]

    document = OutlookDocFormatter().calendar_doc_formatter(
        calendar=calendar,
        child_calendar="Calendar",
        timezone=TIMEZONE,
    )

    assert document["attendees"] == []


def test_contact_doc_formatter_handles_missing_email_and_phone_entries():
    contact = ContactDocument()
    contact.email_addresses = [None, MagicMock(email=None)]
    contact.phone_numbers = [None, MagicMock(phone_number=None)]

    document = OutlookDocFormatter().contact_doc_formatter(
        contact=contact,
        timezone=TIMEZONE,
    )

    assert document["email_addresses"] == []
    assert document["contact_numbers"] == []


def test_distribution_list_doc_formatter():
    # A contact group is indexed by name plus its members' emails.
    distribution_list = DistributionListDocument()

    document = OutlookDocFormatter().distribution_list_doc_formatter(
        distribution_list=distribution_list,
        timezone=TIMEZONE,
    )

    assert document["_id"] == "distribution_list_1"
    assert document["type"] == "Distribution List"
    assert document["name"] == "Dummy Group"
    assert document["email_addresses"] == ["group.member@gmail.com"]


@pytest.mark.asyncio
async def test_fetch_attachments_skips_attachment_without_id():
    async with create_outlook_source() as source:
        mail = build_mail_document()
        mail.attachments = [
            build_attachment(
                attachment_id=None,
                name="broken.txt",
                size=100,
                last_modified_time="2023-12-12T01:01:01Z",
                content=RESPONSE_CONTENT,
            )
        ]
        source._logger = MagicMock()
        account = MockAccount()

        attachments = [
            document
            async for document, _ in source._fetch_attachments(
                attachment_type=MAIL_ATTACHMENT,
                outlook_object=mail,
                timezone=TIMEZONE,
                account=account,
            )
        ]

        assert attachments == []
        source._logger.warning.assert_called_once()


@pytest.mark.asyncio
async def test_get_content_skips_attachment_without_name():
    async with create_outlook_source() as source:
        source._logger = MagicMock()
        # A nameless attachment would crash get_file_extension(None); skip it.
        attachment = build_attachment(
            attachment_id=MockAttachmentId(id="attachment_id_1"),
            name=None,
            size=100,
            last_modified_time="2023-12-12T01:01:01Z",
            content=RESPONSE_CONTENT,
        )

        response = await source.get_content(
            attachment=attachment,
            timezone=TIMEZONE,
            doit=True,
        )

        assert response is None


@pytest.mark.asyncio
async def test_get_content_skips_item_attachment():
    async with create_outlook_source() as source:
        source._logger = MagicMock()
        # ItemAttachment has no `content`; it must be skipped, not crash.
        attachment = MagicMock(spec=ItemAttachment)
        attachment.attachment_id = MockAttachmentId(id="attachment_id_1")
        attachment.name = "embedded.txt"
        attachment.size = 100

        response = await source.get_content(
            attachment=attachment,
            timezone=TIMEZONE,
            doit=True,
        )

        assert response is None


@pytest.mark.asyncio
async def test_get_content_skips_attachment_without_size():
    async with create_outlook_source() as source:
        # `size` is optional in EWS; a None size must not crash the comparison.
        attachment = build_attachment(
            attachment_id=MockAttachmentId(id="attachment_id_1"),
            name="sync.txt",
            size=None,
            last_modified_time="2023-12-12T01:01:01Z",
            content=RESPONSE_CONTENT,
        )

        response = await source.get_content(
            attachment=attachment,
            timezone=TIMEZONE,
            doit=True,
        )

        assert response is None


@pytest.mark.asyncio
async def test_enqueue_calendars_skips_non_calendar_item():
    async with create_outlook_source() as source:
        source._logger = MagicMock()
        account = MockAccount()
        # A Calendar folder can return a Message, which lacks calendar fields.
        non_calendar_item = MagicMock(spec=Message)

        documents = [
            document
            async for document, _ in source._enqueue_calendars(
                calendar=non_calendar_item,
                child_calendar="Calendar",
                timezone=TIMEZONE,
                account=account,
            )
        ]

        assert documents == []
        source._logger.warning.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_mails_skips_non_mail_item():
    async with create_outlook_source() as source:
        source._logger = MagicMock()
        account = MockAccount()
        # A mail folder can return a CalendarItem, which lacks mail fields.
        non_mail_item = MagicMock(spec=CalendarItem)
        source.client.get_mails = AsyncIterator(
            [(non_mail_item, {"constant": INBOX_MAIL_OBJECT})]
        )

        documents = [
            document
            async for document, _ in source._fetch_mails(
                account=account, timezone=TIMEZONE
            )
        ]

        assert documents == []
        source._logger.warning.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "item_type",
    [Message, MeetingRequest, MeetingResponse, MeetingCancellation],
    ids=lambda cls: cls.__name__,
)
async def test_fetch_mails_accepts_all_mail_item_types(item_type):
    # Every type in MAIL_ITEM_TYPES must pass the guard and yield a document,
    # not just Message; meeting items share the same mail fields.
    async with create_outlook_source() as source:
        source._logger = MagicMock()
        account = MockAccount()
        sender = MagicMock()
        sender.email_address = "dummy.user@gmail.com"
        mail = MagicMock(spec=item_type)
        mail.id = "mail_1"
        mail.last_modified_time = "2023-12-12T01:01:01Z"
        mail.subject = "Dummy Subject"
        mail.to_recipients = [sender]
        mail.cc_recipients = [sender]
        mail.bcc_recipients = [sender]
        mail.sender = sender
        mail.importance = "High"
        mail.categories = ["Outlook"]
        mail.body = "This is a dummy mail"
        mail.has_attachments = False
        mail.attachments = []
        source.client.get_mails = AsyncIterator(
            [(mail, {"constant": INBOX_MAIL_OBJECT, "folder": "inbox"})]
        )

        documents = [
            document
            async for document, _ in source._fetch_mails(
                account=account, timezone=TIMEZONE
            )
        ]

        assert [document["_id"] for document in documents] == ["mail_1"]
        source._logger.warning.assert_not_called()


@pytest.mark.parametrize(
    "allowlist, folder_cls",
    [
        (MAIL_ITEM_TYPES, Messages),
        (CALENDAR_ITEM_TYPES, Calendar),
        (TASK_ITEM_TYPES, Tasks),
    ],
    ids=lambda arg: getattr(arg, "__name__", ""),
)
def test_item_type_allowlists_match_exchangelib(allowlist, folder_cls):
    # Fails if a library upgrade changes a folder's item types (update the formatter).
    assert set(allowlist) == set(folder_cls.supported_item_models)


@pytest.mark.asyncio
async def test_fetch_tasks_skips_non_task_item():
    async with create_outlook_source() as source:
        source._logger = MagicMock()
        account = MockAccount()
        # A Tasks folder can return a Message, which lacks task fields.
        non_task_item = MagicMock(spec=Message)
        source.client.get_tasks = AsyncIterator([non_task_item])

        documents = [
            document
            async for document, _ in source._fetch_tasks(
                account=account, timezone=TIMEZONE
            )
        ]

        assert documents == []
        source._logger.warning.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("exception", FOLDER_SKIP_EXCEPTIONS)
async def test_get_mails_skips_junk_when_folder_not_found(exception):
    async with create_outlook_source() as source:
        # Subclass so the folder override stays local and does not leak into
        # the shared MockAccount used by other tests (pytest runs in random order).
        # The no-op setter lets MockAccount.__init__ assign junk; reads raise.
        class MockAccountWithoutJunk(MockAccount):
            @property
            def junk(self):
                raise exception

            @junk.setter
            def junk(self, value):
                pass

        account = MockAccountWithoutJunk()
        account.msg_folder_root = MagicMock()
        account.msg_folder_root.__truediv__.side_effect = exception

        folders = [
            mail_type["folder"]
            async for _, mail_type in source.client.get_mails(account)
        ]

        assert folders == ["inbox", "sent"]


@pytest.mark.asyncio
@pytest.mark.parametrize("exception", FOLDER_SKIP_EXCEPTIONS)
async def test_get_mails_skips_archive_when_folder_not_found(exception):
    async with create_outlook_source() as source:
        account = MockAccount()
        account.msg_folder_root = MagicMock()
        account.msg_folder_root.__truediv__.side_effect = exception

        folders = [
            mail_type["folder"]
            async for _, mail_type in source.client.get_mails(account)
        ]
        assert folders == ["inbox", "sent", "junk"]


@pytest.mark.asyncio
async def test_get_mails_skips_non_mail_archive_folder():
    async with create_outlook_source() as source:
        source.client._logger = MagicMock()
        account = MockAccount()
        # A folder literally named "Archive" that isn't a mail folder must be
        # ignored, not projected with MAIL_FIELDS.
        account.msg_folder_root = MagicMock()
        account.msg_folder_root.__truediv__.return_value = MagicMock(spec=Calendar)

        folders = [
            mail_type["folder"]
            async for _, mail_type in source.client.get_mails(account)
        ]

        assert folders == ["inbox", "sent", "junk"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "is_cloud, user_response",
    [(True, {"value": [{"mail": "dummy.user@gmail.com"}]})],
)
async def test_get_access_control(is_cloud, user_response):
    async with create_outlook_source() as source:
        source.client.is_cloud = is_cloud
        source.client._get_user_instance.get_users = AsyncIterator([user_response])
        source._dls_enabled = MagicMock(return_value=True)
        acl = []
        async for access_control in source.get_access_control():
            acl.append(access_control)
        assert len(acl) == 1


@pytest.mark.asyncio
async def test_get_access_control_for_server_normalizes_ldap_mail_list():
    async with create_outlook_source() as source:
        source.configuration.get_field("data_source").value = "outlook_server"
        user_response = {
            "attributes": {"mail": ["dummy@es.local"]},
            "dn": "CN=Dummy,CN=Users,DC=es,DC=local",
        }
        source.client._get_user_instance.get_users = AsyncIterator([user_response])
        source._dls_enabled = MagicMock(return_value=True)
        acl = []
        async for access_control in source.get_access_control():
            acl.append(access_control)
        assert len(acl) == 1
        assert acl[0]["identity"]["email"] == "email:dummy@es.local"


@pytest.mark.asyncio
async def test_get_access_control_for_server_handles_malformed_dn():
    async with create_outlook_source() as source:
        source.configuration.get_field("data_source").value = "outlook_server"
        user_response = {
            "attributes": {"mail": "dummy@es.local"},
            "dn": "",
        }
        source.client._get_user_instance.get_users = AsyncIterator([user_response])
        source._dls_enabled = MagicMock(return_value=True)
        acl = []
        async for access_control in source.get_access_control():
            acl.append(access_control)
        assert len(acl) == 1
        assert acl[0]["identity"]["display_name"] == "name:"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "user_response",
    [
        {
            "attributes": {"mail": "dummy@es.local"},
            "dn": "CN=Dummy,CN=Users,DC=es,DC=local",
        },
        {
            "attributes": {"mail": "dummy2@es.local"},
            "dn": "CN=Dummy2,CN=Users,DC=es,DC=local",
        },
    ],
)
async def test_get_access_control_for_server(user_response):
    async with create_outlook_source() as source:
        source.configuration.get_field("data_source").value = "outlook_server"
        source.client._get_user_instance.get_users = AsyncIterator([user_response])
        source._dls_enabled = MagicMock(return_value=True)
        acl = []
        async for access_control in source.get_access_control():
            acl.append(access_control)
        assert len(acl) == 1
