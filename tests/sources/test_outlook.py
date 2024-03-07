#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Outlook source class methods"""
from contextlib import asynccontextmanager
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp import StreamReader

from connectors.source import ConfigurableFieldValueError
from connectors.sources.outlook import (
    OUTLOOK_CLOUD,
    OUTLOOK_SERVER,
    Forbidden,
    NotFound,
    OutlookDataSource,
    UnauthorizedException,
    UsersFetchFailed,
)
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


class CustomPath:
    def __truediv__(self, path):
        # Simulate hierarchy navigation and return a list of dictionaries
        if path == "Top of Information Store":
            return self
        elif path == "Archive":
            return MockOutlookObject(object_type=MAIL)
        elif path == "Contacts":
            return MockOutlookObject(object_type=CONTACT)
        else:
            msg = "Unsupported path element"
            raise ValueError(msg)


class MockAttachmentId:
    def __init__(self, id):  # noqa
        self.id = id


class MailDocument:
    def __init__(self):
        sender = MagicMock()
        sender.email_address = "dummy.user@gmail.com"

        self.id = "mail_1"
        self.last_modified_time = "2023-12-12T01:01:01Z"
        self.subject = "Dummy Subject"
        self.to_recipients = [sender]
        self.cc_recipients = [sender]
        self.bcc_recipients = [sender]
        self.sender = sender
        self.importance = "High"
        self.categories = ["Outlook"]
        self.body = "This is a dummy mail"
        self.has_attachments = True
        self.attachments = [MOCK_ATTACHMENT]


class TaskDocument:
    def __init__(self):
        self.id = "task_1"
        self.last_modified_time = "2023-12-12T01:01:01Z"
        self.subject = "Create Test cases for Outlook"
        self.owner = "Test User"
        self.start_date = "2023-12-12T01:01:01Z"
        self.due_date = "2023-12-12T01:01:01Z"
        self.complete_date = "2023-12-12T01:01:01Z"
        self.importance = "High"
        self.text_body = "This is a dummy task"
        self.status = "completed"
        self.categories = ["Outlook"]
        self.has_attachments = True
        self.attachments = [MOCK_ATTACHMENT]


class ContactDocument:
    def __init__(self):
        contact = MagicMock()
        contact.email = "dummy.user@gmail.com"
        contact.phone_number = 99887776655

        self.id = "contact_1"
        self.last_modified_time = "2023-12-12T01:01:01Z"
        self.display_name = "Dummy User"
        self.email_addresses = [contact]
        self.phone_numbers = [contact]
        self.company_name = "ABC"
        self.birthday = "2023-12-12T01:01:01Z"


class CalendarDocument:
    def __init__(self):
        organizer = MagicMock()
        organizer.email_address = "dummy.user@gmail.com"
        organizer.mailbox.email_address = "dummy.user@gmail.com"

        self.id = "calendar_1"
        self.organizer = organizer
        self.last_modified_time = "2023-12-12T01:01:01Z"
        self.subject = "TC Review meeting"
        self.type = "Single"
        self.start = "2023-12-12T01:01:01Z"
        self.end = "2023-12-12T01:01:01Z"
        self.location = "USA"
        self.required_attendees = [organizer]
        self.body = "This is a dummy calendar"
        self.has_attachments = True
        self.attachments = [MOCK_ATTACHMENT]


class AllObjects:
    def __init__(self, object_type):
        self.object_type = object_type

    def only(self, *args):
        match self.object_type:
            case "mail":
                return [MailDocument()]
            case "task":
                return [TaskDocument()]
            case "contact":
                return [ContactDocument()]
            case "calendar":
                return [CalendarDocument()]


class MockOutlookObject:
    def __init__(self, object_type):
        self.object_type = object_type
        self.children = [self]

    def all(self):  # noqa
        return AllObjects(object_type=self.object_type)


class MockAccount:
    def __init__(self):
        self.default_timezone = "UTC"

        self.inbox = MockOutlookObject(object_type=MAIL)
        self.sent = MockOutlookObject(object_type=MAIL)
        self.root = MockOutlookObject(object_type=MAIL)
        self.junk = MockOutlookObject(object_type=MAIL)
        self.tasks = MockOutlookObject(object_type=TASK)
        self.calendar = MockOutlookObject(object_type=CALENDAR)
        self.root = CustomPath()
        self.primary_smtp_address = "alex.wilber@gmail.com"


class MockAttachment:
    def __init__(self, attachment_id, name, size, last_modified_time, content):
        self.attachment_id = attachment_id
        self.name = name
        self.size = size
        self.last_modified_time = last_modified_time
        self.content = content


MOCK_ATTACHMENT = MockAttachment(
    attachment_id=MockAttachmentId(
        id="attachment_id_1",
    ),
    name="sync.txt",
    size=1221,
    last_modified_time="2023-12-12T01:01:01Z",
    content=RESPONSE_CONTENT,
)

MOCK_ATTACHMENT_WITHOUT_EXTENSION = MockAttachment(
    attachment_id=MockAttachmentId(
        id="attachment_id_2",
    ),
    name="sync",
    size=200,
    last_modified_time="2023-12-12T01:01:01Z",
    content=RESPONSE_CONTENT,
)

MOCK_ATTACHMENT_WITH_SIZE_ZERO = MockAttachment(
    attachment_id=MockAttachmentId(
        id="attachment_id_3",
    ),
    name="test.py",
    size=0,
    last_modified_time="2023-12-12T01:01:01Z",
    content=RESPONSE_CONTENT,
)

MOCK_ATTACHMENT_WITH_LARGER_SIZE = MockAttachment(
    attachment_id=MockAttachmentId(
        id="attachment_id_3",
    ),
    name="test.py",
    size=23000000,
    last_modified_time="2023-12-12T01:01:01Z",
    content=RESPONSE_CONTENT,
)

MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION = MockAttachment(
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
    if url == "https://graph.microsoft.com/v1.0/users?$top=999":
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
    with patch(
        "connectors.content_extraction.ContentExtraction.extract_text",
        return_value=str(RESPONSE_CONTENT),
    ), patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
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
        (False, {"type": "user", "attributes": {"mail": "account"}}),
    ],
)
@patch("connectors.sources.outlook.Account", return_value="account")
async def test_get_user_accounts_for_cloud(account, is_cloud, user_response):
    async with create_outlook_source() as source:
        source.client.is_cloud = is_cloud
        source.client._get_user_instance.get_users = AsyncIterator([user_response])

        async for source_account in source.client._get_user_instance.get_user_accounts():
            assert source_account == "account"


@pytest.mark.asyncio
async def test_get_docs():
    async with create_outlook_source() as source:
        source.client._get_user_instance.get_user_accounts = AsyncIterator(
            [MockAccount()]
        )
        async for document, _ in source.get_docs():
            assert document in EXPECTED_RESPONSE
