#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Outlook source class methods"""

import ssl
from contextlib import asynccontextmanager
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests.adapters
from aiohttp import StreamReader
from connectors_sdk.source import ConfigurableFieldValueError
from exchangelib.errors import (
    ErrorFolderNotFound,
    ErrorNonExistentMailbox,
    TransportError,
)
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter

from connectors.sources.outlook import OutlookDataSource
from connectors.sources.outlook.client import (
    ExchangeUsers,
    Forbidden,
    InMemoryCAAdapter,
    NotFound,
    SSLCertificateError,
    UnauthorizedException,
    UsersFetchFailed,
)
from connectors.sources.outlook.constants import (
    OUTLOOK_CLOUD,
    OUTLOOK_SERVER,
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
            return MockOutlookObject(object_type=MAIL)
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
        self.junk = MockOutlookObject(object_type=MAIL)
        self.tasks = MockOutlookObject(object_type=TASK)
        self.calendar = MockOutlookObject(object_type=CALENDAR)
        # Accessed via distinguished folder IDs.
        self.contacts = MockOutlookObject(object_type=CONTACT)
        self.msg_folder_root = MockMsgFolderRoot()
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
@patch("connectors.sources.outlook.client.Connection")
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
@patch("connectors.sources.outlook.client.Connection")
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
@patch("connectors.sources.outlook.client.Connection")
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
@patch("connectors.sources.outlook.client.Connection")
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
            "connectors_sdk.content_extraction.ContentExtraction.extract_text",
            return_value=str(RESPONSE_CONTENT),
        ),
        patch(
            "connectors_sdk.content_extraction.ContentExtraction.get_extraction_config",
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
@patch("connectors.sources.outlook.client.Account", return_value="account")
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
@patch("connectors.sources.outlook.client.logger.warning")
@patch("connectors.sources.outlook.client.Account", return_value="account")
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
@patch("connectors.sources.outlook.client.Account", return_value="account")
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
@patch("connectors.sources.outlook.client.Account", return_value="account")
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
@patch("connectors.sources.outlook.client.ssl.create_default_context")
@patch("connectors.sources.outlook.client.Account", return_value="account")
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
@patch("connectors.sources.outlook.client.Account", return_value="account")
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
@patch("connectors.sources.outlook.client.Account", return_value="account")
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
@patch("connectors.sources.outlook.client.Account", return_value="account")
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
@patch("connectors.sources.outlook.client.Account", return_value="account")
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
@patch("connectors.sources.outlook.client.Account", return_value="account")
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
@patch("connectors.sources.outlook.client.Account", return_value="account")
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
        assert "no associated mailbox" in warning_message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception",
    [
        # Connection-wide failures (e.g. exchangelib wraps TLS errors as
        # TransportError) must abort the sync rather than be silently skipped,
        # which would empty the index.
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
async def test_get_contacts_resolves_via_distinguished_folder_id():
    async with create_outlook_source() as source:
        account = MockAccount()
        contacts = [contact async for contact in source.client.get_contacts(account)]
        assert [contact.id for contact in contacts] == ["contact_1"]


@pytest.mark.asyncio
async def test_get_contacts_skips_when_folder_not_found():
    async with create_outlook_source() as source:
        account = MagicMock()
        account.primary_smtp_address = "alex.wilber@gmail.com"
        type(account).contacts = mock.PropertyMock(
            side_effect=ErrorFolderNotFound("no")
        )

        contacts = [contact async for contact in source.client.get_contacts(account)]
        assert contacts == []


@pytest.mark.asyncio
async def test_get_mails_skips_archive_when_folder_not_found():
    async with create_outlook_source() as source:
        account = MockAccount()
        account.msg_folder_root = MagicMock()
        account.msg_folder_root.__truediv__.side_effect = ErrorFolderNotFound("no")

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
