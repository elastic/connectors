#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Jira database source class methods"""
import ssl
from contextlib import asynccontextmanager
from copy import copy
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import aiohttp
import pytest
from aiohttp import StreamReader
from aiohttp.client_exceptions import ClientResponseError
from freezegun import freeze_time

from connectors.protocol import Filter
from connectors.source import ConfigurableFieldValueError
from connectors.sources.jira import (
    InternalServerError,
    JiraClient,
    JiraDataSource,
    NotFound,
)
from connectors.utils import ssl_context
from tests.commons import AsyncIterator
from tests.sources.support import create_source

ADVANCED_SNIPPET = "advanced_snippet"

HOST_URL = "http://127.0.0.1:8080"
MOCK_MYSELF = {
    "name": "admin",
    "emailAddress": "admin@local.com",
    "displayName": "admin@local.com",
    "timeZone": "Asia/Kolkata",
}

MOCK_PROJECT = [
    {"id": "1", "name": "dummy_project", "key": "DP"},
    {"id": "2", "name": "test_project", "key": "TP"},
]
EXPECTED_PROJECT = [
    {
        "_id": "project-1",
        "_timestamp": "2023-01-24T09:37:19+05:30",
        "Type": "Project",
        "Project": {"id": "1", "name": "dummy_project", "key": "DP"},
    },
    {
        "_id": "project-2",
        "_timestamp": "2023-01-24T09:37:19+05:30",
        "Type": "Project",
        "Project": {"id": "2", "name": "test_project", "key": "TP"},
    },
]

MOCK_ISSUE = {
    "id": "1234",
    "key": "TP-1",
    "fields": {
        "project": {"name": "test_project", "key": "TP"},
        "updated": "2023-02-01T01:02:20",
        "issuetype": {"name": "Task"},
        "attachment": [
            {
                "id": 10001,
                "filename": "test_file.txt",
                "created": "2023-02-01T01:02:20",
                "size": 200,
            }
        ],
        "issuerestriction": {
            "issuerestrictions": {},
        },
    },
}
EXPECTED_ISSUE = {
    "_id": "test_project-TP-1",
    "_timestamp": "2023-02-01T01:02:20",
    "Type": "Task",
    "Issue": {
        "project": {"name": "test_project", "key": "TP"},
        "updated": "2023-02-01T01:02:20",
        "issuetype": {"name": "Task"},
        "attachment": [
            {
                "id": 10001,
                "filename": "test_file.txt",
                "created": "2023-02-01T01:02:20",
                "size": 200,
            }
        ],
        "issuerestriction": {"issuerestrictions": {}},
    },
}

MOCK_ISSUE_TYPE_BUG = {
    "id": "1234",
    "key": "TP-2",
    "fields": {
        "project": {"name": "test_project", "key": "TP"},
        "updated": "2023-02-01T01:02:20",
        "issuetype": {"name": "Bug"},
        "attachment": [
            {
                "id": 10001,
                "filename": "test_file.txt",
                "created": "2023-02-01T01:02:20",
                "size": 200,
            }
        ],
        "issuerestriction": {
            "issuerestrictions": {
                "projectrole": [
                    {"restrictionValue": "10002"},
                ]
            }
        },
    },
}
EXPECTED_ISSUE_TYPE_BUG = {
    "_id": "test_project-TP-2",
    "_timestamp": "2023-02-01T01:02:20",
    "Type": "Bug",
    "Issue": {
        "project": {"name": "test_project", "key": "TP"},
        "updated": "2023-02-01T01:02:20",
        "issuetype": {"name": "Bug"},
        "attachment": [
            {
                "id": 10001,
                "filename": "test_file.txt",
                "created": "2023-02-01T01:02:20",
                "size": 200,
            }
        ],
        "issuerestriction": {
            "issuerestrictions": {
                "projectrole": [
                    {"restrictionValue": "10002"},
                ]
            }
        },
    },
}

EXPECTED_ATTACHMENT_TYPE_BUG = {
    "_id": "TP-2-10001",
    "title": "test_file.txt",
    "Type": "Attachment",
    "issue": "TP-2",
    "_timestamp": "2023-02-01T01:02:20",
    "size": 200,
}

MOCK_ATTACHMENT = [
    {
        "id": 10001,
        "filename": "test_file.txt",
        "created": "2023-02-01T01:02:20",
        "size": 200,
    }
]
EXPECTED_ATTACHMENT = {
    "_id": "TP-1-10001",
    "title": "test_file.txt",
    "Type": "Attachment",
    "issue": "TP-1",
    "_timestamp": "2023-02-01T01:02:20",
    "size": 200,
}
EXPECTED_ATTACHMENT_FOR_BUG = {
    "_id": "TP-2-10001",
    "title": "test_file.txt",
    "Type": "Attachment",
    "issue": "TP-2",
    "_timestamp": "2023-02-01T01:02:20",
    "size": 200,
}

RESPONSE_CONTENT = "# This is the dummy file"
EXPECTED_CONTENT = {
    "_id": "TP-1-10001",
    "_timestamp": "2023-02-01T01:02:20",
    "_attachment": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl",
}
EXPECTED_CONTENT_EXTRACTED = {
    "_id": "TP-1-10001",
    "_timestamp": "2023-02-01T01:02:20",
    "body": RESPONSE_CONTENT,
}

MOCK_USER = (
    {
        "self": "url1",
        "accountId": "607194d6bc3c3f006f4c35d6",
        "accountType": "atlassian",
        "displayName": "user1",
        "active": True,
        "email_address": "user.test@gmail.com",
        "locale": "US",
    },
)

MOCK_SECURITY_LEVEL = {
    "id": "10000",
    "key": "TP-1",
    "fields": {"security": {"id": "level-1", "name": "issue-level"}},
}

MOCK_SECURITY_LEVEL_MEMBERS = {
    "maxResults": 50,
    "startAt": 0,
    "total": 2,
    "isLast": True,
    "values": [
        {
            "id": "level-1",
            "issueSecurityLevelId": "10000",
            "issueSecuritySchemeId": "10002",
            "holder": {
                "type": "user",
                "parameter": "user-1",
                "value": "user-1",
                "user": {
                    "self": "url",
                    "accountId": "user-1",
                    "emailAddress": "test.user@gmail.com",
                    "displayName": "Test User",
                    "active": True,
                    "timeZone": "Asia/Calcutta",
                    "accountType": "atlassian",
                },
                "expand": "user",
            },
        },
        {
            "id": "level-2",
            "issueSecurityLevelId": "10000",
            "issueSecuritySchemeId": "10002",
            "holder": {
                "type": "group",
                "parameter": "Project-admins",
                "value": "d614bbb8-703d-4d41-a51a-0c3cfcef9318",
                "group": {
                    "name": "Project-admins",
                    "groupId": "d614bbb8-703d-4d41-a51a-0c3cfcef9318",
                    "self": "self",
                },
                "expand": "group",
            },
        },
        {
            "id": "level-3",
            "issueSecurityLevelId": "10000",
            "issueSecuritySchemeId": "10002",
            "holder": {
                "type": "projectRole",
                "parameter": "10002",
                "value": "10002",
                "projectRole": {
                    "self": "self",
                    "name": "Administrator",
                    "id": 10002,
                },
                "expand": "projectRole",
            },
        },
    ],
}

MOCK_PROJECT_ROLE_MEMBERS = {
    "self": "self",
    "name": "Administrators",
    "id": 10002,
    "actors": [
        {
            "id": 11542,
            "displayName": "Test User",
            "type": "atlassian-user-role-actor",
            "actorUser": {"accountId": "5ff5815e34847e0069fedee3"},
        },
        {
            "id": 11543,
            "displayName": "Project-admins",
            "type": "atlassian-group-role-actor",
            "name": "Project-admins",
            "actorGroup": {
                "name": "Project-admins",
                "displayName": "Project-admins",
                "groupId": "d614bbb8-703d-4d41-a51a-0c3cfcef9318",
            },
        },
    ],
}

ACCESS_CONTROL = "_allow_access_control"


@asynccontextmanager
async def create_jira_source(use_text_extraction_service=False):
    async with create_source(
        JiraDataSource,
        data_source="jira_cloud",
        username="admin",
        password="changeme",
        account_email="me@example.com",
        api_token="abc#123",
        jira_url="http://127.0.0.1:8080",
        projects="*",
        ssl_enabled=False,
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source


class MockSsl:
    """This class contains methods which returns dummy ssl context"""

    def load_verify_locations(self, cadata):
        """This method verify locations"""
        pass


class JSONAsyncMock(AsyncMock):
    def __init__(self, json, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._json = json

    async def json(self):
        return self._json


class StreamReaderAsyncMock(AsyncMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content = StreamReader


def get_json_mock(mock_response):
    async_mock = AsyncMock()
    async_mock.__aenter__ = AsyncMock(return_value=JSONAsyncMock(mock_response))
    return async_mock


def get_stream_reader():
    async_mock = AsyncMock()
    async_mock.__aenter__ = AsyncMock(return_value=StreamReaderAsyncMock())
    return async_mock


def side_effect_function(url, ssl):
    """Dynamically changing return values for API calls
    Args:
        url, ssl: Params required for get call
    """
    if url == f"{HOST_URL}/rest/api/2/search?jql=&maxResults=100&startAt=0":
        mocked_issue_response = {"issues": [MOCK_ISSUE], "total": 101}
        return get_json_mock(mock_response=mocked_issue_response)
    elif url == f"{HOST_URL}/rest/api/2/issue/TP-1":
        return get_json_mock(mock_response=MOCK_ISSUE)
    elif url == f"{HOST_URL}/rest/api/2/search?jql=&maxResults=100&startAt=100":
        mocked_issue_data = {"issues": [MOCK_ISSUE_TYPE_BUG], "total": 1}
        return get_json_mock(mock_response=mocked_issue_data)
    elif url == f"{HOST_URL}/rest/api/2/myself":
        return get_json_mock(mock_response=MOCK_MYSELF)
    elif url == f"{HOST_URL}/rest/api/2/project?expand=description,lead,url":
        return get_json_mock(mock_response=MOCK_PROJECT)
    elif url == f"{HOST_URL}/rest/api/2/search?jql=type=bug&maxResults=100&startAt=0":
        mocked_issue_data_bug = {"issues": [MOCK_ISSUE_TYPE_BUG], "total": 1}
        return get_json_mock(mock_response=mocked_issue_data_bug)
    elif url == f"{HOST_URL}/rest/api/2/issue/TP-2":
        return get_json_mock(mock_response=MOCK_ISSUE_TYPE_BUG)
    elif url == f"{HOST_URL}/rest/api/2/search?jql=type=task&maxResults=100&startAt=0":
        mocked_issue_data_task = {"issues": [MOCK_ISSUE], "total": 1}
        return get_json_mock(mock_response=mocked_issue_data_task)
    elif (
        url
        == f"{HOST_URL}/rest/api/2/user/permission/search?projectKey=DP&permissions=BROWSE&maxResults=1000&startAt=0"
    ):
        return get_json_mock(mock_response=MOCK_USER)
    elif (
        url
        == f"{HOST_URL}/rest/api/2/user/permission/search?projectKey=DP&permissions=BROWSE&maxResults=1000&startAt=1000"
    ):
        return get_json_mock(mock_response=[])
    elif (
        url
        == f"{HOST_URL}/rest/api/2/user/permission/search?projectKey=TP&permissions=BROWSE&maxResults=1000&startAt=0"
    ):
        return get_json_mock(mock_response=MOCK_USER)
    elif (
        url
        == f"{HOST_URL}/rest/api/2/user/permission/search?projectKey=TP&permissions=BROWSE&maxResults=1000&startAt=1000"
    ):
        return get_json_mock(mock_response=[])
    elif url == f"{HOST_URL}/rest/api/2/issue/TP-1?fields=security":
        return get_json_mock(mock_response=MOCK_SECURITY_LEVEL)
    elif (
        url
        == f"{HOST_URL}/rest/api/3/issuesecurityschemes/level/member?maxResults=100&startAt=0&levelId=level-1&expand=user,group,projectRole"
    ):
        return get_json_mock(mock_response=MOCK_SECURITY_LEVEL_MEMBERS)
    elif url == f"{HOST_URL}/rest/api/3/project/TP/role/10002":
        return get_json_mock(mock_response=MOCK_PROJECT_ROLE_MEMBERS)


@pytest.mark.parametrize(
    "field, data_source",
    [
        ("jira_url", "jira_cloud"),
        ("projects", "jira_cloud"),
        ("api_token", "jira_cloud"),
        ("account_email", "jira_cloud"),
        ("username", "jira_server"),
        ("password", "jira_server"),
        ("data_center_username", "jira_data_center"),
        ("data_center_password", "jira_data_center"),
    ],
)
@pytest.mark.asyncio
async def test_validate_configuration_for_empty_fields(field, data_source):
    async with create_jira_source() as source:
        source.jira_client.configuration.get_field("data_source").value = data_source
        source.jira_client.configuration.get_field(field).value = ""

        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_api_call_negative():
    """Tests the api_call function while getting an exception."""

    async with create_jira_source() as source:
        source.jira_client.retry_count = 0

        with patch.object(
            aiohttp.ClientSession, "get", side_effect=Exception("Something went wrong")
        ):
            with pytest.raises(Exception):
                await anext(source.jira_client.api_call(url_name="ping"))

        with patch.object(
            aiohttp.ClientSession, "get", side_effect=Exception("Something went wrong")
        ):
            with pytest.raises(Exception):
                await anext(source.jira_client.api_call(url_name="ping"))


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_api_call_when_server_is_down():
    """Tests the api_call function while server gets disconnected."""

    async with create_jira_source() as source:
        source.jira_client.retry_count = 0

        with patch.object(
            aiohttp.ClientSession,
            "get",
            side_effect=aiohttp.ServerDisconnectedError("Something went wrong"),
        ):
            with pytest.raises(aiohttp.ServerDisconnectedError):
                await anext(source.jira_client.api_call(url_name="ping"))


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_with_429_status():
    initial_response = ClientResponseError(None, None)
    initial_response.status = 429
    initial_response.message = "rate-limited"
    initial_response.headers = {"Retry-After": 0.1}

    retried_response = AsyncMock()
    payload = {"value": "Test rate limit"}

    retried_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(payload))
    async with create_jira_source() as source:
        with patch(
            "aiohttp.ClientSession.get",
            side_effect=[initial_response, retried_response],
        ):
            async for response in source.jira_client.api_call(
                url="http://localhost:1000/sample"
            ):
                result = await response.json()

    assert result == payload


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_with_429_status_without_retry_after_header():
    initial_response = ClientResponseError(None, None)
    initial_response.status = 429
    initial_response.message = "rate-limited"

    retried_response = AsyncMock()
    payload = {"value": "Test rate limit"}

    retried_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(payload))
    with patch("connectors.sources.jira.DEFAULT_RETRY_SECONDS", 0):
        async with create_jira_source() as source:
            with patch(
                "aiohttp.ClientSession.get",
                side_effect=[initial_response, retried_response],
            ):
                async for response in source.jira_client.api_call(
                    url="http://localhost:1000/sample"
                ):
                    result = await response.json()

        assert result == payload


@pytest.mark.asyncio
async def test_get_with_404_status():
    error = ClientResponseError(None, None)
    error.status = 404

    async with create_jira_source() as source:
        with patch(
            "aiohttp.ClientSession.get",
            side_effect=error,
        ):
            with pytest.raises(NotFound):
                async for response in source.jira_client.api_call(
                    url="http://localhost:1000/err"
                ):
                    await response.json()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_with_500_status():
    error = ClientResponseError(None, None)
    error.status = 500

    async with create_jira_source() as source:
        with patch(
            "aiohttp.ClientSession.get",
            side_effect=error,
        ):
            with pytest.raises(InternalServerError):
                async for response in source.jira_client.api_call(
                    url="http://localhost:1000/err"
                ):
                    await response.json()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_with_ssl(
    mock_get,
):
    """Test ping method of JiraDataSource class with SSL"""

    mock_get.return_value.__aenter__.return_value.status = 200
    async with create_jira_source() as source:
        source.jira_client.ssl_enabled = True
        source.jira_client.certificate = (
            "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"
        )

        with patch.object(ssl, "create_default_context", return_value=MockSsl()):
            source.jira_client.ssl_ctx = ssl_context(
                certificate=source.jira_client.certificate
            )
            await source.ping()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_for_failed_connection_exception(client_session_get):
    """Tests the ping functionality when connection can not be established to Jira."""

    async with create_jira_source() as source:
        with patch.object(
            JiraClient, "api_call", side_effect=Exception("Something went wrong")
        ):
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
async def test_validate_config_for_ssl_enabled_when_ssl_ca_empty_raises_error():
    """This function test _validate_configuration when certification is empty when ssl is enabled"""
    async with create_jira_source() as source:
        source.configuration.get_field("ssl_enabled").value = True
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_invalid_concurrent_downloads():
    """Test validate_config method of BaseDataSource class with invalid concurrent downloads"""

    async with create_jira_source() as source:
        source.configuration.get_field("concurrent_downloads").value = 1000
        with pytest.raises(
            ConfigurableFieldValueError,
            match="Field validation errors: 'Maximum concurrent downloads' value '1000' should be less than 101.",
        ):
            await source.validate_config()


@pytest.mark.asyncio
async def test_tweak_bulk_options():
    """Test tweak_bulk_options method of BaseDataSource class"""

    async with create_jira_source() as source:
        source.concurrent_downloads = 10
        options = {"concurrent_downloads": 5}

        source.tweak_bulk_options(options)

        assert options["concurrent_downloads"] == 10


@pytest.mark.asyncio
async def test_get_session():
    """Test that the instance of session returned is always the same for the datasource class."""
    async with create_jira_source() as source:
        first_instance = source.jira_client._get_session()
        second_instance = source.jira_client._get_session()
        assert first_instance is second_instance


@pytest.mark.asyncio
async def test_close_with_client_session():
    async with create_jira_source() as source:
        source.jira_client._get_session()

    assert source.jira_client.session is None


@pytest.mark.asyncio
async def test_close_without_client_session():
    """Test close method when the session does not exist"""

    async with create_jira_source() as source:
        assert source.jira_client.session is None


@pytest.mark.asyncio
async def test_get_timezone():
    async with create_jira_source() as source:
        with patch.object(
            aiohttp.ClientSession, "get", side_effect=side_effect_function
        ):
            timezone = await source._get_timezone()
            assert timezone == "Asia/Kolkata"


@freeze_time("2023-01-24T04:07:19")
@pytest.mark.asyncio
async def test_get_projects():
    """Test _get_projects method"""

    async with create_jira_source() as source:
        with patch("aiohttp.ClientSession.get", side_effect=side_effect_function):
            await source._get_projects()
            source_projects = []
            expected_projects = [*EXPECTED_PROJECT, "FINISHED"]
            while not source.queue.empty():
                _, project = await source.queue.get()
                if isinstance(project, tuple):
                    source_projects.append(project[0])
                else:
                    source_projects.append(project)
            assert source_projects == expected_projects


@freeze_time("2023-01-24T04:07:19")
@pytest.mark.asyncio
async def test_get_projects_for_specific_project():
    """Test _get_projects method for specific project key"""

    async with create_jira_source() as source:
        source.jira_client.projects = ["DP"]
        async_project_response = AsyncMock()
        async_project_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(MOCK_PROJECT[0])
        )

        myself_mock = AsyncMock()
        myself_mock.__aenter__ = AsyncMock(return_value=JSONAsyncMock(MOCK_MYSELF))

        with patch(
            "aiohttp.ClientSession.get",
            side_effect=[myself_mock, async_project_response],
        ):
            await source._get_projects()
            _, project = await source.queue.get()
            assert project[0] == EXPECTED_PROJECT[0]


@pytest.mark.asyncio
async def test_verify_projects():
    """Test _verify_projects method"""
    async with create_jira_source() as source:
        source.jira_client.projects = ["TP", "DP"]

        with patch("aiohttp.ClientSession.get", side_effect=side_effect_function):
            await source._verify_projects()


@pytest.mark.asyncio
async def test_verify_projects_with_unavailable_project_keys():
    async with create_jira_source() as source:
        source.jira_client.projects = ["TP", "AP"]

        with patch("aiohttp.ClientSession.get", side_effect=side_effect_function):
            with pytest.raises(Exception, match="Configured unavailable projects: AP"):
                await source._verify_projects()


@pytest.mark.asyncio
async def test_put_issue():
    """Test _put_issue method"""

    async with create_jira_source() as source:
        source.get_content = Mock(return_value=EXPECTED_CONTENT)

        with patch("aiohttp.ClientSession.get", side_effect=side_effect_function):
            await source._put_issue(issue=MOCK_ISSUE)
            assert source.queue.qsize() == 3


@pytest.mark.asyncio
async def test_put_attachment_positive():
    """Test _put_attachment method"""

    async with create_jira_source() as source:
        source.get_content = Mock(
            return_value={
                "_id": "TP-123-test-1",
                "_timestamp": "2023-01-03T09:24:50.633Z",
                "_attachment": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl",
            }
        )
        await source._put_attachment(
            attachments=MOCK_ATTACHMENT,
            issue_key="TP-1",
            access_control="access_control",
        )
        assert source.queue.qsize() == 1


@pytest.mark.asyncio
async def test_get_content():
    """Tests the get content method."""

    async with create_jira_source() as source:
        with mock.patch("aiohttp.ClientSession.get", return_value=get_stream_reader()):
            with mock.patch(
                "aiohttp.StreamReader.iter_chunked",
                return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
            ):
                response = await source.get_content(
                    issue_key="TP-1",
                    attachment=MOCK_ATTACHMENT[0],
                    doit=True,
                )
                assert response == EXPECTED_CONTENT


@pytest.mark.asyncio
async def test_get_content_with_text_extraction_enabled_adds_body():
    """Tests the get content method."""
    with patch(
        "connectors.content_extraction.ContentExtraction.extract_text",
        return_value=RESPONSE_CONTENT,
    ), patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
    ):
        async with create_jira_source(use_text_extraction_service=True) as source:
            with mock.patch(
                "aiohttp.ClientSession.get", return_value=get_stream_reader()
            ):
                with mock.patch(
                    "aiohttp.StreamReader.iter_chunked",
                    return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
                ):
                    response = await source.get_content(
                        issue_key="TP-1",
                        attachment=MOCK_ATTACHMENT[0],
                        doit=True,
                    )
                    assert response == EXPECTED_CONTENT_EXTRACTED


@pytest.mark.asyncio
async def test_get_content_with_upper_extension():
    """Tests the get content method."""

    async with create_jira_source() as source:
        with mock.patch("aiohttp.ClientSession.get", return_value=get_stream_reader()):
            with mock.patch(
                "aiohttp.StreamReader.iter_chunked",
                return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
            ):
                attachment = copy(EXPECTED_ATTACHMENT)
                attachment["filename"] = "testfile.TXT"

                response = await source.get_content(
                    issue_key="TP-1",
                    attachment=MOCK_ATTACHMENT[0],
                    doit=True,
                )
                assert response == EXPECTED_CONTENT


@pytest.mark.asyncio
async def test_get_content_when_filesize_is_large():
    """Tests the get content method for file size greater than max limit."""

    async with create_jira_source() as source:
        with mock.patch("aiohttp.ClientSession.get", return_value=get_stream_reader()):
            with mock.patch(
                "aiohttp.StreamReader.iter_any",
                return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
            ):
                attachment = copy(MOCK_ATTACHMENT[0])
                attachment["size"] = 23000000

                response = await source.get_content(
                    issue_key="TP-1",
                    attachment=attachment,
                    doit=True,
                )
                assert response is None


@pytest.mark.asyncio
async def test_get_content_for_unsupported_filetype():
    """Tests the get content method for file type is not supported."""

    async with create_jira_source() as source:
        with mock.patch("aiohttp.ClientSession.get", return_value=get_stream_reader()):
            with mock.patch(
                "aiohttp.StreamReader.iter_any",
                return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
            ):
                attachment = copy(EXPECTED_ATTACHMENT)
                attachment["filename"] = "testfile.xyz"

                response = await source.get_content(
                    issue_key="TP-1",
                    attachment=attachment,
                    doit=True,
                )
                assert response is None


@pytest.mark.asyncio
async def test_get_consumer():
    """Test _get_consumer method"""
    async with create_jira_source() as source:
        source.tasks = 3
        await source.queue.put((EXPECTED_PROJECT[0], None))
        await source.queue.put(("FINISHED"))
        await source.queue.put((EXPECTED_ISSUE, None))
        await source.queue.put(("FINISHED"))
        await source.queue.put((EXPECTED_ATTACHMENT, None))
        await source.queue.put(("FINISHED"))

        items = []
        async for item, _ in source._consumer():
            items.append(item)

        assert items == [EXPECTED_PROJECT[0], EXPECTED_ISSUE, EXPECTED_ATTACHMENT]


@freeze_time("2023-01-24T04:07:19")
@pytest.mark.asyncio
async def test_get_docs():
    """Test _get_docs method"""
    async with create_jira_source() as source:
        source.jira_client.projects = ["*"]
        source.get_content = Mock(return_value=EXPECTED_CONTENT)

        EXPECTED_RESPONSES = [
            EXPECTED_ISSUE,
            EXPECTED_ISSUE_TYPE_BUG,
            EXPECTED_ATTACHMENT,
            EXPECTED_ATTACHMENT_FOR_BUG,
            *EXPECTED_PROJECT,
        ]
        with mock.patch.object(
            source.jira_client._get_session(), "get", side_effect=side_effect_function
        ):
            async for item, _ in source.get_docs():
                assert item in EXPECTED_RESPONSES


@pytest.mark.parametrize(
    "filtering, expected_docs",
    [
        (
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": [{"query": "type=task"}, {"query": "type=bug"}]
                    }
                }
            ),
            [
                EXPECTED_ISSUE,
                EXPECTED_ATTACHMENT,
                EXPECTED_ISSUE_TYPE_BUG,
                EXPECTED_ATTACHMENT_TYPE_BUG,
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules(filtering, expected_docs):
    async with create_jira_source() as source:
        source.get_content = Mock(return_value=EXPECTED_ATTACHMENT_TYPE_BUG)

        yielded_docs = []
        with mock.patch.object(
            source.jira_client._get_session(), "get", side_effect=side_effect_function
        ):
            async for item, _ in source.get_docs(filtering):
                yielded_docs.append(item)
        assert yielded_docs == expected_docs


@pytest.mark.asyncio
async def test_get_access_control_dls_disabled():
    async with create_jira_source() as source:
        source._dls_enabled = MagicMock(return_value=False)

        access_control_list = []
        async for access_control in source.get_access_control():
            access_control_list.append(access_control)

        assert len(access_control_list) == 0


@pytest.mark.asyncio
@freeze_time("2023-01-24T04:07:19")
async def test_get_access_control_dls_enabled():
    mock_users = [
        {
            # Indexable: The user is active and atlassian user.
            "self": "url1",
            "accountId": "607194d6bc3c3f006f4c35d6",
            "accountType": "atlassian",
            "displayName": "user1",
            "locale": "en-US",
            "emailAddress": "user1@dummy-domain.com",
            "active": True,
        },
        {
            # Non-Indexable: The user is no longer active.
            "self": "url2",
            "accountId": "607194d6bc3c3f006f4c35d7",
            "accountType": "atlassian",
            "displayName": "user2",
            "locale": "en-US",
            "emailAddress": "user2@dummy-domain.com",
            "active": False,
        },
        {
            # Non-Indexable: User account type is app; it must be atlassian.
            "self": "url2",
            "accountId": "607194d6bc3c3f006f4c35d7",
            "accountType": "app",
            "displayName": "user2",
            "active": False,
        },
        {
            # Non-Indexable: Personal information about user is missing.
            "accountId": "607194d6bc3c3f006f4c35d7",
            "accountType": "app",
            "displayName": "user2",
            "active": False,
        },
    ]

    mock_user1 = {
        "self": "url1",
        "accountId": "607194d6bc3c3f006f4c35d6",
        "accountType": "atlassian",
        "displayName": "user1",
        "active": True,
        "emailAddress": "user1.test@gmail.com",
        "locale": "US",
        "groups": {
            "size": 1,
            "items": [
                {
                    "name": "group1",
                    "groupId": "607194d6bc3c3f006f4c35d8",
                }
            ],
        },
        "applicationRoles": {
            "size": 0,
            "items": [
                {
                    "name": "role1",
                    "key": "607194d6bc3c3f006f4c35d9",
                }
            ],
        },
    }

    expected_user_doc = {
        "_id": "607194d6bc3c3f006f4c35d6",
        "identity": {
            "account_id": "account_id:607194d6bc3c3f006f4c35d6",
            "display_name": "name:user1",
            "email_address": "email_address:user1.test@gmail.com",
            "locale": "locale:US",
        },
        "created_at": "2023-01-24T04:07:19+00:00",
        "query": {
            "template": {
                "params": {
                    "access_control": [
                        "account_id:607194d6bc3c3f006f4c35d6",
                        "group_id:607194d6bc3c3f006f4c35d8",
                        "role_key:607194d6bc3c3f006f4c35d9",
                    ]
                }
            },
            "source": {
                "bool": {
                    "filter": {
                        "bool": {
                            "should": [
                                {
                                    "terms": {
                                        "_allow_access_control.enum": [
                                            "account_id:607194d6bc3c3f006f4c35d6",
                                            "group_id:607194d6bc3c3f006f4c35d8",
                                            "role_key:607194d6bc3c3f006f4c35d9",
                                        ]
                                    }
                                },
                            ]
                        }
                    }
                }
            },
        },
    }

    async with create_jira_source() as source:
        source._dls_enabled = MagicMock(return_value=True)

        source.atlassian_access_control.fetch_all_users = AsyncIterator([mock_users])
        source.atlassian_access_control.fetch_user = AsyncIterator([mock_user1])

        user_documents = []
        async for user_doc in source.get_access_control():
            user_documents.append(user_doc)

        assert expected_user_doc in user_documents


@freeze_time("2023-01-24T04:07:19")
@pytest.mark.asyncio
async def test_get_docs_with_dls_enabled():
    expected_projects_with_access_controls = [
        project
        | {
            "_allow_access_control": [
                "account_id:607194d6bc3c3f006f4c35d6",
                "name:user1",
            ]
        }
        for project in copy(EXPECTED_PROJECT)
    ]

    EXPECTED_RESPONSES = [
        copy(EXPECTED_ISSUE)
        | {
            "_allow_access_control": [
                "account_id:5ff5815e34847e0069fedee3",
                "account_id:user-1",
                "group_id:d614bbb8-703d-4d41-a51a-0c3cfcef9318",
                "name:Test-User",
            ]
        },
        copy(EXPECTED_ISSUE_TYPE_BUG)
        | {
            "_allow_access_control": [
                "account_id:5ff5815e34847e0069fedee3",
                "group_id:d614bbb8-703d-4d41-a51a-0c3cfcef9318",
                "name:Test-User",
            ]
        },
        copy(EXPECTED_ATTACHMENT)
        | {
            "_allow_access_control": [
                "account_id:5ff5815e34847e0069fedee3",
                "account_id:user-1",
                "group_id:d614bbb8-703d-4d41-a51a-0c3cfcef9318",
                "name:Test-User",
            ]
        },
        copy(EXPECTED_ATTACHMENT_FOR_BUG)
        | {
            "_allow_access_control": [
                "account_id:5ff5815e34847e0069fedee3",
                "group_id:d614bbb8-703d-4d41-a51a-0c3cfcef9318",
                "name:Test-User",
            ]
        },
        *expected_projects_with_access_controls,
    ]

    async with create_jira_source() as source:
        source._dls_enabled = MagicMock(return_value=True)
        source.jira_client.projects = ["*"]
        source.get_content = Mock(return_value=EXPECTED_CONTENT)

        with mock.patch.object(
            source.jira_client._get_session(), "get", side_effect=side_effect_function
        ):
            async for item, _ in source.get_docs():
                item.get(ACCESS_CONTROL, []).sort()
                assert item in EXPECTED_RESPONSES
