#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Jira database source class methods"""
import ssl
from copy import copy
from unittest import mock
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest
from aiohttp import StreamReader
from freezegun import freeze_time

from connectors.protocol import Filter
from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.jira import JiraClient, JiraDataSource
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
        "Type": "Project",
        "_timestamp": "2023-01-24T09:37:19+05:30",
        "Project": {"id": "1", "name": "dummy_project", "key": "DP"},
    },
    {
        "_id": "project-2",
        "Type": "Project",
        "_timestamp": "2023-01-24T09:37:19+05:30",
        "Project": {"id": "2", "name": "test_project", "key": "TP"},
    },
]

MOCK_ISSUE = {
    "id": "1234",
    "key": "TP-1",
    "fields": {
        "project": {"name": "test_project"},
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
    },
}
EXPECTED_ISSUE = {
    "_id": "test_project-TP-1",
    "_timestamp": "2023-02-01T01:02:20",
    "Type": "Task",
    "Issue": {
        "project": {"name": "test_project"},
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
    },
}

MOCK_ISSUE_TYPE_BUG = {
    "id": "1234",
    "key": "TP-2",
    "fields": {
        "project": {"name": "test_project"},
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
    },
}
EXPECTED_ISSUE_TYPE_BUG = {
    "_id": "test_project-TP-2",
    "_timestamp": "2023-02-01T01:02:20",
    "Type": "Bug",
    "Issue": {
        "project": {"name": "test_project"},
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

RESPONSE_CONTENT = "# This is the dummy file"
EXPECTED_CONTENT = {
    "_id": "TP-1-10001",
    "_timestamp": "2023-02-01T01:02:20",
    "_attachment": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl",
}


class mock_ssl:
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
        mocked_issue_data = {"issues": [MOCK_ISSUE], "total": 1}
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


@pytest.mark.asyncio
async def test_configuration():
    """Tests the get configurations method of the Jira source class."""
    # Setup
    klass = JiraDataSource

    # Execute
    config = DataSourceConfiguration(config=klass.get_default_configuration())

    # Assert
    assert config["jira_url"] == HOST_URL


@pytest.mark.parametrize(
    "field, is_cloud",
    [
        ("jira_url", True),
        ("projects", True),
        ("api_token", True),
        ("account_email", True),
        ("username", False),
        ("password", False),
    ],
)
@pytest.mark.asyncio
async def test_validate_configuration_for_empty_fields(field, is_cloud):
    source = create_source(JiraDataSource)
    source.jira_client.is_cloud = is_cloud
    source.jira_client.configuration.set_field(name=field, value="")

    # Execute
    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_api_call_negative():
    """Tests the api_call function while getting an exception."""

    # Setup
    source = create_source(JiraDataSource)
    source.jira_client.retry_count = 0

    # Execute
    with patch.object(
        aiohttp.ClientSession, "get", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await anext(source.jira_client.api_call(url_name="ping"))

    with patch.object(
        aiohttp.ClientSession, "get", side_effect=Exception("Something went wrong")
    ):
        source.jira_client.is_cloud = False
        with pytest.raises(Exception):
            await anext(source.jira_client.api_call(url_name="ping"))
    await source.close()


@pytest.mark.asyncio
async def test_api_call_when_server_is_down():
    """Tests the api_call function while server gets disconnected."""

    # Setup
    source = create_source(JiraDataSource)
    source.jira_client.retry_count = 0

    # Execute
    with patch.object(
        aiohttp.ClientSession,
        "get",
        side_effect=aiohttp.ServerDisconnectedError("Something went wrong"),
    ):
        with pytest.raises(aiohttp.ServerDisconnectedError):
            await anext(source.jira_client.api_call(url_name="ping"))
    await source.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_with_ssl(
    mock_get,
):
    """Test ping method of JiraDataSource class with SSL"""

    # Execute
    mock_get.return_value.__aenter__.return_value.status = 200
    source = create_source(JiraDataSource)

    source.jira_client.ssl_enabled = True
    source.jira_client.certificate = (
        "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"
    )

    # Execute
    with patch.object(ssl, "create_default_context", return_value=mock_ssl()):
        source.jira_client.ssl_ctx = ssl_context(
            certificate=source.jira_client.certificate
        )
        await source.ping()
    await source.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_for_failed_connection_exception(client_session_get):
    """Tests the ping functionality when connection can not be established to Jira."""

    # Setup
    source = create_source(JiraDataSource)

    # Execute
    with patch.object(
        JiraClient, "api_call", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await source.ping()


@pytest.mark.asyncio
async def test_validate_config_for_ssl_enabled_when_ssl_ca_empty_raises_error():
    """This function test _validate_configuration when certification is empty when ssl is enabled"""
    # Setup
    source = create_source(JiraDataSource, ssl_enabled=True)

    # Execute
    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_invalid_concurrent_downloads():
    """Test validate_config method of BaseDataSource class with invalid concurrent downloads"""

    # Setup
    source = create_source(JiraDataSource, concurrent_downloads=1000)

    # Execute
    with pytest.raises(
        ConfigurableFieldValueError,
        match="Field validation errors: 'Maximum concurrent downloads' value '1000' should be less than 101.",
    ):
        await source.validate_config()


def test_tweak_bulk_options():
    """Test tweak_bulk_options method of BaseDataSource class"""

    # Setup
    source = create_source(JiraDataSource)
    source.concurrent_downloads = 10
    options = {"concurrent_downloads": 5}

    # Execute
    source.tweak_bulk_options(options)

    assert options["concurrent_downloads"] == 10


@pytest.mark.asyncio
async def test_get_session():
    """Test that the instance of session returned is always the same for the datasource class."""
    source = create_source(JiraDataSource)
    first_instance = source.jira_client._get_session()
    second_instance = source.jira_client._get_session()
    assert first_instance is second_instance
    await source.close()


@pytest.mark.asyncio
async def test_close_with_client_session():
    # Setup
    source = create_source(JiraDataSource)
    source.jira_client._get_session()

    # Execute
    await source.close()

    assert source.jira_client.session is None


@pytest.mark.asyncio
async def test_close_without_client_session():
    """Test close method when the session does not exist"""
    # Setup
    source = create_source(JiraDataSource)

    # Execute
    await source.close()

    assert source.jira_client.session is None


@pytest.mark.asyncio
async def test_get_timezone():
    # Setup
    source = create_source(JiraDataSource)

    # Execute and Assert
    with patch.object(aiohttp.ClientSession, "get", side_effect=side_effect_function):
        timezone = await source._get_timezone()
        assert timezone == "Asia/Kolkata"
    await source.close()


@freeze_time("2023-01-24T04:07:19")
@pytest.mark.asyncio
async def test_get_projects():
    """Test _get_projects method"""
    # Setup
    source = create_source(JiraDataSource)

    # Execute and Assert
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
    await source.close()


@freeze_time("2023-01-24T04:07:19")
@pytest.mark.asyncio
async def test_get_projects_for_specific_project():
    """Test _get_projects method for specific project key"""
    # Setup
    source = create_source(JiraDataSource)
    source.jira_client.projects = ["DP"]
    async_project_response = AsyncMock()
    async_project_response.__aenter__ = AsyncMock(
        return_value=JSONAsyncMock(MOCK_PROJECT[0])
    )

    myself_mock = AsyncMock()
    myself_mock.__aenter__ = AsyncMock(return_value=JSONAsyncMock(MOCK_MYSELF))

    # Execute and Assert
    with patch(
        "aiohttp.ClientSession.get", side_effect=[myself_mock, async_project_response]
    ):
        await source._get_projects()
        _, project = await source.queue.get()
        assert project[0] == EXPECTED_PROJECT[0]
    await source.close()


@pytest.mark.asyncio
async def test_verify_projects():
    """Test _verify_projects method"""
    source = create_source(JiraDataSource)
    source.jira_client.projects = ["TP", "DP"]

    with patch("aiohttp.ClientSession.get", side_effect=side_effect_function):
        await source._verify_projects()
    await source.close()


@pytest.mark.asyncio
async def test_verify_projects_with_unavailable_project_keys():
    source = create_source(JiraDataSource)
    source.jira_client.projects = ["TP", "AP"]

    with patch("aiohttp.ClientSession.get", side_effect=side_effect_function):
        with pytest.raises(Exception, match="Configured unavailable projects: AP"):
            await source._verify_projects()
    await source.close()


@pytest.mark.asyncio
async def test_put_issue():
    """Test _put_issue method"""
    # Setup
    source = create_source(JiraDataSource)

    # Execute and Assert
    source.get_content = Mock(return_value=EXPECTED_CONTENT)

    with patch("aiohttp.ClientSession.get", side_effect=side_effect_function):
        await source._put_issue(issue=MOCK_ISSUE)
        assert source.queue.qsize() == 3
    await source.close()


@pytest.mark.asyncio
async def test_put_attachment_positive():
    """Test _put_attachment method"""
    # Setup
    source = create_source(JiraDataSource)

    source.get_content = Mock(
        return_value={
            "_id": "TP-123-test-1",
            "_timestamp": "2023-01-03T09:24:50.633Z",
            "_attachment": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl",
        }
    )
    await source._put_attachment(attachments=MOCK_ATTACHMENT, issue_key="TP-1")
    assert source.queue.qsize() == 1


@pytest.mark.asyncio
async def test_get_content():
    """Tests the get content method."""
    # Setup
    source = create_source(JiraDataSource)

    # Execute and Assert
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
    await source.close()


@pytest.mark.asyncio
async def test_get_content_with_upper_extension():
    """Tests the get content method."""
    # Setup
    source = create_source(JiraDataSource)

    # Execute and Assert
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
    await source.close()


@pytest.mark.asyncio
async def test_get_content_when_filesize_is_large():
    """Tests the get content method for file size greater than max limit."""
    # Setup
    source = create_source(JiraDataSource)

    RESPONSE_CONTENT = "# This is the dummy file"

    # Execute and Assert
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
    await source.close()


@pytest.mark.asyncio
async def test_get_content_for_unsupported_filetype():
    """Tests the get content method for file type is not supported."""
    # Setup
    source = create_source(JiraDataSource)

    RESPONSE_CONTENT = "# This is the dummy file"

    # Execute and Assert
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
    await source.close()


@pytest.mark.asyncio
async def test_get_consumer():
    """Test _get_consumer method"""
    source = create_source(JiraDataSource)

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
    source = create_source(JiraDataSource)

    source.jira_client.projects = ["*"]
    source.get_content = Mock(return_value=EXPECTED_CONTENT)

    EXPECTED_RESPONSES = [EXPECTED_ISSUE, EXPECTED_ATTACHMENT, *EXPECTED_PROJECT]
    with mock.patch.object(
        source.jira_client._get_session(), "get", side_effect=side_effect_function
    ):
        async for item, _ in source.get_docs():
            assert item in EXPECTED_RESPONSES
    await source.close()


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
    source = create_source(JiraDataSource)

    source.get_content = Mock(return_value=EXPECTED_ATTACHMENT_TYPE_BUG)

    yielded_docs = []
    with mock.patch.object(
        source.jira_client._get_session(), "get", side_effect=side_effect_function
    ):
        async for item, _ in source.get_docs(filtering):
            yielded_docs.append(item)

    assert yielded_docs == expected_docs
    await source.close()
