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

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.jira import JiraClient, JiraDataSource
from connectors.sources.tests.support import create_source
from connectors.tests.commons import AsyncIterator
from connectors.utils import ssl_context

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
        "_id": "dummy_project-1",
        "Type": "Project",
        "_timestamp": "2023-01-24T09:37:19+05:30",
        "Project": {"id": "1", "name": "dummy_project", "key": "DP"},
    },
    {
        "_id": "test_project-2",
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
    if url == f"{HOST_URL}/rest/api/2/search?maxResults=100&startAt=0":
        mocked_issue_response = {"issues": [MOCK_ISSUE], "total": 101}
        return get_json_mock(mock_response=mocked_issue_response)
    elif url == f"{HOST_URL}/rest/api/2/issue/TP-1":
        return get_json_mock(mock_response=MOCK_ISSUE)
    elif url == f"{HOST_URL}/rest/api/2/search?maxResults=100&startAt=100":
        mocked_issue_data = {"issues": [MOCK_ISSUE], "total": 1}
        return get_json_mock(mock_response=mocked_issue_data)
    elif url == f"{HOST_URL}/rest/api/2/myself":
        return get_json_mock(mock_response=MOCK_MYSELF)
    elif url == f"{HOST_URL}/rest/api/2/project?expand=description,lead,url":
        return get_json_mock(mock_response=MOCK_PROJECT)


@pytest.mark.asyncio
async def test_configuration(patch_logger):
    """Tests the get configurations method of the Jira source class."""
    # Setup
    klass = JiraDataSource

    # Execute
    config = DataSourceConfiguration(config=klass.get_default_configuration())

    # Assert
    assert config["host_url"] == HOST_URL


@pytest.mark.asyncio
async def test_validate_config_for_host_url(patch_logger):
    """This function test validate_config when host_url is invalid"""
    # Setup
    source = create_source(JiraDataSource)
    source.configuration.set_field(name="host_url", value="")

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


@pytest.mark.asyncio
async def test_api_call_when_server_is_down(patch_logger):
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


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_with_ssl(mock_get, patch_logger):
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


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_for_failed_connection_exception(patch_logger):
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
async def test_validate_config_for_ssl_enabled(patch_logger):
    """This function test _validate_configuration when certification is empty when ssl is enabled"""
    # Setup
    source = create_source(JiraDataSource)
    source.jira_client.ssl_enabled = True

    # Execute
    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_invalid_concurrent_downloads(patch_logger):
    """Test validate_config method of BaseDataSource class with invalid concurrent downloads"""

    # Setup
    source = create_source(JiraDataSource)
    source.concurrent_downloads = 1000

    # Execute
    with pytest.raises(
        ConfigurableFieldValueError,
        match="Configured concurrent downloads can't be set more than *",
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
async def test_close_with_client_session(patch_logger):
    # Setup
    source = create_source(JiraDataSource)
    source.jira_client._get_session()

    # Execute
    await source.close()

    assert source.jira_client.session is None


@pytest.mark.asyncio
async def test_close_without_client_session(patch_logger):
    """Test close method when the session does not exist"""
    # Setup
    source = create_source(JiraDataSource)

    # Execute
    await source.close()

    assert source.jira_client.session is None


@pytest.mark.asyncio
async def test_get_timezone(patch_logger):
    # Setup
    source = create_source(JiraDataSource)

    # Execute and Assert
    with patch.object(aiohttp.ClientSession, "get", side_effect=side_effect_function):
        timezone = await source._get_timezone()
        assert timezone == "Asia/Kolkata"


@freeze_time("2023-01-24T04:07:19")
@pytest.mark.asyncio
async def test_get_projects(patch_logger):
    """Test _get_projects method"""
    # Setup
    source = create_source(JiraDataSource)

    # Execute and Assert
    with patch.object(aiohttp.ClientSession, "get", side_effect=side_effect_function):
        source_projects = []
        async for project in source._get_projects():
            source_projects.append(project)
        assert source_projects == EXPECTED_PROJECT


@pytest.mark.asyncio
async def test_get_issues(patch_logger):
    """Test _get_issues method"""
    # Setup
    source = create_source(JiraDataSource)

    # Execute and Assert
    with patch("aiohttp.ClientSession.get", side_effect=side_effect_function):
        async for issue_data, _ in source._get_issues():
            assert issue_data == EXPECTED_ISSUE


@pytest.mark.asyncio
async def test_get_attachments_positive(patch_logger):
    """Test _get_attachments method"""
    # Setup
    source = create_source(JiraDataSource)

    # Execute and Assert
    async for attachment, _ in source._get_attachments(MOCK_ATTACHMENT, "TP-1"):
        assert attachment == EXPECTED_ATTACHMENT


@pytest.mark.asyncio
async def test_get_content(patch_logger):
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


@pytest.mark.asyncio
async def test_get_content_when_filesize_is_large(patch_logger):
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


@pytest.mark.asyncio
async def test_get_content_for_unsupported_filetype(patch_logger):
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


@pytest.mark.asyncio
async def test_grab_content(patch_logger):
    """Test _grab_content method"""
    # Setup
    source = create_source(JiraDataSource)
    issue_data = (
        {
            "key": "1234",
            "fields": {
                "attachment": [
                    {"id": "test_1234", "filename": "test_file.txt", "size": 200}
                ]
            },
        },
        {"id": "test_1234", "filename": "test_file.txt", "size": 200},
    )
    source._get_attachments = Mock(return_value=AsyncIterator([issue_data]))
    source.get_content = Mock(return_value={"id": "123"})

    # Execute
    await source._grab_content("TP-1", issue_data[0])


@freeze_time("2023-01-24T04:07:19")
@pytest.mark.asyncio
async def test_get_docs():
    """Test _get_docs method"""
    # Setup
    source = create_source(JiraDataSource)
    source._get_projects = Mock(return_value=AsyncIterator([*EXPECTED_PROJECT]))
    source._get_issues = Mock(
        return_value=AsyncIterator([(EXPECTED_ISSUE, MOCK_ISSUE)])
    )
    source._get_attachments = Mock(
        return_value=AsyncIterator([(EXPECTED_ATTACHMENT, MOCK_ATTACHMENT)])
    )
    source.get_content = Mock(return_value=EXPECTED_CONTENT)

    expected_docs = [
        *EXPECTED_PROJECT,
        EXPECTED_ISSUE,
        EXPECTED_ATTACHMENT,
    ]

    # Execute
    documents = []
    async for item, _ in source.get_docs():
        documents.append(item)

    assert documents == expected_docs
