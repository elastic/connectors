#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Jira database source class methods"""
import ssl
from unittest import mock
from unittest.mock import Mock, patch

import aiohttp
import pytest
from aiohttp import StreamReader

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.jira import JiraDataSource
from connectors.sources.tests.support import create_source
from connectors.utils import ssl_context

HOST_URL = "http://127.0.0.1:8080"
MOCK_PROJECT = {
    "_id": "dummy_project-test123",
    "Type": "Project",
    "Project": {"name": "dummy_project", "id": "test123"},
}
EXPECTED_PROJECT = {
    "_id": "dummy_project-test123",
    "Type": "Project",
    "_timestamp": "2023-02-01:01:02:20",
    "Project": {"name": "dummy_project", "id": "test123"},
}
EXPECTED_ISSUE = {
    "_id": "dummy_project-test123-1234",
    "_timestamp": "2023-02-01:01:02:20",
    "Issue": {
        "key": "1234",
        "fields": {
            "project": {"name": "test_project"},
            "updated": "2023-02-01:01:02:20",
            "issuetype": {"name": "test_project"},
        },
    },
}
MOCK_ATTACHMENT = [
    {
        "id": 10001,
        "filename": "test_file.txt",
        "created": "2023-02-01T06:35:48.497+0000",
        "size": 200,
    }
]
EXPECTED_ATTACHMENT = {
    "_id": "TP-1-10001",
    "title": "test_file.txt",
    "type": "Attachment",
    "size": 200,
    "_timestamp": "2023-02-01T06:35:48.497+0000",
}


class mock_ssl:
    """This class contains methods which returns dummy ssl context"""

    def load_verify_locations(self, cadata):
        """This method verify locations"""
        pass


class MockResponse:
    """Mock class for ClientResponse"""

    def __init__(self, json, status):
        self._json = json
        self.status = status

    async def json(self):
        """This Method is used to return a json response"""
        return self._json

    async def __aexit__(self, exc_type, exc, tb):
        """Closes an async with block"""
        pass

    async def __aenter__(self):
        """Enters an async with block"""
        return self


class MockObjectResponse:
    """Class to mock object response of aiohttp session.get method"""

    def __init__(self):
        """Setup a streamReader object"""
        self.content = StreamReader

    async def __aexit__(self, exc_type, exc, tb):
        """Closes an async with block"""
        pass

    async def __aenter__(self):
        """Enters an async with block"""
        return self


class AsyncIter:
    """This Class is use to return async generator"""

    def __init__(self, item):
        """Setup list of dictionary"""
        self.item = item

    async def __aiter__(self):
        """This Method is used to return async generator"""
        yield self.item

    async def __anext__(self):
        """This Method is used to return one document"""
        return self.item[0]


def side_effect_function(url, ssl):
    """Dynamically changing return values for API calls
    Args:
        url, ssl: Params required for get call
    """
    if url == f"{HOST_URL}/rest/api/2/search?maxResults=100&startAt=0":
        mocked_issue_response = {"issues": [{"key": "1234"}], "total": 101}
        return MockResponse(mocked_issue_response, 200)
    elif url == f"{HOST_URL}/rest/api/2/issue/1234":
        mocked_issue_data = {
            "key": "1234",
            "fields": {
                "project": {"name": "test_project"},
                "updated": "2023-02-01:01:02:20",
                "issuetype": {"name": "test_project"},
            },
        }
        return MockResponse(mocked_issue_data, 200)
    elif url == f"{HOST_URL}/rest/api/2/search?maxResults=100&startAt=100":
        mocked_issue_data = {"issues": [{"key": "1234"}], "total": 1}
        return MockResponse(mocked_issue_data, 200)


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
    """Tests the _api_call function while getting an exception."""

    # Setup
    source = create_source(JiraDataSource)
    source.retry_count = 0

    # Execute
    with patch.object(
        aiohttp.ClientSession, "get", side_effect=Exception("Something went wrong")
    ):
        source._generate_session()
        with pytest.raises(Exception):
            await anext(source._api_call(url_name="ping"))


@pytest.mark.asyncio
async def test_api_call_when_server_is_down():
    """Tests the _api_call function while server gets disconnected."""

    # Setup
    source = create_source(JiraDataSource)
    source.retry_count = 0

    # Execute
    with patch.object(
        aiohttp.ClientSession,
        "get",
        side_effect=aiohttp.ServerDisconnectedError("Something went wrong"),
    ):
        source._generate_session()
        with pytest.raises(aiohttp.ServerDisconnectedError):
            await anext(source._api_call(url_name="ping"))


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_with_ssl(mock_get, patch_logger):
    """Test ping method of JiraDataSource class with SSL"""

    # Execute
    mock_get.return_value.__aenter__.return_value.status = 200
    source = create_source(JiraDataSource)

    source.ssl_enabled = True
    source.certificate = (
        "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"
    )

    # Execute
    with patch.object(ssl, "create_default_context", return_value=mock_ssl()):
        source.ssl_ctx = ssl_context(certificate=source.certificate)
        await source.ping()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_for_failed_connection_exception(patch_logger):
    """Tests the ping functionality when connection can not be established to Jira."""

    # Setup
    source = create_source(JiraDataSource)

    # Execute
    with patch.object(
        JiraDataSource, "_api_call", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await source.ping()


@pytest.mark.asyncio
async def test_validate_config_for_ssl_enabled(patch_logger):
    """This function test _validate_configuration when certification is empty when ssl is enabled"""
    # Setup
    source = create_source(JiraDataSource)
    source.ssl_enabled = True

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
    options = {"concurrent_downloads": 10}

    # Execute
    source.tweak_bulk_options(options)


@pytest.mark.asyncio
async def test_close():
    """Test close method for closing the unclosed session"""

    # Setup
    source = create_source(JiraDataSource)

    # Execute
    source.session = None
    await source.close()


@pytest.mark.asyncio
async def test_get_projects(patch_logger):
    """Test _get_projects method"""
    # Setup
    source = create_source(JiraDataSource)
    mocked_project_response = [{"name": "dummy_project", "id": "test123"}]
    async_project_response = MockResponse(mocked_project_response, 200)
    excepted_project_response = {
        "_id": "dummy_project-test123",
        "Type": "Project",
        "Project": {"name": "dummy_project", "id": "test123"},
    }

    mocked_myself_response = {
        "name": "admin",
        "emailAddress": "admin@local.com",
        "displayName": "admin@local.com",
        "timeZone": "Asia/Kolkata",
    }
    myself_mock = MockResponse(mocked_myself_response, 200)

    # Execute and Assert
    with patch(
        "aiohttp.ClientSession.get", side_effect=[myself_mock, async_project_response]
    ):
        source._generate_session()
        async for response in source._get_projects():
            del response["_timestamp"]
            assert response == excepted_project_response


@pytest.mark.asyncio
async def test_get_issues(patch_logger):
    """Test _get_issues method"""
    # Setup
    source = create_source(JiraDataSource)
    expected_response = {
        "_id": "test_project-1234",
        "_timestamp": "2023-02-01:01:02:20",
        "Type": "test_project",
        "Issue": {
            "project": {"name": "test_project"},
            "updated": "2023-02-01:01:02:20",
            "issuetype": {"name": "test_project"},
        },
    }

    # Execute and Assert
    with patch("aiohttp.ClientSession.get", side_effect=side_effect_function):
        source._generate_session()
        async for issue_data, _ in source._get_issues():
            assert expected_response == issue_data


@pytest.mark.asyncio
async def test_get_attachments_positive(patch_logger):
    """Test _get_attachments method"""
    # Setup
    source = create_source(JiraDataSource)
    attachments1 = [
        {
            "id": 10001,
            "filename": "test_file.txt",
            "created": "2023-02-01T06:35:48.497+0000",
            "size": 200,
        }
    ]
    excepted_response = {
        "_id": "TP-1-10001",
        "title": "test_file.txt",
        "type": "Attachment",
        "issue": "TP-1",
        "size": 200,
        "_timestamp": "2023-02-01T06:35:48.497+0000",
    }

    # Execute and Assert
    async for attachment, _ in source._get_attachments(attachments1, "TP-1"):
        assert attachment == excepted_response


@pytest.mark.asyncio
async def test_get_content(patch_logger):
    """Tests the get content method."""
    # Setup
    source = create_source(JiraDataSource)

    async_response = MockObjectResponse()

    RESPONSE_CONTENT = "# This is the dummy file"
    EXPECTED_ATTACHMENT = {
        "id": "att3637249",
        "type": "attachment",
        "created": "2023-01-03T09:24:50.633Z",
        "filename": "demo.py",
        "size": 230,
    }
    EXPECTED_CONTENT = {
        "_id": "TP-1-att3637249",
        "_timestamp": "2023-01-03T09:24:50.633Z",
        "_attachment": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl",
    }

    # Execute and Assert
    with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
        source._generate_session()
        with mock.patch(
            "aiohttp.StreamReader.iter_chunked",
            return_value=AsyncIter(bytes(RESPONSE_CONTENT, "utf-8")),
        ):
            response = await source.get_content(
                issue_key="TP-1",
                attachment=EXPECTED_ATTACHMENT,
                doit=True,
            )
            assert response == EXPECTED_CONTENT


@pytest.mark.asyncio
async def test_get_content_when_filesize_is_large(patch_logger):
    """Tests the get content method for file size greater than max limit."""
    # Setup
    source = create_source(JiraDataSource)

    async_response = MockObjectResponse()

    RESPONSE_CONTENT = "# This is the dummy file"
    EXPECTED_ATTACHMENT = {
        "id": "att3637249",
        "type": "attachment",
        "created": "2023-01-03T09:24:50.633Z",
        "filename": "demo.py",
        "size": 230,
    }

    # Execute and Assert
    with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
        with mock.patch(
            "aiohttp.StreamReader.iter_any",
            return_value=AsyncIter(bytes(RESPONSE_CONTENT, "utf-8")),
        ):
            attachment = EXPECTED_ATTACHMENT
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

    async_response = MockObjectResponse()

    RESPONSE_CONTENT = "# This is the dummy file"
    EXPECTED_ATTACHMENT = {
        "id": "att3637249",
        "type": "attachment",
        "created": "2023-01-03T09:24:50.633Z",
        "filename": "demo.py",
        "size": 230,
    }

    # Execute and Assert
    with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
        with mock.patch(
            "aiohttp.StreamReader.iter_any",
            return_value=AsyncIter(bytes(RESPONSE_CONTENT, "utf-8")),
        ):
            attachment = EXPECTED_ATTACHMENT
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
    source._get_attachments = Mock(return_value=AsyncIter(issue_data))
    source.get_content = Mock(return_value={"id": "123"})

    # Execute
    await source._grab_content("TP-1", issue_data[0])


@pytest.mark.asyncio
async def test_get_docs():
    """Test _get_docs method"""
    # Setup
    source = create_source(JiraDataSource)
    source._get_projects = Mock(
        return_value=AsyncIter(
            {
                "_id": "dummy_project-test123",
                "Type": "Project",
                "Project": {"name": "dummy_project", "id": "test123"},
            }
        )
    )
    source._get_issues = Mock(
        return_value=AsyncIter(
            (
                {
                    "_id": "test_project-1234",
                    "_timestamp": "2023-02-01:01:02:20",
                    "Type": "Task",
                    "Issue": {
                        "project": {"name": "test_project"},
                        "updated": "2023-02-01:01:02:20",
                        "issuetype": {"name": "Task"},
                    },
                },
                {
                    "key": "TP-1234",
                    "fields": {
                        "attachment": [
                            {
                                "id": "test_1",
                                "filename": "test_file.txt",
                                "created": "2023-01-03T09:24:50.633Z",
                                "size": 200,
                            }
                        ]
                    },
                },
            )
        )
    )
    attachment_data = (
        {
            "_id": "TP-1234-test_1",
            "title": "test_file.txt",
            "type": "Attachment",
            "issue": "TP-1234",
            "_timestamp": "2023-01-03T09:24:50.633Z",
            "size": 200,
        },
        {
            "id": "test_1",
            "filename": "test_file.txt",
            "created": "2023-01-03T09:24:50.633Z",
            "size": 200,
        },
    )
    source._get_attachments = Mock(return_value=AsyncIter(attachment_data))
    source.get_content = Mock(
        return_value={
            "_id": "TP-123-test-1",
            "_timestamp": "2023-01-03T09:24:50.633Z",
            "_attachment": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl",
        }
    )

    expected_docs = [
        {
            "_id": "dummy_project-test123",
            "Type": "Project",
            "Project": {"name": "dummy_project", "id": "test123"},
        },
        {
            "_id": "test_project-1234",
            "_timestamp": "2023-02-01:01:02:20",
            "Type": "Task",
            "Issue": {
                "project": {"name": "test_project"},
                "updated": "2023-02-01:01:02:20",
                "issuetype": {"name": "Task"},
            },
        },
        {
            "_id": "TP-1234-test_1",
            "title": "test_file.txt",
            "type": "Attachment",
            "issue": "TP-1234",
            "_timestamp": "2023-01-03T09:24:50.633Z",
            "size": 200,
        },
    ]

    # Execute
    documents = []
    async for item, _ in source.get_docs():
        documents.append(item)

    assert documents == expected_docs
