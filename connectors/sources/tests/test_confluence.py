#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Confluence database source class methods"""
import ssl
from copy import copy
from unittest import mock
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp import StreamReader
from freezegun import freeze_time

from connectors.source import DataSourceConfiguration
from connectors.sources.confluence import ConfluenceClient, ConfluenceDataSource
from connectors.sources.tests.support import create_source
from connectors.tests.commons import AsyncIterator
from connectors.utils import ssl_context

HOST_URL = "http://127.0.0.1:5000"
CONTENT_QUERY = "limit=1&expand=children.attachment,history.lastUpdated,body.storage"
RESPONSE_SPACE = {
    "results": [
        {
            "id": 4554779,
            "name": "DEMO",
            "_links": {
                "webui": "/spaces/DM",
            },
        }
    ],
    "start": 0,
    "limit": 1,
    "size": 1,
    "_links": {},
}

RESPONSE_PAGE = {
    "results": [
        {
            "id": 4779,
            "title": "ES-scrum",
            "type": "page",
            "history": {"lastUpdated": {"when": "2023-01-24T04:07:19.672Z"}},
            "children": {"attachment": {"size": 2}},
            "body": {"storage": {"value": "This is a test page"}},
            "space": {"name": "DEMO"},
            "_links": {
                "webui": "/spaces/~1234abc/pages/4779/ES-scrum",
            },
        }
    ],
    "start": 0,
    "limit": 1,
    "size": 1,
    "_links": {},
}

EXPECTED_PAGE = {
    "_id": 4779,
    "type": "page",
    "_timestamp": "2023-01-24T04:07:19.672Z",
    "title": "ES-scrum",
    "body": "This is a test page",
    "space": "DEMO",
    "url": f"{HOST_URL}/spaces/~1234abc/pages/4779/ES-scrum",
}

EXPECTED_SPACE = {
    "_id": 4554779,
    "type": "Space",
    "title": "DEMO",
    "_timestamp": "2023-01-24T04:07:19+00:00",
    "url": f"{HOST_URL}/spaces/DM",
}

RESPONSE_ATTACHMENT = {
    "results": [
        {
            "id": "att3637249",
            "title": "demo.py",
            "type": "attachment",
            "version": {"when": "2023-01-03T09:24:50.633Z"},
            "extensions": {"fileSize": 230},
            "_links": {
                "download": "/download/attachments/1113/demo.py?version=1&modificationDate=1672737890633&cacheVersion=1&api=v2",
                "webui": "/pages/viewpageattachments.action?pageId=1113&preview=demo.py",
            },
        }
    ],
    "start": 0,
    "limit": 1,
    "size": 1,
    "_links": {},
}

EXPECTED_ATTACHMENT = {
    "_id": "att3637249",
    "type": "attachment",
    "_timestamp": "2023-01-03T09:24:50.633Z",
    "title": "demo.py",
    "size": 230,
    "space": "DEMO",
    "page": "ES-scrum",
    "url": f"{HOST_URL}/pages/viewpageattachments.action?pageId=1113&preview=demo.py",
}

RESPONSE_CONTENT = "# This is the dummy file"

EXPECTED_CONTENT = {
    "_id": "att3637249",
    "_timestamp": "2023-01-03T09:24:50.633Z",
    "_attachment": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl",
}

EXPECTED_BLOG = {
    "_id": 4779,
    "type": "blogpost",
    "_timestamp": "2023-01-24T04:07:19.672Z",
    "title": "demo-blog",
    "body": "This is a test blog",
    "space": "DEMO",
    "url": f"{HOST_URL}/spaces/~1234abc/blogposts/4779/demo-blog",
}

EXPECTED_BLOG_ATTACHMENT = {
    "_id": "att3637249",
    "type": "attachment",
    "_timestamp": "2023-01-03T09:24:50.633Z",
    "title": "demo.py",
    "size": 230,
    "space": "DEMO",
    "blog": "demo-blog",
    "url": f"{HOST_URL}/pages/viewpageattachments.action?pageId=1113&preview=demo.py",
}


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


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_concurrent_downloads():
    """Test validate configuration method of BaseDataSource class with invalid concurrent downloads"""

    # Setup
    source = create_source(ConfluenceDataSource)
    source.concurrent_downloads = 1000

    # Execute
    with pytest.raises(
        Exception, match="Configured concurrent downloads can't be set more than *"
    ):
        await source.validate_config()


def test_tweak_bulk_options():
    """Test tweak_bulk_options method of BaseDataSource class"""

    # Setup
    source = create_source(ConfluenceDataSource)
    source.concurrent_downloads = 10
    options = {"concurrent_downloads": 5}

    # Execute
    source.tweak_bulk_options(options)

    assert options["concurrent_downloads"] == 10


@pytest.mark.asyncio
async def test_close_with_client_session(patch_logger):
    """Test close method for closing the existing session"""

    # Setup
    source = create_source(ConfluenceDataSource)
    source.confluence_client._get_session()

    # Execute
    await source.close()

    assert source.confluence_client.session is None


@pytest.mark.asyncio
async def test_close_without_client_session(patch_logger):
    """Test close method when the session does not exist"""
    # Setup
    source = create_source(ConfluenceDataSource)

    # Execute
    await source.close()

    assert source.confluence_client.session is None


class MockSSL:
    """This class contains methods which returns dummy ssl context"""

    def load_verify_locations(self, cadata):
        """This method verify locations"""
        pass


@pytest.mark.asyncio
async def test_configuration():
    """Tests the get_default_configurations method of the Confluence source class."""
    # Setup
    config = DataSourceConfiguration(
        config=ConfluenceDataSource.get_default_configuration()
    )

    assert config["host_url"] == HOST_URL


@pytest.mark.parametrize(
    "field, is_cloud",
    [
        ("host_url", True),
        ("service_account_id", True),
        ("api_token", True),
        ("username", False),
        ("password", False),
    ],
)
@pytest.mark.asyncio
async def test_validate_configuration_for_empty_fields(field, is_cloud):
    source = create_source(ConfluenceDataSource)
    source.confluence_client.is_cloud = is_cloud
    source.confluence_client.configuration.set_field(name=field, value="")

    # Execute
    with pytest.raises(Exception):
        await source.validate_config()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_with_ssl(mock_get):
    """Test ping method of ConfluenceDataSource class with SSL"""

    # Execute
    mock_get.return_value.__aenter__.return_value.status = 200
    source = create_source(ConfluenceDataSource)

    source.confluence_client.ssl_enabled = True
    source.confluence_client.certificate = (
        "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"
    )

    # Execute
    with patch.object(ssl, "create_default_context", return_value=MockSSL()):
        source.confluence_client.ssl_ctx = ssl_context(
            certificate=source.confluence_client.certificate
        )
        await source.ping()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_for_failed_connection_exception(mock_get):
    """Tests the ping functionality when connection can not be established to Confluence."""

    # Setup
    source = create_source(ConfluenceDataSource)

    # Execute
    with patch.object(
        ConfluenceClient, "api_call", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await source.ping()


def test_validate_configuration_for_ssl_enabled(patch_logger):
    """This function tests _validate_configuration when certification is empty and ssl is enabled"""
    # Setup
    source = create_source(ConfluenceDataSource)
    source.ssl_enabled = True

    # Execute
    with pytest.raises(Exception):
        source._validate_configuration()


@freeze_time("2023-01-24T04:07:19")
@pytest.mark.asyncio
async def test_fetch_spaces():
    # Setup
    source = create_source(ConfluenceDataSource)

    async_response = AsyncMock()
    async_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(RESPONSE_SPACE))

    with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
        async for response in source.fetch_spaces():
            assert response == EXPECTED_SPACE


@pytest.mark.asyncio
async def test_fetch_documents():
    # Setup
    source = create_source(ConfluenceDataSource)
    async_response = AsyncMock()
    async_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(RESPONSE_PAGE))

    # Execute
    with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
        async for response, _ in source.fetch_documents(api_query=""):
            assert response == EXPECTED_PAGE


@pytest.mark.asyncio
async def test_fetch_attachments():
    # Setup
    source = create_source(ConfluenceDataSource)
    async_response = AsyncMock()
    async_response.__aenter__ = AsyncMock(
        return_value=JSONAsyncMock(RESPONSE_ATTACHMENT)
    )

    # Execute
    with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
        async for response, _ in source.fetch_attachments(
            content_id=1113,
            parent_name="ES-scrum",
            parent_space="DEMO",
            parent_type="page",
        ):
            assert response == EXPECTED_ATTACHMENT


@pytest.mark.asyncio
async def test_download_attachment():
    # Setup
    source = create_source(ConfluenceDataSource)

    async_response = AsyncMock()
    async_response.__aenter__ = AsyncMock(return_value=StreamReaderAsyncMock())

    # Execute
    with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
        with mock.patch(
            "aiohttp.StreamReader.iter_chunked",
            return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
        ):
            response = await source.download_attachment(
                url="download/attachments/1113/demo.py?version=1&modificationDate=1672737890633&cacheVersion=1&api=v2",
                attachment=EXPECTED_ATTACHMENT,
                doit=True,
            )
            assert response == EXPECTED_CONTENT


@pytest.mark.asyncio
async def test_download_attachment_when_filesize_is_large_then_download_skips():
    """Tests the download attachments method for file size greater than max limit."""
    # Setup
    source = create_source(ConfluenceDataSource)

    async_response = AsyncMock()
    async_response.__aenter__ = AsyncMock(return_value=StreamReaderAsyncMock())

    # Execute

    with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
        with mock.patch(
            "aiohttp.StreamReader.iter_chunked",
            return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
        ):
            attachment = copy(EXPECTED_ATTACHMENT)
            attachment["size"] = 23000000

            response = await source.download_attachment(
                url="download/attachments/1113/demo.py?version=1&modificationDate=1672737890633&cacheVersion=1&api=v2",
                attachment=attachment,
                doit=True,
            )
            assert response is None


@pytest.mark.asyncio
async def test_download_attachment_when_unsupported_filetype_used_then_fail_download_skips():
    """Tests the download attachments method for file type is not supported"""
    # Setup
    source = create_source(ConfluenceDataSource)

    async_response = AsyncMock()
    async_response.__aenter__ = AsyncMock(return_value=StreamReaderAsyncMock())

    with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
        with mock.patch(
            "aiohttp.StreamReader.iter_chunked",
            return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
        ):
            attachment = copy(EXPECTED_ATTACHMENT)

            attachment["title"] = "batch.mysy"

            response = await source.download_attachment(
                url="download/attachments/1113/demo.py?version=1&modificationDate=1672737890633&cacheVersion=1&api=v2",
                attachment=attachment,
                doit=True,
            )
            assert response is None


@pytest.mark.asyncio
@mock.patch.object(
    ConfluenceDataSource, "fetch_spaces", return_value=AsyncIterator([EXPECTED_SPACE])
)
@mock.patch.object(
    ConfluenceDataSource,
    "fetch_documents",
    side_effect=[
        (AsyncIterator([[EXPECTED_PAGE, 1]])),
        (AsyncIterator([[EXPECTED_BLOG, 1]])),
    ],
)
@mock.patch.object(
    ConfluenceDataSource,
    "fetch_attachments",
    side_effect=[
        (AsyncIterator([[EXPECTED_ATTACHMENT, "download-url"]])),
        (AsyncIterator([[EXPECTED_BLOG_ATTACHMENT, "download-url"]])),
    ],
)
@mock.patch.object(
    ConfluenceDataSource,
    "download_attachment",
    return_value=AsyncIterator([[EXPECTED_CONTENT]]),
)
async def test_get_docs(spaces_patch, pages_patch, attachment_patch, content_patch):
    """Tests the get_docs method"""

    # Setup
    source = create_source(ConfluenceDataSource)
    expected_responses = [
        EXPECTED_SPACE,
        EXPECTED_PAGE,
        EXPECTED_BLOG,
        EXPECTED_ATTACHMENT,
        EXPECTED_BLOG_ATTACHMENT,
    ]

    # Execute
    documents = []
    async for item, _ in source.get_docs():
        documents.append(item)

    assert documents == expected_responses


def test_get_session():
    """Test that the instance of session returned is always the same for the datasource class."""
    source = create_source(ConfluenceDataSource)
    first_instance = source.confluence_client._get_session()
    second_instance = source.confluence_client._get_session()
    assert first_instance is second_instance
