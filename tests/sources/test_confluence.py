#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Confluence database source class methods"""
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

from connectors.access_control import DLS_QUERY
from connectors.protocol import Filter
from connectors.source import ConfigurableFieldValueError
from connectors.sources.confluence import (
    CONFLUENCE_CLOUD,
    CONFLUENCE_DATA_CENTER,
    CONFLUENCE_SERVER,
    ConfluenceClient,
    ConfluenceDataSource,
    InternalServerError,
    InvalidConfluenceDataSourceTypeError,
    NotFound,
)
from connectors.utils import ssl_context
from tests.commons import AsyncIterator
from tests.sources.support import create_source

ADVANCED_SNIPPET = "advanced_snippet"
HOST_URL = "http://127.0.0.1:9696"
EXCEPTION_MESSAGE = "Something went wrong"
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

SPACE = {
    "id": 4554779,
    "name": "DEMO",
    "_links": {
        "webui": "/spaces/DM",
    },
    "permissions": [
        {
            "id": 1,
            "subjects": {
                "group": {
                    "results": [
                        {
                            "type": "group",
                            "name": "group1",
                            "id": "group_id_1",
                        }
                    ],
                    "size": 1,
                },
            },
            "operation": {"operation": "read", "targetType": "space"},
        }
    ],
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
    "_timestamp": "2024-04-02T09:53:15.818621+00:00",
    "url": "http://127.0.0.1:9696/spaces/DM",
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

RESPONSE_SPACE_KEYS = {
    "results": [
        {
            "id": 23456,
            "key": "DM",
        },
        {
            "id": 9876,
            "key": "ES",
        },
    ],
    "start": 0,
    "limit": 100,
    "size": 2,
    "_links": {},
}

EXPECTED_CONTENT = {
    "_id": "att3637249",
    "_timestamp": "2023-01-03T09:24:50.633Z",
    "_attachment": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl",
}

EXPECTED_CONTENT_EXTRACTED = {
    "_id": "att3637249",
    "_timestamp": "2023-01-03T09:24:50.633Z",
    "body": RESPONSE_CONTENT,
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

RESPONSE_SEARCH_RESULT = {
    "results": [
        {
            "content": {
                "id": "983046",
                "type": "page",
                "space": {"name": "Software Development"},
            },
            "title": "Product Details",
            "excerpt": "Confluence Connector currently supports below objects for ingestion of data in ElasticSearch.\nBlogs\nAttachments\nPages\nSpaces",
            "url": "/spaces/SD/pages/983046/Product+Details",
            "lastModified": "2022-12-19T13:06:18.000Z",
            "entityType": "content",
        },
        {
            "content": {
                "id": "att4587521",
                "type": "attachment",
                "extensions": {
                    "mediaType": "application/pdf",
                    "fileSize": 1119256,
                },
                "space": {"name": "Software Development"},
                "container": {"type": "page", "title": "Product Details"},
                "_links": {"download": "/download/attachments/196717/Potential.pdf"},
            },
            "title": "Potential.pdf",
            "excerpt": "Evaluation Overview",
            "url": "/pages/viewpageattachments.action?pageId=196717&preview=%2F196717%2F4587521%2FPotential.pdf",
            "lastModified": "2023-01-24T03:34:38.000Z",
            "entityType": "content",
        },
        {
            "space": {
                "id": 196612,
                "key": "SD",
                "type": "global",
            },
            "title": "Software Development",
            "excerpt": "",
            "url": "/spaces/SD",
            "lastModified": "2022-12-13T09:49:01.000Z",
            "entityType": "space",
        },
    ]
}

EXPECTED_SEARCH_RESULT = [
    {
        "_id": "983046",
        "title": "Product Details",
        "_timestamp": "2022-12-19T13:06:18.000Z",
        "body": "Confluence Connector currently supports below objects for ingestion of data in ElasticSearch.\nBlogs\nAttachments\nPages\nSpaces",
        "type": "page",
        "space": "Software Development",
        "url": f"{HOST_URL}/spaces/SD/pages/983046/Product+Details",
    },
    {
        "_id": "att4587521",
        "title": "Potential.pdf",
        "_timestamp": "2023-01-24T03:34:38.000Z",
        "type": "attachment",
        "url": f"{HOST_URL}/pages/viewpageattachments.action?pageId=196717&preview=%2F196717%2F4587521%2FPotential.pdf",
        "space": "Software Development",
        "size": 1119256,
        "page": "Product Details",
    },
    {
        "_id": 196612,
        "title": "Software Development",
        "_timestamp": "2022-12-13T09:49:01.000Z",
        "body": "",
        "type": "space",
        "url": f"{HOST_URL}/spaces/SD",
    },
]

SPACE_PERMISSION_RESPONSE = [
    {
        "id": 1,
        "subjects": {
            "group": {
                "results": [
                    {
                        "type": "group",
                        "name": "group1",
                        "id": "group_id_1",
                    }
                ],
                "size": 1,
            },
        },
        "operation": {"operation": "read", "targetType": "space"},
    },
]
PAGE_PERMISSION_RESPONSE = [
    {
        "id": 1,
        "subjects": {
            "group": {
                "results": [
                    {
                        "type": "group",
                        "name": "group2",
                        "id": "group_id_2",
                    }
                ],
                "size": 1,
            },
        },
        "operation": {"operation": "read", "targetType": "page"},
    },
]
BLOG_POST_PERMISSION_RESPONSE = [
    {
        "id": 1,
        "subjects": {
            "group": {
                "results": [
                    {
                        "type": "group",
                        "name": "group2",
                        "id": "group_id_2",
                    }
                ],
                "size": 1,
            },
        },
        "operation": {"operation": "read", "targetType": "blogpost"},
    },
]
PAGE_RESTRICTION_RESPONSE = {
    "user": {
        "results": [
            {
                "type": "known",
                "accountId": "user_id_4",
                "accountType": "atlassian",
                "displayName": "user_4",
            },
            {
                "type": "known",
                "accountId": "user_id_5",
                "accountType": "atlassian",
                "displayName": "user_5",
            },
        ],
        "size": 2,
    },
    "group": {"results": [], "size": 0},
}


@asynccontextmanager
async def create_confluence_source(
    use_text_extraction_service=False, data_source=CONFLUENCE_SERVER
):
    async with create_source(
        ConfluenceDataSource,
        data_source=data_source,
        username="admin",
        password="changeme",
        confluence_url=HOST_URL,
        spaces="*",
        ssl_enabled=False,
        use_document_level_security=False,
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source


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
    async with create_confluence_source() as source:
        source.configuration.get_field("concurrent_downloads").value = 100000
        # Execute
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "configs",
    [
        (
            # Confluence Server with blank dependent fields
            {
                "data_source": CONFLUENCE_SERVER,
                "username": "",
                "password": "",
                "account_email": "foo@bar.me",
                "api_token": "foo",
            }
        ),
        (
            # Confluence Data Center with blank dependent fields
            {
                "data_source": CONFLUENCE_DATA_CENTER,
                "data_center_username": "",
                "data_center_password": "",
                "account_email": "foo@bar.me",
                "api_token": "foo",
            }
        ),
        (
            # Confluence Cloud with blank dependent fields
            {
                "data_source": CONFLUENCE_CLOUD,
                "username": "foo",
                "password": "bar",
                "account_email": "",
                "api_token": "",
            }
        ),
    ],
)
async def test_validate_configuration_with_invalid_dependency_fields_raises_error(
    configs,
):
    # Setup
    async with create_confluence_source() as source:
        for k, v in configs.items():
            source.configuration.get_field(k).value = v
        # Execute
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "configs",
    [
        (
            # Confluence Server with blank non-dependent fields
            {
                "data_source": CONFLUENCE_SERVER,
                "username": "foo",
                "password": "bar",
                "account_email": "",
                "api_token": "",
            }
        ),
        (
            # Confluence Data Center with blank dependent fields
            {
                "data_source": CONFLUENCE_DATA_CENTER,
                "data_center_username": "foo",
                "data_center_password": "bar",
                "account_email": "",
                "api_token": "",
            }
        ),
        (
            # Confluence Cloud with blank non-dependent fields
            {
                "data_source": CONFLUENCE_CLOUD,
                "username": "",
                "password": "",
                "account_email": "foo@bar.me",
                "api_token": "foobar",
            }
        ),
        (
            # SSL certificate not enabled (empty ssl_ca okay)
            {
                "username": "foo",
                "password": "bar",
                "ssl_enabled": False,
                "ssl_ca": "",
            }
        ),
    ],
)
async def test_validate_config_with_valid_dependency_fields_does_not_raise_error(
    configs,
):
    async with create_confluence_source() as source:
        source.confluence_client.ping = AsyncMock()
        for k, v in configs.items():
            source.configuration.get_field(k).value = v

        await source.validate_config()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_validate_config_when_ssl_enabled_and_ssl_ca_not_empty_does_not_raise_error(
    mock_get,
):
    with patch.object(ssl, "create_default_context", return_value=MockSSL()):
        async with create_confluence_source() as source:
            source.configuration.get_field("username").value = "foo"
            source.configuration.get_field("password").value = "foo"
            source.configuration.get_field("ssl_enabled").value = True
            source.configuration.get_field(
                "ssl_ca"
            ).value = (
                "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"
            )
            await source.validate_config()


@pytest.mark.asyncio
async def test_tweak_bulk_options():
    """Test tweak_bulk_options method of BaseDataSource class"""

    # Setup
    async with create_confluence_source() as source:
        source.concurrent_downloads = 10
        options = {"concurrent_downloads": 5}

        # Execute
        source.tweak_bulk_options(options)

        assert options["concurrent_downloads"] == 10


@pytest.mark.asyncio
async def test_close_with_client_session():
    """Test close method for closing the existing session"""

    # Setup
    async with create_confluence_source() as source:
        source.confluence_client._get_session()

        # Execute
        await source.close()

        assert source.confluence_client.session is None


@pytest.mark.asyncio
async def test_close_without_client_session():
    """Test close method when the session does not exist"""
    # Setup
    async with create_confluence_source() as source:
        # Execute
        await source.close()

        assert source.confluence_client.session is None


@pytest.mark.asyncio
async def test_remote_validation_when_space_keys_are_valid():
    async with create_confluence_source() as source:
        source.spaces = ["DM", "ES"]
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(RESPONSE_SPACE_KEYS)
        )

        with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
            await source._remote_validation()


@pytest.mark.asyncio
async def test_remote_validation_when_space_keys_are_unavailable_then_raise_exception():
    async with create_confluence_source() as source:
        source.spaces = ["ES", "CS"]
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(RESPONSE_SPACE_KEYS)
        )

        with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
            with pytest.raises(
                ConfigurableFieldValueError,
                match="Spaces 'CS' are not available. Available spaces are: 'DM, ES'",
            ):
                await source._remote_validation()


class MockSSL:
    """This class contains methods which returns dummy ssl context"""

    def load_verify_locations(self, cadata):
        """This method verify locations"""
        pass


@pytest.mark.parametrize(
    "field, data_source",
    [
        ("confluence_url", "confluence_cloud"),
        ("account_email", "confluence_cloud"),
        ("api_token", "confluence_cloud"),
        ("username", "confluence_server"),
        ("password", "confluence_server"),
        ("data_center_username", "confluence_data_center"),
        ("data_center_password", "confluence_data_center"),
    ],
)
@pytest.mark.asyncio
async def test_validate_configuration_for_empty_fields(field, data_source):
    async with create_confluence_source() as source:
        source.confluence_client.configuration.get_field(
            "data_source"
        ).value = data_source
        source.confluence_client.configuration.get_field(field).value = ""

        # Execute
        with pytest.raises(Exception):
            await source.validate_config()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_with_ssl(mock_get):
    """Test ping method of ConfluenceDataSource class with SSL"""

    # Execute
    mock_get.return_value.__aenter__.return_value.status = 200
    async with create_confluence_source() as source:
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
    async with create_confluence_source() as source:
        # Execute
        with patch.object(
            ConfluenceClient, "api_call", side_effect=Exception("Something went wrong")
        ):
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
async def test_validate_configuration_for_ssl_enabled():
    """This function tests _validate_configuration when certification is empty and ssl is enabled"""
    # Setup
    async with create_confluence_source() as source:
        source.ssl_enabled = True

        # Execute
        with pytest.raises(Exception):
            source._validate_configuration()


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
    async with create_confluence_source() as source:
        with patch(
            "aiohttp.ClientSession.get",
            side_effect=[initial_response, retried_response],
        ):
            async for response in source.confluence_client.api_call(
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
    with patch("connectors.sources.confluence.DEFAULT_RETRY_SECONDS", 0):
        async with create_confluence_source() as source:
            with patch(
                "aiohttp.ClientSession.get",
                side_effect=[initial_response, retried_response],
            ):
                async for response in source.confluence_client.api_call(
                    url="http://localhost:1000/sample"
                ):
                    result = await response.json()

        assert result == payload


@pytest.mark.asyncio
async def test_get_with_404_status():
    error = ClientResponseError(None, None)
    error.status = 404

    async with create_confluence_source() as source:
        with patch(
            "aiohttp.ClientSession.get",
            side_effect=error,
        ):
            with pytest.raises(NotFound):
                async for response in source.confluence_client.api_call(
                    url="http://localhost:1000/err"
                ):
                    await response.json()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_with_500_status():
    error = ClientResponseError(None, None)
    error.status = 500

    async with create_confluence_source() as source:
        with patch(
            "aiohttp.ClientSession.get",
            side_effect=error,
        ):
            with pytest.raises(InternalServerError):
                async for response in source.confluence_client.api_call(
                    url="http://localhost:1000/err"
                ):
                    await response.json()


@freeze_time("2023-01-24T04:07:19")
@pytest.mark.asyncio
async def test_fetch_spaces():
    # Setup
    async with create_confluence_source() as source:
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(RESPONSE_SPACE)
        )

        with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
            async for response in source.confluence_client.fetch_spaces():
                assert response == RESPONSE_SPACE["results"][0]


@pytest.mark.asyncio
async def test_fetch_documents():
    # Setup
    async with create_confluence_source() as source:
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(RESPONSE_PAGE))

        # Execute
        with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
            async for response, _, _, _, _ in source.fetch_documents(api_query=""):
                assert response == EXPECTED_PAGE


@pytest.mark.asyncio
async def test_fetch_attachments():
    # Setup
    async with create_confluence_source() as source:
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
async def test_search_by_query():
    async with create_confluence_source() as source:
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(RESPONSE_SEARCH_RESULT)
        )
        documents = []
        with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
            async for response, _ in source.search_by_query(
                query="type in ('space', 'page', 'attachment') AND space.key ='SD'"
            ):
                documents.append(response)
        assert documents == EXPECTED_SEARCH_RESULT


@pytest.mark.asyncio
async def test_search_by_query_for_datacenter():
    async with create_confluence_source() as source:
        async_response = AsyncMock()
        source.confluence_client.data_source_type = "confluence_data_center"
        async_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(RESPONSE_SEARCH_RESULT)
        )
        documents = []
        with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
            async for response, _ in source.search_by_query(
                query="type in ('space', 'page', 'attachment') AND space.key ='SD'"
            ):
                documents.append(response)
        assert documents == EXPECTED_SEARCH_RESULT


@pytest.mark.asyncio
async def test_download_attachment():
    # Setup
    async with create_confluence_source() as source:
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
async def test_download_attachment_with_upper_extension():
    # Setup
    async with create_confluence_source() as source:
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(return_value=StreamReaderAsyncMock())

        # Execute
        with mock.patch("aiohttp.ClientSession.get", return_value=async_response):
            with mock.patch(
                "aiohttp.StreamReader.iter_chunked",
                return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
            ):
                attachment = copy(EXPECTED_ATTACHMENT)
                attachment["title"] = "batch.TXT"

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
    async with create_confluence_source() as source:
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
    async with create_confluence_source() as source:
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
@patch(
    "connectors.content_extraction.ContentExtraction._check_configured",
    lambda *_: True,
)
async def test_download_attachment_with_text_extraction_enabled_adds_body():
    with patch(
        "connectors.content_extraction.ContentExtraction.extract_text",
        return_value=RESPONSE_CONTENT,
    ), patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
    ):
        async with create_confluence_source(use_text_extraction_service=True) as source:
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
                    assert response == EXPECTED_CONTENT_EXTRACTED


@pytest.mark.asyncio
@mock.patch.object(
    ConfluenceClient,
    "fetch_spaces",
    return_value=AsyncIterator([copy(SPACE)]),
)
@mock.patch.object(
    ConfluenceDataSource,
    "fetch_documents",
    side_effect=[
        (AsyncIterator([[copy(EXPECTED_PAGE), 1, "space_key", [], {}]])),
        (AsyncIterator([[copy(EXPECTED_BLOG), 1, "space_key", [], {}]])),
    ],
)
@mock.patch.object(
    ConfluenceDataSource,
    "fetch_attachments",
    side_effect=[
        (AsyncIterator([[copy(EXPECTED_ATTACHMENT), "download-url"]])),
        (AsyncIterator([[copy(EXPECTED_BLOG_ATTACHMENT), "download-url"]])),
    ],
)
@mock.patch.object(
    ConfluenceDataSource,
    "download_attachment",
    return_value=AsyncIterator([[copy(EXPECTED_CONTENT)]]),
)
@freeze_time("2024-04-02T09:53:15.818621+00:00")
async def test_get_docs(spaces_patch, pages_patch, attachment_patch, content_patch):
    """Tests the get_docs method"""

    # Setup
    async with create_confluence_source() as source:
        expected_responses = [
            EXPECTED_SPACE,
            EXPECTED_PAGE,
            EXPECTED_BLOG,
            EXPECTED_ATTACHMENT,
            EXPECTED_BLOG_ATTACHMENT,
        ]

        # Execute
        documents = []
        source.confluence_client.data_source_type = "confluence_cloud"
        async for item, _ in source.get_docs():
            documents.append(item)
        assert documents == expected_responses


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "data_source_type", [CONFLUENCE_CLOUD, CONFLUENCE_DATA_CENTER, CONFLUENCE_SERVER]
)
async def test_get_session(data_source_type):
    async with create_confluence_source(data_source=data_source_type) as source:
        try:
            source.confluence_client._get_session()
        except Exception as e:
            pytest.fail(
                f"Should not raise for valid data source type '{data_source_type}'. Exception: {e}"
            )


@pytest.mark.asyncio
async def test_get_session_multiple_calls_return_same_instance():
    async with create_confluence_source() as source:
        first_instance = source.confluence_client._get_session()
        second_instance = source.confluence_client._get_session()
        assert first_instance is second_instance


@pytest.mark.asyncio
async def test_get_session_raise_on_invalid_data_source_type():
    async with create_confluence_source(data_source="invalid") as source:
        with pytest.raises(InvalidConfluenceDataSourceTypeError):
            source.confluence_client._get_session()


@pytest.mark.asyncio
async def test_get_access_control_dls_disabled():
    async with create_confluence_source() as source:
        source._dls_enabled = MagicMock(return_value=False)

        acl = []
        async for access_control in source.get_access_control():
            acl.append(access_control)

        assert len(acl) == 0


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
        "locale": "en-US",
        "emailAddress": "user1@dummy-domain.com",
        "active": True,
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
            "locale": "locale:en-US",
            "email_address": "email_address:user1@dummy-domain.com",
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
                },
                "source": DLS_QUERY,
            },
        },
    }

    async with create_confluence_source() as source:
        source._dls_enabled = MagicMock(return_value=True)
        source.confluence_client.data_source_type = "confluence_cloud"

        source.atlassian_access_control.fetch_all_users = AsyncIterator([mock_users])
        source.atlassian_access_control.fetch_user = AsyncIterator([mock_user1])

        user_documents = []
        async for user_doc in source.get_access_control():
            user_documents.append(user_doc)
        assert expected_user_doc in user_documents


@pytest.mark.asyncio
@freeze_time("2023-01-24T04:07:19")
async def test_get_access_control_dls_enabled_for_datacenter():
    mock_users = [
        {
            "username": "user1",
            "displayName": "user1",
            "userKey": "607194d6bc3c3f006f4c35d6",
        },
    ]

    expected_user_doc = {
        "_id": "607194d6bc3c3f006f4c35d6",
        "identity": {
            "account_id": "account_id:607194d6bc3c3f006f4c35d6",
            "display_name": "name:user1",
            "username": "name:user1",
        },
        "created_at": "2023-01-24T04:07:19+00:00",
        "query": {
            "template": {
                "params": {
                    "access_control": [
                        "account_id:607194d6bc3c3f006f4c35d6",
                        "name:user1",
                        "name:user1",
                    ]
                },
                "source": DLS_QUERY,
            },
        },
    }

    async with create_confluence_source() as source:
        source._dls_enabled = MagicMock(return_value=True)
        source.confluence_client.data_source_type = "confluence_data_center"

        source.confluence_client.fetch_confluence_server_users = AsyncIterator(
            [mock_users]
        )

        user_documents = []
        async for user_doc in source.get_access_control():
            user_documents.append(user_doc)

        assert expected_user_doc in user_documents


@pytest.mark.asyncio
@freeze_time("2023-01-24T04:07:19")
async def test_get_access_control_dls_enabled_for_server():
    mock_users = [
        {
            "fullName": "user1",
            "name": "user1",
            "key": "607194d6bc3c3f006f4c35d6",
            "email": "nedog73667@rentaen.com",
        },
    ]

    expected_user_doc = {
        "_id": "607194d6bc3c3f006f4c35d6",
        "identity": {
            "account_id": "account_id:607194d6bc3c3f006f4c35d6",
            "display_name": "name:user1",
            "email_address": "email_address:nedog73667@rentaen.com",
        },
        "created_at": "2023-01-24T04:07:19+00:00",
        "query": {
            "template": {
                "params": {
                    "access_control": [
                        "account_id:607194d6bc3c3f006f4c35d6",
                        "email_address:nedog73667@rentaen.com",
                        "name:user1",
                    ]
                },
                "source": DLS_QUERY,
            },
        },
    }

    async with create_confluence_source() as source:
        source._dls_enabled = MagicMock(return_value=True)

        source.confluence_client.fetch_confluence_server_users = AsyncIterator(
            [mock_users]
        )

        user_documents = []
        async for user_doc in source.get_access_control():
            user_documents.append(user_doc)

        assert expected_user_doc in user_documents


@pytest.mark.asyncio
async def test_fetch_confluence_server_users():
    async with create_confluence_source() as source:
        source.confluence_client.api_call = AsyncIterator(
            [JSONAsyncMock({"start": 0, "users": []})]
        )
        async for user in source.confluence_client.fetch_confluence_server_users():
            assert user is None


@pytest.mark.asyncio
@mock.patch.object(
    ConfluenceClient,
    "fetch_spaces",
    return_value=AsyncIterator(([copy(SPACE)])),
)
@mock.patch.object(
    ConfluenceDataSource,
    "fetch_documents",
    side_effect=[
        (
            AsyncIterator(
                [
                    [
                        copy(EXPECTED_BLOG),
                        1,
                        "space_key",
                        BLOG_POST_PERMISSION_RESPONSE,
                        {},
                    ]
                ]
            )
        ),
        (
            AsyncIterator(
                [
                    [
                        copy(EXPECTED_PAGE),
                        1,
                        "space_key",
                        PAGE_PERMISSION_RESPONSE,
                        PAGE_RESTRICTION_RESPONSE,
                    ]
                ]
            )
        ),
    ],
)
@mock.patch.object(
    ConfluenceDataSource,
    "fetch_attachments",
    side_effect=[
        (
            AsyncIterator(
                [
                    [
                        copy(EXPECTED_BLOG_ATTACHMENT),
                        "download-url",
                    ]
                ]
            )
        ),
        (
            AsyncIterator(
                [
                    [
                        copy(EXPECTED_ATTACHMENT),
                        "download-url",
                    ]
                ]
            )
        ),
    ],
)
@mock.patch.object(
    ConfluenceDataSource,
    "download_attachment",
    return_value=AsyncIterator([[copy(EXPECTED_CONTENT)]]),
)
@freeze_time("2024-04-02T09:53:15.818621+00:00")
async def test_get_docs_dls_enabled(
    spaces_patch, pages_patch, attachment_patch, content_patch
):
    async with create_confluence_source() as source:
        source._dls_enabled = MagicMock(return_value=True)
        source.confluence_client.data_source_type = "confluence_cloud"

        expected_responses = [
            copy(EXPECTED_SPACE) | {"_allow_access_control": ["group_id:group_id_1"]},
            copy(EXPECTED_BLOG) | {"_allow_access_control": ["group_id:group_id_2"]},
            copy(EXPECTED_PAGE)
            | {
                "_allow_access_control": [
                    "account_id:user_id_4",
                    "account_id:user_id_5",
                ]
            },
            copy(EXPECTED_ATTACHMENT)
            | {
                "_allow_access_control": [
                    "account_id:user_id_4",
                    "account_id:user_id_5",
                ]
            },
            copy(EXPECTED_BLOG_ATTACHMENT)
            | {"_allow_access_control": ["group_id:group_id_2"]},
        ]

        async for item, _ in source.get_docs():
            item.get("_allow_access_control", []).sort()
            assert item in expected_responses


@pytest.mark.parametrize(
    "filtering, expected_docs",
    [
        (
            Filter({ADVANCED_SNIPPET: {"value": [{"query": "space = DEMO"}]}}),
            [
                EXPECTED_SPACE,
                EXPECTED_ATTACHMENT,
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules(filtering, expected_docs):
    async with create_confluence_source() as source:
        with patch.object(
            ConfluenceDataSource,
            "search_by_query",
            side_effect=[
                (AsyncIterator([[copy(EXPECTED_ATTACHMENT), "download-url"]])),
                (AsyncIterator([[copy(EXPECTED_SPACE), None]])),
            ],
        ):
            with patch.object(
                ConfluenceDataSource,
                "download_attachment",
                return_value=AsyncIterator([[copy(EXPECTED_CONTENT)]]),
            ):
                source.get_content = mock.Mock(return_value=EXPECTED_ATTACHMENT)

                async for item, _ in source.get_docs(filtering):
                    assert item in expected_docs


@pytest.mark.asyncio
async def test_extract_identities_for_datacenter():
    async with create_confluence_source() as source:
        source._dls_enabled = MagicMock(return_value=True)
        response = {
            "user": {
                "results": [
                    {
                        "type": "known",
                        "username": "demo",
                        "userKey": "8ab284ca8d53fe15018d5989eb750001",
                        "displayName": "Demo",
                    }
                ],
            },
            "group": {
                "results": [
                    {
                        "type": "group",
                        "name": "confluence-users",
                    }
                ],
            },
        }
        expected_response = source._extract_identities_for_datacenter(response=response)
        assert expected_response == {"group:confluence-users", "user:demo"}


@pytest.mark.asyncio
async def test_fetch_server_space_permission():
    async with create_confluence_source() as source:
        source._dls_enabled = MagicMock(return_value=True)
        payload = {
            "permissions": {
                "VIEWSPACE": {
                    "groups": ["confluence-users"],
                    "users": [
                        "admin",
                    ],
                }
            },
        }
        source.confluence_client.api_call = AsyncIterator([JSONAsyncMock(payload)])
        expected_response = await source.fetch_server_space_permission(space_key="key")
        assert expected_response == {
            "permissions": {
                "VIEWSPACE": {"groups": ["confluence-users"], "users": ["admin"]}
            }
        }


@pytest.mark.asyncio
async def test_api_call_for_exception(patch_sleep):
    """This function test _api_call when credentials are incorrect"""
    async with create_confluence_source() as source:
        source.confluence_client.retry_count = 1
        with patch.object(
            aiohttp, "ClientSession", side_effect=Exception(EXCEPTION_MESSAGE)
        ):
            with pytest.raises(Exception):
                await anext(source.confluence_client.api_call(url="abc"))


@pytest.mark.asyncio
async def test_get_permission():
    async with create_confluence_source() as source:
        actual_permission = {"users": ["admin"], "groups": ["group"]}
        permisssions = source.get_permission(permission=actual_permission)
        assert permisssions == {"group:group", "user:admin"}


@mock.patch.object(
    ConfluenceDataSource,
    "fetch_documents",
    side_effect=[
        (AsyncIterator([[copy(EXPECTED_PAGE), 1, "space_key", [], {}]])),
        (AsyncIterator([[copy(EXPECTED_BLOG), 1, "space_key", [], {}]])),
    ],
)
@pytest.mark.asyncio
async def test_page_blog_coro(fetch_documents):
    async with create_confluence_source() as source:
        await source._page_blog_coro("api_query", "target")


@pytest.mark.asyncio
async def test_end_signal_is_added_to_queue_in_case_of_exception():
    END_SIGNAL = "FINISHED_TASK"
    async with create_confluence_source() as source:
        with patch.object(
            source,
            "fetch_attachments",
            side_effect=Exception("Error fetching attachments"),
        ):
            with pytest.raises(Exception):
                await source._attachment_coro(document=EXPECTED_PAGE, access_control=[])
                assert source.queue.get_nowait() == END_SIGNAL
