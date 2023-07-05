#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Sharepoint source class methods.
"""
from unittest import mock
from unittest.mock import Mock, patch

import aiohttp
import pytest
from aiohttp import StreamReader

from connectors.source import DataSourceConfiguration
from connectors.sources.sharepoint_server import SharepointServerDataSource
from tests.sources.support import create_source

EXCEPTION_MESSAGE = "Something went wrong"
HOST_URL = "http://127.0.0.1:8491"


class AsyncIter:
    """This Class is use to return async generator"""

    def __init__(self, items):
        """Setup list of dictionary

        Args:
            items (any): Item to iter on
        """
        self.items = items

    async def __aiter__(self):
        """This Method is used to return async generator"""
        if isinstance(self.items, str):
            yield self.items.encode().decode()
        else:
            yield self.items


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


async def async_native_coroutine_generator(item):
    """create a method for returning fake async value for
    Args:
        item: Value for converting into async
    """
    yield item


def test_get_configuration():
    """Tests the get configurations method of the Sharepoint source class."""
    # Setup
    klass = SharepointServerDataSource

    # Execute
    config = DataSourceConfiguration(config=klass.get_default_configuration())

    # Assert
    assert config["host_url"] == HOST_URL


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    """Tests the ping functionality for ensuring connection to the Sharepoint."""

    # Setup
    source = create_source(SharepointServerDataSource)
    source.sharepoint_client._api_call = Mock(
        return_value=async_native_coroutine_generator(200)
    )

    # Execute
    await source.ping()


@pytest.mark.asyncio
async def test_ping_for_failed_connection_exception():
    """Tests the ping functionality when connection can not be established to Sharepoint."""

    # Setup
    source = create_source(SharepointServerDataSource)
    source.sharepoint_client.retry_count = 0
    mock_response = {"access_token": "test2344", "expires_in": "1234555"}
    async_response = MockResponse(mock_response, 200)
    with patch.object(
        aiohttp, "ClientSession", side_effect=Exception(EXCEPTION_MESSAGE)
    ):
        with patch("aiohttp.request", return_value=async_response):
            # Execute
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
async def test_api_call_for_exception():
    """This function test _api_call when credentials are incorrect"""
    # Setup
    source = create_source(SharepointServerDataSource)
    source.sharepoint_client.retry_count = 0
    # Execute
    with patch.object(
        aiohttp, "ClientSession", side_effect=Exception(EXCEPTION_MESSAGE)
    ):
        with pytest.raises(Exception):
            await source.sharepoint_client._api_call(url_name="lists", url="abc")


def test_prepare_drive_items_doc():
    """Test the prepare drive items method"""
    # Setup
    source = create_source(SharepointServerDataSource)
    list_items = {
        "File": {
            "Name": "dummy",
            "TimeLastModified": "2023-01-30T12:48:31Z",
            "TimeCreated": "2023-01-30T12:48:31Z",
            "Length": "12",
            "ServerRelativeUrl": "/site",
        },
        "GUID": 1,
        "item_type": "File",
    }
    expected_response = {
        "_id": 1,
        "type": "File",
        "size": 12,
        "title": "dummy",
        "creation_time": "2023-01-30T12:48:31Z",
        "_timestamp": "2023-01-30T12:48:31Z",
        "url": f"{HOST_URL}/site",
        "server_relative_url": "/site",
    }

    # Execute
    target_response = source.format_drive_item(item=list_items)
    # Assert
    assert target_response == expected_response


def test_prepare_list_items_doc():
    """Test the prepare list items method"""
    # Setup
    source = create_source(SharepointServerDataSource)
    list_items = {
        "Title": "dummy",
        "AuthorId": 123,
        "EditorId": 123,
        "Created": "2023-01-30T12:48:31Z",
        "Modified": "2023-01-30T12:48:31Z",
        "GUID": 1,
        "FileRef": "/site",
        "url": f"{HOST_URL}/site%5E",
        "file_name": "filename",
        "server_relative_url": "/site^",
    }
    expected_response = {
        "type": "list_item",
        "_id": 1,
        "file_name": "filename",
        "size": 0,
        "title": "dummy",
        "author_id": 123,
        "editor_id": 123,
        "creation_time": "2023-01-30T12:48:31Z",
        "_timestamp": "2023-01-30T12:48:31Z",
        "url": "http://127.0.0.1:8491/site%5E",
        "server_relative_url": "/site^",
    }

    # Execute
    target_response = source.format_list_item(
        item=list_items,
    )
    # Assert
    assert target_response == expected_response


def test_prepare_sites_doc():
    """Test the method for preparing sites document"""
    # Setup
    source = create_source(SharepointServerDataSource)
    list_items = {
        "Title": "dummy",
        "LastItemModifiedDate": "2023-01-30T12:48:31Z",
        "Created": "2023-01-30T12:48:31Z",
        "Id": 1,
        "Url": "sharepoint.com",
        "ServerRelativeUrl": "/site",
    }
    expected_response = {
        "_id": 1,
        "type": "sites",
        "title": "dummy",
        "creation_time": "2023-01-30T12:48:31Z",
        "_timestamp": "2023-01-30T12:48:31Z",
        "url": "sharepoint.com",
        "server_relative_url": "/site",
    }

    # Execute
    target_response = source.format_sites(item=list_items)
    # Assert
    assert target_response == expected_response


@pytest.mark.asyncio
async def test_get_sites_when_no_site_available():
    """Test get sites method with valid details"""
    # Setup
    source = create_source(SharepointServerDataSource)
    api_response = []
    # Execute
    source.sharepoint_client._fetch_data_with_query = Mock(
        return_value=AsyncIter(api_response)
    )
    actual_response = None
    async for item in source.sharepoint_client.get_sites(
        site_url="sites/collection1/ctest"
    ):
        actual_response = item

    # Assert
    assert actual_response is None


@pytest.mark.asyncio
async def test_get_list_items():
    """Test get list items method with valid details"""
    # Setup
    source = create_source(SharepointServerDataSource)
    api_response = [
        {
            "AttachmentFiles": [
                {
                    "FileName": "s3 queries.txt",
                    "ServerRelativeUrl": "/sites/collection1/ctest/Lists/ctestlist/Attachments/1/v4.txt",
                }
            ],
            "Title": "HelloWorld",
            "Id": 1,
            "FileRef": "/site",
            "ContentTypeId": "12345",
            "Modified": "2022-06-20T10:04:03Z",
            "Created": "2022-06-20T10:04:03Z",
            "EditorId": 1073741823,
            "AuthorId": 1073741823,
            "Attachments": True,
            "GUID": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
            "Length": "12",
        },
        {
            "AttachmentFiles": {},
            "Id": 1,
            "ContentTypeId": "12345",
            "Title": "HelloWorld",
            "FileRef": "/site",
            "Modified": "2022-06-20T10:04:03Z",
            "Created": "2022-06-20T10:04:03Z",
            "EditorId": 1073741823,
            "AuthorId": 1073741823,
            "Attachments": False,
            "GUID": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
        },
    ]
    target_response = [
        {
            "AttachmentFiles": [
                {
                    "FileName": "s3 queries.txt",
                    "ServerRelativeUrl": "/sites/collection1/ctest/Lists/ctestlist/Attachments/1/v4.txt",
                }
            ],
            "Title": "HelloWorld",
            "Id": 1,
            "FileRef": "/site",
            "ContentTypeId": "12345",
            "Modified": "2022-06-20T10:04:03Z",
            "Created": "2022-06-20T10:04:03Z",
            "EditorId": 1073741823,
            "AuthorId": 1073741823,
            "Attachments": True,
            "GUID": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
            "Length": None,
            "_id": "1",
            "url": "http://127.0.0.1:8491/sites/collection1/ctest/Lists/ctestlist/Attachments/1/v4.txt",
            "file_name": "s3 queries.txt",
            "server_relative_url": "/sites/collection1/ctest/Lists/ctestlist/Attachments/1/v4.txt",
        },
        {
            "AttachmentFiles": {},
            "Id": 1,
            "ContentTypeId": "12345",
            "Title": "HelloWorld",
            "FileRef": "/site",
            "Modified": "2022-06-20T10:04:03Z",
            "Created": "2022-06-20T10:04:03Z",
            "EditorId": 1073741823,
            "AuthorId": 1073741823,
            "Attachments": False,
            "GUID": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
            "url": "http://127.0.0.1:8491/sites/enterprise/site1%5E/DispForm.aspx?ID=1&Source=http://127.0.0.1:8491/sites/enterprise/site1%5E/AllItems.aspx&ContentTypeId=12345",
        },
    ]
    attachment_response = {"UniqueId": "1"}
    source.sharepoint_client._fetch_data_with_next_url = Mock(
        return_value=AsyncIter(api_response)
    )
    source.sharepoint_client._api_call = Mock(
        return_value=async_native_coroutine_generator(attachment_response)
    )
    expected_response = []
    # Execute
    async for item, _ in source.sharepoint_client.get_list_items(
        list_id="620070a1-ee50-4585-b6a7-0f6210b1a69d",
        site_url="/sites/enterprise/ctest",
        server_relative_url="/sites/enterprise/site1^",
        selected_field="",
    ):
        expected_response.append(item)
    # Assert
    assert expected_response == target_response


@pytest.mark.asyncio
async def test_get_drive_items():
    """Test get drive items method with valid details"""
    # Setup
    source = create_source(SharepointServerDataSource)
    api_response = [
        {
            "File": {
                "Length": "3356",
                "Name": "Home.txt",
                "ServerRelativeUrl": "/sites/enterprise/ctest/SitePages/Home.aspx",
                "TimeCreated": "2022-05-02T07:20:33Z",
                "TimeLastModified": "2022-05-02T07:20:34Z",
                "Title": "Home.txt",
            },
            "Folder": {"__deferred": {}},
            "Modified": "2022-05-02T07:20:35Z",
            "GUID": "111111122222222-c77f-4ed3-84ef-8a4dd87c80d0",
            "Length": "3356",
        },
        {
            "File": {},
            "Folder": {
                "Length": "3356",
                "Name": "Home.txt",
                "ServerRelativeUrl": "/sites/enterprise/ctest/SitePages/Home.aspx",
                "TimeCreated": "2022-05-02T07:20:33Z",
                "TimeLastModified": "2022-05-02T07:20:34Z",
                "Title": "Home.txt",
            },
            "Modified": "2022-05-02T07:20:35Z",
            "GUID": "111111122222222-c77f-4ed3-084ef-8a4dd87c80d0",
            "Length": "3356",
        },
    ]
    target_response = [
        (
            {
                "File": {
                    "Length": "3356",
                    "Name": "Home.txt",
                    "ServerRelativeUrl": "/sites/enterprise/ctest/SitePages/Home.aspx",
                    "TimeCreated": "2022-05-02T07:20:33Z",
                    "TimeLastModified": "2022-05-02T07:20:34Z",
                    "Title": "Home.txt",
                },
                "Folder": {"__deferred": {}},
                "Modified": "2022-05-02T07:20:35Z",
                "GUID": "111111122222222-c77f-4ed3-84ef-8a4dd87c80d0",
                "Length": "3356",
                "item_type": "File",
            },
            "/sites/enterprise/ctest/SitePages/Home.aspx",
        ),
        (
            {
                "File": {},
                "Folder": {
                    "Length": "3356",
                    "Name": "Home.txt",
                    "ServerRelativeUrl": "/sites/enterprise/ctest/SitePages/Home.aspx",
                    "TimeCreated": "2022-05-02T07:20:33Z",
                    "TimeLastModified": "2022-05-02T07:20:34Z",
                    "Title": "Home.txt",
                },
                "Modified": "2022-05-02T07:20:35Z",
                "GUID": "111111122222222-c77f-4ed3-084ef-8a4dd87c80d0",
                "Length": "3356",
                "item_type": "Folder",
            },
            None,
        ),
    ]

    source.sharepoint_client._fetch_data_with_next_url = Mock(
        return_value=AsyncIter(api_response)
    )
    expected_response = []
    # Execute
    async for item in source.sharepoint_client.get_drive_items(
        list_id="620070a1-ee50-4585-b6a7-0f6210b1a69d",
        site_url="/sites/enterprise/ctest",
        server_relative_url=None,
        selected_field="",
    ):
        expected_response.append(item)
    # Assert
    assert target_response == expected_response


@pytest.mark.asyncio
async def test_get_docs_list_items():
    """Test get docs method for list items"""

    # Setup
    source = create_source(SharepointServerDataSource)
    item_content_response = {
        "RootFolder": {
            "ServerRelativeUrl": "/sites/enterprise/ctest/_api",
        },
        "Created": "2023-03-19T05:02:52Z",
        "BaseType": 0,
        "Id": "f764b597-ed44-49be-8867-f8e9ca5d0a6e",
        "LastItemModifiedDate": "2023-03-19T05:02:52Z",
        "ParentWebUrl": "/sites/enterprise/ctest/_api",
        "Title": "HelloWorld",
    }
    list_content_response = {
        "AttachmentFiles": {},
        "Id": 1,
        "ContentTypeId": "12345",
        "Title": "HelloWorld",
        "FileRef": "/site",
        "Modified": "2022-06-20T10:04:03Z",
        "Created": "2022-06-20T10:04:03Z",
        "EditorId": 1073741823,
        "AuthorId": 1073741823,
        "Attachments": False,
        "GUID": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
        "url": "/sites/enterprise/ctest/_api",
    }
    actual_response = []
    source.sharepoint_client._fetch_data_with_query = Mock(return_value=AsyncIter([]))
    source.sharepoint_client.get_lists = Mock(
        return_value=AsyncIter([item_content_response])
    )
    source.sharepoint_client.get_list_items = Mock(
        return_value=AsyncIter((list_content_response, None))
    )
    # Execute
    async for document, _ in source.get_docs():
        actual_response.append(document)
    # Assert
    assert len(actual_response) == 2


@pytest.mark.asyncio
async def test_get_docs_list_items_when_relativeurl_is_not_none():
    """Test get docs method for list items"""

    # Setup
    source = create_source(SharepointServerDataSource)
    item_content_response = {
        "RootFolder": {
            "ServerRelativeUrl": "/sites/enterprise/ctest/_api",
        },
        "Created": "2023-03-19T05:02:52Z",
        "BaseType": 0,
        "Id": "f764b597-ed44-49be-8867-f8e9ca5d0a6e",
        "LastItemModifiedDate": "2023-03-19T05:02:52Z",
        "ParentWebUrl": "/sites/enterprise/ctest/_api",
        "Title": "HelloWorld",
    }
    list_content_response = {
        "AttachmentFiles": {},
        "Id": 1,
        "ContentTypeId": "12345",
        "Title": "HelloWorld",
        "FileRef": "/site",
        "Modified": "2022-06-20T10:04:03Z",
        "Created": "2022-06-20T10:04:03Z",
        "EditorId": 1073741823,
        "AuthorId": 1073741823,
        "Attachments": False,
        "GUID": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
        "url": "/sites/enterprise/ctest/_api",
    }
    actual_response = []
    source.sharepoint_client._fetch_data_with_query = Mock(return_value=AsyncIter([]))
    source.sharepoint_client.get_lists = Mock(
        return_value=AsyncIter([item_content_response])
    )
    source.sharepoint_client.get_list_items = Mock(
        return_value=AsyncIter((list_content_response, "/sites/enterprise/ctest/_api"))
    )
    # Execute
    async for document, _ in source.get_docs():
        actual_response.append(document)
    # Assert
    assert len(actual_response) == 2


@pytest.mark.asyncio
async def test_get_docs_drive_items():
    """Test get docs method for drive items"""

    # Setup
    source = create_source(SharepointServerDataSource)
    item_content_response = {
        "RootFolder": {
            "ServerRelativeUrl": "/sites/enterprise/ctest/_api",
        },
        "Created": "2023-03-19T05:02:52Z",
        "BaseType": 1,
        "Id": "f764b597-ed44-49be-8867-f8e9ca5d0a6e",
        "LastItemModifiedDate": "2023-03-19T05:02:52Z",
        "ParentWebUrl": "/sites/enterprise/ctest/_api",
        "Title": "HelloWorld",
    }
    drive_content_response = {
        "File": {
            "Length": "3356",
            "Name": "Home.aspx",
            "ServerRelativeUrl": "/sites/enterprise/ctest/SitePages/Home.aspx",
            "TimeCreated": "2022-05-02T07:20:33Z",
            "TimeLastModified": "2022-05-02T07:20:34Z",
            "Title": "Home.aspx",
        },
        "Folder": {"__deferred": {}},
        "Modified": "2022-05-02T07:20:35Z",
        "GUID": "111111122222222-c77f-4ed3-84ef-8a4dd87c80d0",
        "Length": "3356",
        "item_type": "File",
    }
    actual_response = []
    source.sharepoint_client._fetch_data_with_query = Mock(return_value=AsyncIter([]))
    source.sharepoint_client.get_lists = Mock(
        return_value=AsyncIter([item_content_response])
    )
    source.sharepoint_client.get_drive_items = Mock(
        return_value=AsyncIter((drive_content_response, None))
    )
    # Execute
    async for document, _ in source.get_docs():
        actual_response.append(document)
    # Assert
    assert len(actual_response) == 2


@pytest.mark.asyncio
async def test_get_docs_drive_items_for_web_pages():
    # Setup
    source = create_source(SharepointServerDataSource)
    item_content_response = {
        "RootFolder": {
            "ServerRelativeUrl": "/sites/enterprise/ctest/_api",
        },
        "Created": "2023-03-19T05:02:52Z",
        "BaseType": 1,
        "Id": "f764b597-ed44-49be-8867-f8e9ca5d0a6e",
        "LastItemModifiedDate": "2023-03-19T05:02:52Z",
        "ParentWebUrl": "/sites/enterprise/ctest/_api",
        "Title": "Site Pages",
    }
    drive_content_response = {
        "File": {
            "Length": "3356",
            "Name": "Home.aspx",
            "ServerRelativeUrl": "/sites/enterprise/ctest/SitePages/Home.aspx",
            "TimeCreated": "2022-05-02T07:20:33Z",
            "TimeLastModified": "2022-05-02T07:20:34Z",
            "Title": "Home.aspx",
        },
        "Folder": {"__deferred": {}},
        "Modified": "2022-05-02T07:20:35Z",
        "GUID": "111111122222222-c77f-4ed3-84ef-8a4dd87c80d0",
        "Length": "3356",
        "item_type": "File",
    }
    actual_response = []
    source.sharepoint_client._fetch_data_with_query = Mock(return_value=AsyncIter([]))
    source.sharepoint_client.get_lists = Mock(
        return_value=AsyncIter([item_content_response])
    )
    source.sharepoint_client.get_drive_items = Mock(
        return_value=AsyncIter((drive_content_response, None))
    )
    # Execute
    async for document, _ in source.get_docs():
        actual_response.append(document)
    # Assert
    assert len(actual_response) == 2


@pytest.mark.asyncio
async def test_get_docs_when_no_site_available():
    """Test get docs when site is not available method"""

    # Setup
    source = create_source(SharepointServerDataSource)
    actual_response = []
    source.sharepoint_client._fetch_data_with_query = Mock(return_value=AsyncIter([]))
    source.sharepoint_client._fetch_data_with_next_url = Mock(
        return_value=AsyncIter([])
    )
    # Execute
    async for document, _ in source.get_docs():
        actual_response.append(document)
    # Assert
    assert len(actual_response) == 0


@pytest.mark.asyncio
async def test_get_content():
    """Test the get content method"""
    # Setup
    source = create_source(SharepointServerDataSource)
    async_response = MockObjectResponse()

    RESPONSE_CONTENT = "This is a dummy sharepoint body response"
    EXPECTED_ATTACHMENT = {
        "id": 1,
        "server_relative_url": "/url",
        "_timestamp": "2022-06-20T10:37:44Z",
        "size": 11,
        "type": "sites",
        "file_name": "dummy.pdf",
    }
    EXPECTED_CONTENT = {
        "_id": 1,
        "_attachment": "VGhpcyBpcyBhIGR1bW15IHNoYXJlcG9pbnQgYm9keSByZXNwb25zZQ==",
        "_timestamp": "2022-06-20T10:37:44Z",
    }
    source.sharepoint_client._api_call = Mock(return_value=AsyncIter(async_response))
    with mock.patch(
        "aiohttp.StreamReader.iter_chunked",
        return_value=AsyncIter(bytes(RESPONSE_CONTENT, "utf-8")),
    ):
        # Execute
        response_content = await source.sharepoint_client.get_content(
            document=EXPECTED_ATTACHMENT,
            file_relative_url="abc.com",
            site_url="/site",
            doit=True,
        )

    # Assert
    assert response_content == EXPECTED_CONTENT


class ContentResponse:
    content = b"This is a dummy sharepoint body response"


@pytest.mark.asyncio
async def test_get_content_when_size_is_bigger():
    """Test the get content method when document size is greater than the allowed size limit."""
    # Setup
    document = {
        "id": 1,
        "_timestamp": "2022-06-20T10:37:44Z",
        "size": 1048576011,
        "title": "dummy",
        "type": "sites",
        "server_relative_url": "/sites",
        "file_name": "dummy.pdf",
    }
    source = create_source(SharepointServerDataSource)
    # Execute
    response_content = await source.sharepoint_client.get_content(
        document=document, file_relative_url="abc.com", site_url="/site", doit=True
    )

    # Assert
    assert response_content is None


@pytest.mark.asyncio
async def test_get_content_when_doit_is_none():
    """Test the get content method when doit is None"""
    # Setup
    document = {
        "id": 1,
        "_timestamp": "2022-06-20T10:37:44Z",
        "size": 11,
        "type": "sites",
        "file_name": "dummy.pdf",
    }
    source = create_source(SharepointServerDataSource)
    # Execute
    response_content = await source.sharepoint_client.get_content(
        document=document, file_relative_url="abc.com", site_url="/site"
    )

    # Assert
    assert response_content is None


@pytest.mark.asyncio
async def test_fetch_data_with_query_sites():
    """Test get invoke call for sites"""
    # Setup
    source = create_source(SharepointServerDataSource)
    get_response = {
        "value": [
            {
                "Id": "111111122222222-0fd8-471c-96aa-c75f71293131",
                "ServerRelativeUrl": "/sites/collection1",
                "Title": "ctest",
            }
        ]
    }
    source.sharepoint_client._api_call = Mock(
        return_value=async_native_coroutine_generator(get_response)
    )
    actual_response = []

    # Execute
    async for response in source.sharepoint_client._fetch_data_with_query(
        site_url="/sites/collection1/_api/web/webs", param_name="sites"
    ):
        actual_response.extend(response)

    # Assert
    assert [actual_response] == [
        [
            {
                "Id": "111111122222222-0fd8-471c-96aa-c75f71293131",
                "ServerRelativeUrl": "/sites/collection1",
                "Title": "ctest",
            }
        ]
    ]


@pytest.mark.asyncio
async def test_fetch_data_with_query_list():
    """Test get invoke call for list"""
    # Setup
    source = create_source(SharepointServerDataSource)
    get_response = {
        "value": [
            {
                "Id": "111111122222222-0fd8-471c-96aa-c75f71293131",
                "ServerRelativeUrl": "/sites/collection1",
                "Title": "ctest",
            }
        ]
    }
    source.sharepoint_client._api_call = Mock(
        return_value=async_native_coroutine_generator(get_response)
    )
    actual_response = []

    # Execute
    async for response in source.sharepoint_client._fetch_data_with_query(
        site_url="/sites/collection1/_api/web/webs", param_name="lists"
    ):
        actual_response.extend(response)

    # Assert
    assert [actual_response] == [
        [
            {
                "Id": "111111122222222-0fd8-471c-96aa-c75f71293131",
                "ServerRelativeUrl": "/sites/collection1",
                "Title": "ctest",
            }
        ]
    ]


@pytest.mark.asyncio
async def test_fetch_data_with_next_url_items():
    """Test get invoke call for drive item"""
    # Setup
    source = create_source(SharepointServerDataSource)
    get_response = {
        "value": [
            {
                "Id": "111111122222222-0fd8-471c-96aa-c75f71293131",
                "ServerRelativeUrl": "/sites/collection1",
                "Title": "ctest",
            }
        ]
    }
    actual_response = []
    source.sharepoint_client._api_call = Mock(
        return_value=async_native_coroutine_generator(get_response)
    )
    # Execute
    async for response in source.sharepoint_client._fetch_data_with_next_url(
        site_url="/sites/collection1/_api/web/webs",
        list_id="123abc",
        param_name="drive_items",
    ):
        actual_response.append(response)

    # Assert
    assert actual_response == [
        [
            {
                "Id": "111111122222222-0fd8-471c-96aa-c75f71293131",
                "ServerRelativeUrl": "/sites/collection1",
                "Title": "ctest",
            }
        ]
    ]


@pytest.mark.asyncio
async def test_fetch_data_with_next_url_list_items():
    """Test get invoke call when for list item"""
    # Setup
    source = create_source(SharepointServerDataSource)
    get_response = {
        "value": [
            {
                "Id": "111111122222222-0fd8-471c-96aa-c75f71293131",
                "ServerRelativeUrl": "/sites/collection1",
                "Title": "ctest",
            }
        ]
    }
    actual_response = []
    source.sharepoint_client._api_call = Mock(
        return_value=async_native_coroutine_generator(get_response)
    )
    # Execute
    async for response in source.sharepoint_client._fetch_data_with_next_url(
        site_url="/sites/collection1/_api/web/webs",
        list_id="123abc",
        param_name="list_items",
    ):
        actual_response.append(response)

    # Assert
    assert actual_response == [
        [
            {
                "Id": "111111122222222-0fd8-471c-96aa-c75f71293131",
                "ServerRelativeUrl": "/sites/collection1",
                "Title": "ctest",
            }
        ]
    ]


class ClientSession:
    """Mock Client Session Class"""

    async def close(self):
        """Close method of Mock Client Session Class"""
        pass


@pytest.mark.asyncio
async def test_close_with_client_session():
    """Test close method of SharepointServerDataSource with client session"""

    # Setup
    source = create_source(SharepointServerDataSource)
    source.sharepoint_client.session = ClientSession()

    # Execute
    await source.sharepoint_client.close_session()


@pytest.mark.asyncio
async def test_close_without_client_session():
    """Test close method of SharepointServerDataSource without client session"""

    # Setup
    source = create_source(SharepointServerDataSource)
    source.sharepoint_client.session = None

    # Execute
    await source.sharepoint_client.close_session()


@pytest.mark.asyncio
async def test_api_call_negative(patch_default_wait_multiplier):
    """Tests the _api_call function while getting an exception."""

    # Setup
    source = create_source(SharepointServerDataSource)
    source.sharepoint_client.retry_count = 2

    with patch.object(
        aiohttp.ClientSession, "get", side_effect=Exception(EXCEPTION_MESSAGE)
    ):
        source.sharepoint_client.session = source.sharepoint_client._get_session()
        with pytest.raises(Exception):
            # Execute
            await anext(source.sharepoint_client._api_call(url_name="ping"))


@pytest.mark.asyncio
async def test_api_call_successfully():
    """Tests the _api_call function."""

    # Setup
    source = create_source(SharepointServerDataSource)
    mocked_response = [{"name": "dummy_project", "id": "test123"}]
    mock_token = {"access_token": "test2344", "expires_in": "1234555"}
    async_response = MockResponse(mocked_response, 200)
    async_response_token = MockResponse(mock_token, 200)

    with patch("aiohttp.ClientSession.get", return_value=async_response), patch(
        "aiohttp.request", return_value=async_response_token
    ):
        source.sharepoint_client._get_session()
        # Execute
        async for response in source.sharepoint_client._api_call(
            url_name="ping", site_collections="abc", host_url="sharepoint.com"
        ):
            # Assert
            assert response == [{"name": "dummy_project", "id": "test123"}]


@pytest.fixture
def patch_default_wait_multiplier():
    with mock.patch("connectors.sources.sharepoint_server.RETRY_INTERVAL", 0):
        yield


@pytest.mark.asyncio
async def test_api_call_when_token_is_expired(patch_default_wait_multiplier):
    """Tests the _api_call function while token expire."""

    # Setup
    source = create_source(SharepointServerDataSource)
    mock_response = {"access_token": "test2344", "expires_in": "1234555"}
    async_response = MockResponse(mock_response, 401)
    source.sharepoint_client.retry_count = 2
    async_response.headers = {"x-ms-diagnostics": "token has expired"}

    with patch.object(
        aiohttp.ClientSession,
        "get",
        side_effect=aiohttp.client_exceptions.ClientResponseError(
            request_info=aiohttp.client_reqrep.RequestInfo,
            history=None,
            status=401,
            message="Unauthorized",
            headers=async_response.headers,
        ),
    ):
        with patch("aiohttp.request", return_value=async_response):
            with pytest.raises(aiohttp.client_exceptions.ClientResponseError):
                source.sharepoint_client._get_session()

                # Execute
                await anext(
                    source.sharepoint_client._api_call(
                        url_name="attachment", url="abc.com"
                    )
                )
            await source.close()


class ClientErrorException:
    real_url = ""


@pytest.mark.asyncio
async def test_api_call_when_status_429_exception(patch_default_wait_multiplier):
    # Setup
    source = create_source(SharepointServerDataSource)
    mock_response = {"access_token": "test2344", "expires_in": "1234555"}
    async_response = MockResponse(mock_response, 429)
    source.sharepoint_client.retry_count = 2

    with patch.object(
        aiohttp.ClientSession,
        "get",
        side_effect=aiohttp.ClientResponseError(
            history="history", request_info=ClientErrorException
        ),
    ):
        with patch("aiohttp.request", return_value=async_response):
            source.sharepoint_client._get_session()
            with pytest.raises(aiohttp.ClientResponseError):
                # Execute
                await anext(
                    source.sharepoint_client._api_call(
                        url_name="attachment", url="abc.com"
                    )
                )
            await source.close()


@pytest.mark.asyncio
async def test_api_call_when_server_is_down(patch_default_wait_multiplier):
    """Tests the _api_call function while server gets disconnected."""
    # Setup
    source = create_source(SharepointServerDataSource)
    mock_response = {"access_token": "test2344", "expires_in": "1234555"}
    async_response = MockResponse(mock_response, 200)
    source.sharepoint_client.retry_count = 2
    with patch.object(
        aiohttp.ClientSession,
        "get",
        side_effect=aiohttp.ServerDisconnectedError("Something went wrong"),
    ):
        with patch("aiohttp.request", return_value=async_response):
            source.sharepoint_client._get_session()
            with pytest.raises(aiohttp.ServerDisconnectedError):
                # Execute
                await anext(
                    source.sharepoint_client._api_call(
                        url_name="attachment", url="abc.com"
                    )
                )


@pytest.mark.asyncio
async def test_get_session():
    """Test that the instance of session returned is always the same for the datasource class."""
    source = create_source(SharepointServerDataSource)
    first_instance = source.sharepoint_client._get_session()
    second_instance = source.sharepoint_client._get_session()
    assert first_instance is second_instance


@pytest.mark.asyncio
async def test_get_site_pages_content():
    # Setup
    source = create_source(SharepointServerDataSource)
    EXPECTED_ATTACHMENT = {
        "id": 1,
        "server_relative_url": "/url",
        "_timestamp": "2022-06-20T10:37:44Z",
        "size": 11,
        "type": "sites",
        "file_name": "dummy.pdf",
    }
    RESPONSE_DATA = {"WikiField": "This is a dummy sharepoint body response"}
    EXPECTED_CONTENT = {
        "_id": 1,
        "_attachment": "VGhpcyBpcyBhIGR1bW15IHNoYXJlcG9pbnQgYm9keSByZXNwb25zZQ==",
        "_timestamp": "2022-06-20T10:37:44Z",
    }
    # Execute
    response_content = await source.sharepoint_client.get_site_pages_content(
        document=EXPECTED_ATTACHMENT,
        list_response=RESPONSE_DATA,
        doit=True,
    )
    # Assert
    assert response_content == EXPECTED_CONTENT


async def coroutine_generator(item):
    """create a method for returning fake coroutine value for
    Args:
        item: Value for converting into coroutine
    """
    return item


@pytest.mark.asyncio
async def test_get_site_pages_content_when_doit_is_none():
    document = {"title": "Home.aspx", "type": "File", "size": 1000000}
    source = create_source(SharepointServerDataSource)
    response_content = await source.sharepoint_client.get_site_pages_content(
        document=document,
        list_response={},
        doit=None,
    )

    assert response_content is None


@pytest.mark.asyncio
async def test_get_site_pages_content_for_wikifiled_none():
    # Setup
    source = create_source(SharepointServerDataSource)
    EXPECTED_ATTACHMENT = {"title": "Home.aspx", "type": "File", "size": "1000000"}
    # Execute
    response_content = await source.sharepoint_client.get_site_pages_content(
        document=EXPECTED_ATTACHMENT,
        list_response={"WikiField": None},
        doit=True,
    )
    # Assert
    assert response_content is None


@pytest.mark.asyncio
async def test_get_list_items_with_no_extension():
    # Setup
    source = create_source(SharepointServerDataSource)
    api_response = [
        {
            "AttachmentFiles": [
                {
                    "FileName": "s3 queries",
                    "ServerRelativeUrl": "/sites/collection1/ctest/Lists/ctestlist/Attachments/1/v4",
                }
            ],
            "Title": "HelloWorld",
            "Id": 1,
            "FileRef": "/site",
            "ContentTypeId": "12345",
            "Modified": "2022-06-20T10:04:03Z",
            "Created": "2022-06-20T10:04:03Z",
            "EditorId": 1073741823,
            "AuthorId": 1073741823,
            "Attachments": True,
            "GUID": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
            "Length": "12",
        },
    ]
    target_response = [
        {
            "AttachmentFiles": [
                {
                    "FileName": "s3 queries",
                    "ServerRelativeUrl": "/sites/collection1/ctest/Lists/ctestlist/Attachments/1/v4",
                }
            ],
            "Title": "HelloWorld",
            "Id": 1,
            "FileRef": "/site",
            "ContentTypeId": "12345",
            "Modified": "2022-06-20T10:04:03Z",
            "Created": "2022-06-20T10:04:03Z",
            "EditorId": 1073741823,
            "AuthorId": 1073741823,
            "Attachments": True,
            "GUID": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
            "Length": None,
            "_id": "1",
            "url": "http://127.0.0.1:8491/sites/collection1/ctest/Lists/ctestlist/Attachments/1/v4",
            "file_name": "s3 queries",
            "server_relative_url": "/sites/collection1/ctest/Lists/ctestlist/Attachments/1/v4",
        },
    ]
    attachment_response = {"UniqueId": "1"}
    source.sharepoint_client._fetch_data_with_next_url = Mock(
        return_value=AsyncIter(api_response)
    )
    source.sharepoint_client._api_call = Mock(
        return_value=async_native_coroutine_generator(attachment_response)
    )
    expected_response = []
    # Execute
    async for item, _ in source.sharepoint_client.get_list_items(
        list_id="620070a1-ee50-4585-b6a7-0f6210b1a69d",
        site_url="/sites/enterprise/ctest",
        server_relative_url="/sites/enterprise/site1^",
        selected_field="",
    ):
        expected_response.append(item)
    # Assert
    assert expected_response == target_response


@pytest.mark.asyncio
async def test_get_list_items_with_extension_only():
    # Setup
    source = create_source(SharepointServerDataSource)
    api_response = [
        {
            "AttachmentFiles": [
                {
                    "FileName": ".env",
                    "ServerRelativeUrl": "/sites/collection1/ctest/Lists/ctestlist/Attachments/1/.env",
                }
            ],
            "Title": "HelloWorld",
            "Id": 1,
            "FileRef": "/site",
            "ContentTypeId": "12345",
            "Modified": "2022-06-20T10:04:03Z",
            "Created": "2022-06-20T10:04:03Z",
            "EditorId": 1073741823,
            "AuthorId": 1073741823,
            "Attachments": True,
            "GUID": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
            "Length": "12",
        },
    ]
    target_response = [
        {
            "AttachmentFiles": [
                {
                    "FileName": ".env",
                    "ServerRelativeUrl": "/sites/collection1/ctest/Lists/ctestlist/Attachments/1/.env",
                }
            ],
            "Title": "HelloWorld",
            "Id": 1,
            "FileRef": "/site",
            "ContentTypeId": "12345",
            "Modified": "2022-06-20T10:04:03Z",
            "Created": "2022-06-20T10:04:03Z",
            "EditorId": 1073741823,
            "AuthorId": 1073741823,
            "Attachments": True,
            "GUID": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
            "Length": None,
            "_id": "1",
            "url": "http://127.0.0.1:8491/sites/collection1/ctest/Lists/ctestlist/Attachments/1/.env",
            "file_name": ".env",
            "server_relative_url": "/sites/collection1/ctest/Lists/ctestlist/Attachments/1/.env",
        },
    ]
    attachment_response = {"UniqueId": "1"}
    source.sharepoint_client._fetch_data_with_next_url = Mock(
        return_value=AsyncIter(api_response)
    )
    source.sharepoint_client._api_call = Mock(
        return_value=async_native_coroutine_generator(attachment_response)
    )
    expected_response = []
    # Execute
    async for item, _ in source.sharepoint_client.get_list_items(
        list_id="620070a1-ee50-4585-b6a7-0f6210b1a69d",
        site_url="/sites/enterprise/ctest",
        server_relative_url="/sites/enterprise/site1^",
        selected_field="",
    ):
        expected_response.append(item)
    # Assert
    assert expected_response == target_response
