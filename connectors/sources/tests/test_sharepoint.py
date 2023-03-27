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

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.sharepoint import SharepointDataSource
from connectors.sources.tests.support import create_source

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


def test_get_configuration(patch_logger):
    """Tests the get configurations method of the Sharepoint source class."""
    # Setup
    klass = SharepointDataSource

    # Execute
    config = DataSourceConfiguration(config=klass.get_default_configuration())

    # Assert
    assert config["host_url"] == HOST_URL


@pytest.mark.asyncio
async def test_ping_for_successful_connection(patch_logger):
    """Tests the ping functionality for ensuring connection to the Sharepoint."""

    # Setup
    source = create_source(SharepointDataSource)
    source.is_cloud = False
    source._api_call = Mock(return_value=async_native_coroutine_generator(200))

    # Execute
    await source.ping()


@pytest.mark.asyncio
async def test_ping_for_failed_connection_exception(patch_logger):
    """Tests the ping functionality when connection can not be established to Sharepoint."""

    # Setup
    source = create_source(SharepointDataSource)
    source.retry_count = 0
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
async def test_validate_config_when_host_url_is_empty():
    """This function test validate_config when host_url is empty"""
    # Setup
    source = create_source(SharepointDataSource)
    source.configuration.set_field(name="host_url", value="")

    # Execute
    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_for_ssl_enabled():
    """This function test validate_config when ssl is enabled and certificate is missing"""
    # Setup
    source = create_source(SharepointDataSource)
    source.ssl_enabled = True

    # Execute
    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_api_call_for_exception(patch_logger):
    """This function test _api_call when credentials are incorrect"""
    # Setup
    source = create_source(SharepointDataSource)
    source.retry_count = 0
    # Execute
    with patch.object(
        aiohttp, "ClientSession", side_effect=Exception(EXCEPTION_MESSAGE)
    ):
        with pytest.raises(Exception):
            await source._api_call(url_name="lists", url="abc")


def test_prepare_drive_items_doc(patch_logger):
    """Test the prepare drive items method"""
    # Setup
    source = create_source(SharepointDataSource)
    list_items = {
        "File": {
            "Name": "dummy",
            "TimeLastModified": "2023-01-30T12:48:31Z",
            "TimeCreated": "2023-01-30T12:48:31Z",
            "Length": "12",
            "ServerRelativeUrl": "/site",
        },
        "GUID": 1,
    }
    document_type = "drive_item"
    expected_response = {
        "_id": 1,
        "type": "File",
        "size": "12",
        "title": "dummy",
        "creation_time": "2023-01-30T12:48:31Z",
        "_timestamp": "2023-01-30T12:48:31Z",
        "url": f"{HOST_URL}/site",
    }

    # Execute
    target_response = source.format_document(
        item=list_items, document_type=document_type, item_type="File"
    )
    # Assert
    assert target_response == expected_response


def test_prepare_list_items_doc(patch_logger):
    """Test the prepare list items method"""
    # Setup
    source = create_source(SharepointDataSource)
    list_items = {
        "Title": "dummy",
        "EditorId": "123",
        "Created": "2023-01-30T12:48:31Z",
        "Modified": "2023-01-30T12:48:31Z",
        "GUID": 1,
        "FileRef": "/site",
        "url": f"{HOST_URL}/site",
    }
    document_type = "list_item"
    server_relative_url = "/site"
    expected_response = {
        "type": "list_item",
        "_id": 1,
        "file_name": "filename",
        "size": None,
        "title": "dummy",
        "author_id": "123",
        "creation_time": "2023-01-30T12:48:31Z",
        "_timestamp": "2023-01-30T12:48:31Z",
        "url": f"{HOST_URL}/site",
    }

    # Execute
    target_response = source.format_document(
        item=list_items,
        document_type=document_type,
        item_type=server_relative_url,
        file_name="filename",
    )
    # Assert
    assert target_response == expected_response


def test_prepare_sites_doc(patch_logger):
    """Test the method for preparing sites document"""
    # Setup
    source = create_source(SharepointDataSource)
    list_items = {
        "Title": "dummy",
        "LastItemModifiedDate": "2023-01-30T12:48:31Z",
        "Created": "2023-01-30T12:48:31Z",
        "Id": 1,
        "Url": "sharepoint.com",
        "ServerRelativeUrl": "/site",
    }
    document_type = "sites"
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
    target_response = source.format_document(
        item=list_items, document_type=document_type
    )
    # Assert
    assert target_response == expected_response


@pytest.mark.asyncio
async def test_get_sites_when_no_site_available(patch_logger):
    """Test get sites method with valid details"""
    # Setup
    source = create_source(SharepointDataSource)
    api_response = []
    # Execute
    source.invoke_get_call = Mock(return_value=AsyncIter(api_response))
    actual_response = None
    async for item in source.get_sites(site_url="sites/collection1/ctest"):
        actual_response = item

    # Assert
    assert actual_response is None


@pytest.mark.asyncio
async def test_get_list_items(patch_logger):
    """Test get list items method with valid details"""
    # Setup
    source = create_source(SharepointDataSource)
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
            "Attachments": True,
            "GUID": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
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
            "Attachments": False,
            "GUID": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
        },
    ]
    target_response = [
        {
            "type": "list_item",
            "_id": "1",
            "file_name": "s3 queries.txt",
            "size": None,
            "url": f"{HOST_URL}/sites/collection1/ctest/Lists/ctestlist/Attachments/1/v4.txt",
            "title": "HelloWorld",
            "author_id": 1073741823,
            "creation_time": "2022-06-20T10:04:03Z",
            "_timestamp": "2022-06-20T10:04:03Z",
        },
        {
            "type": "list_item",
            "_id": "111111122222222-adfa-4e4f-93c4-bfedddda8510",
            "file_name": None,
            "size": None,
            "url": f"{HOST_URL}/sites/enterprise/site1/DispForm.aspx?ID=1&Source={HOST_URL}/sites/enterprise/site1/AllItems.aspx&ContentTypeId=12345",
            "title": "HelloWorld",
            "author_id": 1073741823,
            "creation_time": "2022-06-20T10:04:03Z",
            "_timestamp": "2022-06-20T10:04:03Z",
        },
    ]
    attachment_response = {"UniqueId": "1"}
    source.invoke_get_call = Mock(return_value=AsyncIter(api_response))
    source._api_call = Mock(
        return_value=async_native_coroutine_generator(attachment_response)
    )
    expected_response = []
    # Execute
    async for item, _ in source.get_list_items(
        list_id="620070a1-ee50-4585-b6a7-0f6210b1a69d",
        site_url="/sites/enterprise/ctest",
        server_relative_url="/sites/enterprise/site1",
    ):
        expected_response.append(item)
    # Assert
    assert expected_response == target_response


@pytest.mark.asyncio
async def test_get_drive_items(patch_logger):
    """Test get drive items method with valid details"""
    # Setup
    source = create_source(SharepointDataSource)
    api_response = [
        {
            "File": {
                "Length": "3356",
                "Name": "Home.txt",
                "ServerRelativeUrl": "/sites/enterprise/ctest/SitePages/Home.aspx",
                "TimeCreated": "2022-05-02T07:20:33Z",
                "TimeLastModified": "2022-05-02T07:20:34Z",
                "Title": None,
            },
            "Folder": {"__deferred": {}},
            "Modified": "2022-05-02T07:20:35Z",
            "GUID": "111111122222222-c77f-4ed3-84ef-8a4dd87c80d0",
        },
        {
            "File": {},
            "Folder": {
                "Length": "3356",
                "Name": "Home.txt",
                "ServerRelativeUrl": "/sites/enterprise/ctest/SitePages/Home.aspx",
                "TimeCreated": "2022-05-02T07:20:33Z",
                "TimeLastModified": "2022-05-02T07:20:34Z",
                "Title": None,
            },
            "Modified": "2022-05-02T07:20:35Z",
            "GUID": "111111122222222-c77f-4ed3-084ef-8a4dd87c80d0",
        },
    ]
    target_response = [
        (
            {
                "type": "File",
                "_id": "111111122222222-c77f-4ed3-84ef-8a4dd87c80d0",
                "size": "3356",
                "url": f"{HOST_URL}/sites/enterprise/ctest/SitePages/Home.aspx",
                "title": "Home.txt",
                "creation_time": "2022-05-02T07:20:33Z",
                "_timestamp": "2022-05-02T07:20:34Z",
            },
            "%2Fsites%2Fenterprise%2Fctest%2FSitePages%2FHome.aspx",
        ),
        (
            {
                "type": "Folder",
                "_id": "111111122222222-c77f-4ed3-084ef-8a4dd87c80d0",
                "size": None,
                "url": f"{HOST_URL}/sites/enterprise/ctest/SitePages/Home.aspx",
                "title": "Home.txt",
                "creation_time": "2022-05-02T07:20:33Z",
                "_timestamp": "2022-05-02T07:20:34Z",
            },
            None,
        ),
    ]

    source.invoke_get_call = Mock(return_value=AsyncIter(api_response))
    expected_response = []
    # Execute
    async for item in source.get_drive_items(
        list_id="620070a1-ee50-4585-b6a7-0f6210b1a69d",
        site_url="/sites/enterprise/ctest",
        server_relative_url=None,
    ):
        expected_response.append(item)
    # Assert
    assert target_response == expected_response


@pytest.mark.asyncio
async def test_get_lists_and_items(patch_logger):
    """Test get items method with valid details"""
    # Setup
    source = create_source(SharepointDataSource)
    lists = [
        {
            "BaseType": 0,
            "Id": "111111122222222-1c10-4277-8f0c-6fb9806c4a21",
            "ParentWebUrl": "/sites/enterprise/sptest",
            "Title": "dummy",
            "LastItemModifiedDate": "2023-01-30T12:48:31Z",
            "Created": "2023-01-30T12:48:31Z",
            "RootFolder": {"ServerRelativeUrl": "/site"},
        },
        {
            "BaseType": 1,
            "Id": "111111122222222-1c10-4277-8f0c-6fb9806c4a21",
            "ParentWebUrl": "/sites/enterprise/sptest",
            "Title": "dummy",
            "LastItemModifiedDate": "2023-01-30T12:48:31Z",
            "Created": "2023-01-30T12:48:31Z",
            "RootFolder": {"ServerRelativeUrl": "/site"},
        },
    ]
    source.invoke_get_call = Mock(return_value=AsyncIter(lists))
    drive_item = [
        {
            "_id": 1,
            "type": "drive_item",
            "server_relative_url": "/site",
            "title": "dummy",
            "creation_time": "2023-01-30T12:48:31Z",
            "_timestamp": "2023-01-30T12:48:31Z",
            "length": "12",
        }
    ]
    list_item = [
        {
            "_id": 1,
            "type": "drive_item",
            "server_relative_url": "/site",
            "title": "dummy",
            "author_id": "123",
            "creation_time": "2023-01-30T12:48:31Z",
            "_timestamp": "2023-01-30T12:48:31Z",
        }
    ]
    expected_response = [
        (
            {
                "type": "lists",
                "url": f"{HOST_URL}/site",
                "title": "dummy",
                "parent_web_url": "/sites/enterprise/sptest",
                "_id": "111111122222222-1c10-4277-8f0c-6fb9806c4a21",
                "_timestamp": "2023-01-30T12:48:31Z",
                "creation_time": "2023-01-30T12:48:31Z",
            },
            None,
        ),
        (
            [
                {
                    "_id": 1,
                    "type": "drive_item",
                    "server_relative_url": "/site",
                    "title": "dummy",
                    "author_id": "123",
                    "creation_time": "2023-01-30T12:48:31Z",
                    "_timestamp": "2023-01-30T12:48:31Z",
                }
            ],
            None,
        ),
        (
            {
                "type": "document_library",
                "url": f"{HOST_URL}/site",
                "title": "dummy",
                "parent_web_url": "/sites/enterprise/sptest",
                "_id": "111111122222222-1c10-4277-8f0c-6fb9806c4a21",
                "_timestamp": "2023-01-30T12:48:31Z",
                "creation_time": "2023-01-30T12:48:31Z",
            },
            None,
        ),
        (
            [
                {
                    "_id": 1,
                    "type": "drive_item",
                    "server_relative_url": "/site",
                    "title": "dummy",
                    "creation_time": "2023-01-30T12:48:31Z",
                    "_timestamp": "2023-01-30T12:48:31Z",
                    "length": "12",
                }
            ],
            None,
        ),
    ]
    source.get_drive_items = Mock(
        return_value=async_native_coroutine_generator((drive_item, None))
    )
    source.get_list_items = Mock(
        return_value=async_native_coroutine_generator((list_item, None))
    )
    actual_response = []
    # Execute
    async for items in source.get_lists_and_items(site="/sites/enterprise/sptest"):
        actual_response.append(items)
    # Assert
    assert actual_response == expected_response


@pytest.mark.asyncio
async def test_get_docs(patch_logger):
    """Test get docs method"""

    # Setup
    source = create_source(SharepointDataSource)
    item_content_response = {
        "_id": "11111-adfa-4e4f-93c4-bfedddda8510",
        "type": "list_item",
        "server_relative_url": "/sites/enterprise/ctest/_api",
        "file_name": "s3 queries.txt",
        "title": "HelloWorld",
        "editor_id": 1073741823,
        "author_id": 1073741823,
        "creation_time": "2022-06-20T10:04:03Z",
        "_timestamp": "2022-06-20T10:04:03Z",
    }
    actual_response = []
    source.get_sites = Mock(return_value=AsyncIter(item_content_response))
    source.get_lists_and_items = Mock(return_value=AsyncIter(([], None)))
    source.get_content = Mock(return_value=None)
    # Execute
    async for document, _ in source.get_docs():
        actual_response.append(document)
    # Assert
    assert len(actual_response) == 3


@pytest.mark.asyncio
async def test_get_docs_when_no_site_available(patch_logger):
    """Test get docs when site is not available method"""

    # Setup
    source = create_source(SharepointDataSource)
    actual_response = []
    source.invoke_get_call = Mock(return_value=AsyncIter([]))
    # Execute
    async for document, _ in source.get_docs():
        actual_response.append(document)
    # Assert
    assert len(actual_response) == 0


@pytest.mark.asyncio
async def test_get_content(patch_logger):
    """Test the get content method"""
    # Setup
    source = create_source(SharepointDataSource)
    async_response = MockObjectResponse()

    RESPONSE_CONTENT = "This is a dummy sharepoint body response"
    EXPECTED_ATTACHMENT = {
        "id": 1,
        "server_relative_url": "/url",
        "_timestamp": "2022-06-20T10:37:44Z",
        "size": "11",
        "type": "sites",
    }
    EXPECTED_CONTENT = {
        "_id": 1,
        "_attachment": "VGhpcyBpcyBhIGR1bW15IHNoYXJlcG9pbnQgYm9keSByZXNwb25zZQ==",
        "_timestamp": "2022-06-20T10:37:44Z",
    }
    source._api_call = Mock(return_value=AsyncIter(async_response))
    with mock.patch(
        "aiohttp.StreamReader.iter_chunked",
        return_value=AsyncIter(bytes(RESPONSE_CONTENT, "utf-8")),
    ):
        # Execute
        response_content = await source.get_content(
            document=EXPECTED_ATTACHMENT,
            file_relative_url="abc.com",
            site_url="/site",
            doit=True,
        )

    # Assert
    assert response_content == EXPECTED_CONTENT


@pytest.mark.asyncio
async def test_get_content_when_size_is_bigger(patch_logger):
    """Test the get content method when document size is greater than the allowed size limit."""
    # Setup
    document = {
        "id": 1,
        "_timestamp": "2022-06-20T10:37:44Z",
        "size": "1048576011",
        "title": "dummy",
        "type": "sites",
        "server_relative_url": "/sites",
    }

    source = create_source(SharepointDataSource)
    # Execute
    response_content = await source.get_content(
        document=document, file_relative_url="abc.com", site_url="/site", doit=True
    )

    # Assert
    assert response_content is None


@pytest.mark.asyncio
async def test_get_content_when_doit_is_none(patch_logger):
    """Test the get content method when doit is None"""
    # Setup
    document = {
        "id": 1,
        "_timestamp": "2022-06-20T10:37:44Z",
        "length": "11",
        "type": "sites",
    }
    source = create_source(SharepointDataSource)
    # Execute
    response_content = await source.get_content(
        document=document, file_relative_url="abc.com", site_url="/site"
    )

    # Assert
    assert response_content is None


@pytest.mark.asyncio
async def test_get_invoke_call_with_list(patch_logger):
    """Test get invoke call when param name is lists"""
    # Setup
    source = create_source(SharepointDataSource)
    get_response = {
        "value": [
            {
                "Id": "111111122222222-0fd8-471c-96aa-c75f71293131",
                "ServerRelativeUrl": "/sites/collection1",
                "Title": "ctest",
            }
        ]
    }
    source._api_call = Mock(return_value=async_native_coroutine_generator(get_response))
    actual_response = []

    # Execute
    async for response in source.invoke_get_call(
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
async def test_get_invoke_call_with_drive_items(patch_logger):
    """Test get invoke call when param name is drive_item"""
    # Setup
    source = create_source(SharepointDataSource)
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
    source._api_call = Mock(return_value=async_native_coroutine_generator(get_response))
    # Execute
    async for response in source.invoke_get_call(
        site_url="/sites/collection1/_api/web/webs", param_name="drive_item"
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
async def test_close_with_client_session(patch_logger):
    """Test close method of SharepointDataSource with client session"""

    # Setup
    source = create_source(SharepointDataSource)
    source.session = ClientSession()

    # Execute
    await source.close()


@pytest.mark.asyncio
async def test_close_without_client_session(patch_logger):
    """Test close method of SharepointDataSource without client session"""

    # Setup
    source = create_source(SharepointDataSource)
    source.session = None

    # Execute
    await source.close()


@pytest.mark.asyncio
async def test_api_call_negative():
    """Tests the _api_call function while getting an exception."""

    # Setup
    source = create_source(SharepointDataSource)
    source.retry_count = 0

    with patch.object(
        aiohttp.ClientSession, "get", side_effect=Exception(EXCEPTION_MESSAGE)
    ):
        source.session = source._generate_session()
        with pytest.raises(Exception):
            # Execute
            await anext(source._api_call(url_name="ping"))


@pytest.mark.asyncio
async def test_api_call_successfully():
    """Tests the _api_call function."""

    # Setup
    source = create_source(SharepointDataSource)
    mocked_response = [{"name": "dummy_project", "id": "test123"}]
    mock_token = {"access_token": "test2344", "expires_in": "1234555"}
    async_response = MockResponse(mocked_response, 200)
    async_response_token = MockResponse(mock_token, 200)

    with patch("aiohttp.ClientSession.get", return_value=async_response), patch(
        "aiohttp.request", return_value=async_response_token
    ):
        await source._generate_session()
        # Execute
        async for response in source._api_call(
            url_name="ping", site_collections="abc", host_url="sharepoint.com"
        ):
            # Assert
            assert response == [{"name": "dummy_project", "id": "test123"}]


@pytest.mark.asyncio
async def test_api_call_when_server_is_down():
    """Tests the _api_call function while server gets disconnected."""

    # Setup
    source = create_source(SharepointDataSource)
    source.retry_count = 0
    mock_response = {"access_token": "test2344", "expires_in": "1234555"}
    async_response = MockResponse(mock_response, 200)

    with patch.object(
        aiohttp.ClientSession,
        "get",
        side_effect=aiohttp.ServerDisconnectedError("Something went wrong"),
    ):
        with patch("aiohttp.request", return_value=async_response):
            await source._generate_session()
            with pytest.raises(aiohttp.ServerDisconnectedError):
                # Execute
                await anext(source._api_call(url_name="attachment", url="abc.com"))


@pytest.mark.asyncio
async def test_set_access_token():
    """This method tests set access token  api call"""
    # Setup
    source = create_source(SharepointDataSource)
    mock_token = {"access_token": "test2344", "expires_in": "1234555"}
    async_response_token = MockResponse(mock_token, 200)

    # Execute
    with patch("aiohttp.request", return_value=async_response_token):
        await source._set_access_token()
        # Assert
        assert source.access_token == "test2344"


@pytest.mark.asyncio
async def test_set_access_token_when_token_expires_at_is_str():
    """This method tests set access token  api call when token_expires_at type is str"""
    # Setup
    source = create_source(SharepointDataSource)
    source.token_expires_at = "2023-02-10T09:02:23.629821"
    mock_token = {"access_token": "test2344", "expires_in": "1234555"}
    async_response_token = MockResponse(mock_token, 200)
    actual_response = source._set_access_token()

    # Execute
    with patch("aiohttp.request", return_value=async_response_token):
        actual_response = await source._set_access_token()
        # Assert
        assert actual_response is None


@pytest.mark.asyncio
async def test_api_call_when_token_is_expired():
    """Tests the _api_call function while token expire."""

    # Setup
    source = create_source(SharepointDataSource)
    source.retry_count = 0
    mock_response = {"access_token": "test2344", "expires_in": "1234555"}
    async_response = MockResponse(mock_response, 401)

    # Add a custom header to the mock response
    async_response.headers = {"x-ms-diagnostics": "token has expired"}

    with patch.object(
        aiohttp.ClientSession,
        "get",
        side_effect=aiohttp.ClientResponseError(
            request_info=None,
            history=None,
            status=401,
            message="Unauthorized",
            headers=async_response.headers,
        ),
    ):
        with patch("aiohttp.request", return_value=async_response):
            await source._generate_session()
            with pytest.raises(aiohttp.ClientResponseError):
                # Execute
                await anext(source._api_call(url_name="attachment", url="abc.com"))
