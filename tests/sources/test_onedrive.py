#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the OneDrive source class methods"""
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp import StreamReader
from aiohttp.client_exceptions import ClientResponseError

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.onedrive import (
    AccessToken,
    OneDriveClient,
    OneDriveDataSource,
    TokenRetrievalError,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source

EXPECTED_USERS = [
    "3fada5d6-125c-4aaa-bc23-09b5d301b2a7",
    "8e083933-1720-4d2f-80d0-3976669b40ee",
]
RESPONSE_USERS = {
    "value": [
        {
            "displayName": "Adele Vance",
            "givenName": "Adele",
            "jobTitle": "Retail Manager",
            "mail": "AdeleV@w076v.onmicrosoft.com",
            "officeLocation": "18/2111",
            "preferredLanguage": "en-US",
            "id": "3fada5d6-125c-4aaa-bc23-09b5d301b2a7",
        },
        {
            "displayName": "Alex Wilber",
            "givenName": "Alex",
            "jobTitle": "Marketing Assistant",
            "mail": "AlexW@w076v.onmicrosoft.com",
            "officeLocation": "131/1104",
            "preferredLanguage": "en-US",
            "id": "8e083933-1720-4d2f-80d0-3976669b40ee",
        },
    ]
}

RESPONSE_FILES = {
    "value": [
        {
            "createdDateTime": "2023-04-23T05:09:39Z",
            "id": "01DABHRNV6Y2GOVW7725BZO354PWSELRRZ",
            "lastModifiedDateTime": "2023-08-04T02:55:19Z",
            "name": "root",
            "webUrl": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents",
            "size": 206100,
        },
        {
            "createdDateTime": "2023-05-01T09:09:19Z",
            "eTag": '"{FF3F899A-2CBB-4D06-AE16-BBBBF5C35C4E},1"',
            "id": "01DABHRNU2RE777OZMAZG24FV3XP24GXCO",
            "lastModifiedDateTime": "2023-05-01T09:09:19Z",
            "name": "folder1",
            "webUrl": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents/folder1",
            "cTag": '"c:{FF3F899A-2CBB-4D06-AE16-BBBBF5C35C4E},0"',
            "size": 10484,
        },
        {
            "createdDateTime": "2023-05-01T09:09:31Z",
            "eTag": '"{2E301580-9B39-4D32-A6D4-E34680133F84},3"',
            "id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
            "lastModifiedDateTime": "2023-05-01T09:10:21Z",
            "name": "Document.docx",
            "webUrl": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=Document.docx&action=default&mobileredirect=true",
            "cTag": '"c:{2E301580-9B39-4D32-A6D4-E34680133F84},3"',
            "size": 10484,
            "file": {
                "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            },
        },
    ]
}

EXPECTED_FILES = [
    {
        "created_at": "2023-05-01T09:09:19Z",
        "_id": "01DABHRNU2RE777OZMAZG24FV3XP24GXCO",
        "_timestamp": "2023-05-01T09:09:19Z",
        "title": "folder1",
        "type": "folder",
        "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents/folder1",
        "size": 10484,
    },
    {
        "created_at": "2023-05-01T09:09:31Z",
        "_id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
        "_timestamp": "2023-05-01T09:10:21Z",
        "title": "Document.docx",
        "type": "file",
        "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=Document.docx&action=default&mobileredirect=true",
        "size": 10484,
    },
]

EXPECTED_USER1_FILES = [
    {
        "created_at": "2023-05-01T09:09:19Z",
        "_id": "01DABHRNU2RE777OZMAZG24FV3XP24GXCO",
        "_timestamp": "2023-05-01T09:09:19Z",
        "title": "folder3",
        "type": "folder",
        "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents/folder1",
        "size": 10484,
    },
    {
        "created_at": "2023-05-01T09:09:31Z",
        "_id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
        "_timestamp": "2023-05-01T09:10:21Z",
        "title": "doit.py",
        "type": "file",
        "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=doit.py&action=default&mobileredirect=true",
        "size": 10484,
    },
]

EXPECTED_USER2_FILES = [
    {
        "created_at": "2023-05-01T09:09:19Z",
        "_id": "01DABHRNU2RE777OZMAZG24FV3XP24GXCO",
        "_timestamp": "2023-05-01T09:09:19Z",
        "title": "folder4",
        "type": "folder",
        "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents/folder1",
        "size": 10484,
    },
    {
        "created_at": "2023-05-01T09:09:31Z",
        "_id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
        "_timestamp": "2023-05-01T09:10:21Z",
        "title": "mac.txt",
        "type": "file",
        "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=mac.txt&action=default&mobileredirect=true",
        "size": 10484,
    },
]

MOCK_ATTACHMENT = {
    "created_at": "2023-05-01T09:09:31Z",
    "id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
    "_timestamp": "2023-05-01T09:10:21Z",
    "title": "Document.docx",
    "type": "file",
    "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=Document.docx&action=default&mobileredirect=true",
    "size": 10484,
}

MOCK_ATTACHMENT_WITHOUT_EXTENSION = {
    "created_at": "2023-05-01T09:09:31Z",
    "id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
    "_timestamp": "2023-05-01T09:10:21Z",
    "title": "Document",
    "type": "file",
    "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=Document&action=default&mobileredirect=true",
    "size": 10484,
}

MOCK_ATTACHMENT_WITH_LARGE_DATA = {
    "created_at": "2023-05-01T09:09:31Z",
    "id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
    "_timestamp": "2023-05-01T09:10:21Z",
    "title": "Document.docx",
    "type": "file",
    "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=Document.docx&action=default&mobileredirect=true",
    "size": 23000000,
}

MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION = {
    "created_at": "2023-05-01T09:09:31Z",
    "id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
    "_timestamp": "2023-05-01T09:10:21Z",
    "title": "Document.xyz",
    "type": "file",
    "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=Document.xyz&action=default&mobileredirect=true",
    "size": 10484,
}

RESPONSE_CONTENT = "# This is the dummy file"
EXPECTED_CONTENT = {
    "_id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
    "_timestamp": "2023-05-01T09:10:21Z",
    "_attachment": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl",
}

EXPECTED_FILES_FOLDERS = {}


def token_retrieval_errors(message, error_code):
    error = ClientResponseError(None, None)
    error.status = error_code
    error.message = message
    return error


def get_stream_reader():
    async_mock = AsyncMock()
    async_mock.__aenter__ = AsyncMock(return_value=StreamReaderAsyncMock())
    return async_mock


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


def test_get_configuration():
    config = DataSourceConfiguration(
        config=OneDriveDataSource.get_default_configuration()
    )

    assert config["client_id"] == "client#123"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extras",
    [
        (
            {
                "client_id": "",
                "client_secret": "",
                "tenant_id": "",
            }
        ),
    ],
)
async def test_validate_configuration_with_invalid_dependency_fields_raises_error(
    extras,
):
    async with create_source(OneDriveDataSource, **extras) as source:
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_close_with_client_session():
    async with create_source(OneDriveDataSource) as source:
        source.get_client.access_token = "dummy"

        await source.close()

        assert not hasattr(source.get_client.__dict__, "_get_session")


@pytest.mark.asyncio
async def test_set_access_token():
    async with create_source(OneDriveDataSource) as source:
        mock_token = {"access_token": "msgraphtoken", "expires_in": "1234555"}
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(mock_token, 200)
        )

        with patch("aiohttp.request", return_value=async_response):
            await source.get_client.token._set_access_token()

            assert source.get_client.token.access_token == "msgraphtoken"


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    async with create_source(OneDriveDataSource) as source:
        DUMMY_RESPONSE = {}
        source.get_client.get = AsyncIterator([[DUMMY_RESPONSE]])

        await source.ping()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_for_failed_connection_exception(mock_get):
    async with create_source(OneDriveDataSource) as source:
        with patch.object(
            OneDriveClient, "get", side_effect=Exception("Something went wrong")
        ):
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
async def test_get_token_raises_correct_exception_when_400():
    klass = OneDriveDataSource

    config = DataSourceConfiguration(config=klass.get_default_configuration())
    message = "Bad Request"
    token = AccessToken(configuration=config)

    with patch.object(
        AccessToken,
        "_set_access_token",
        side_effect=token_retrieval_errors(message, 400),
    ):
        with pytest.raises(TokenRetrievalError) as e:
            await token.get()

        assert e.match("Tenant ID")
        assert e.match("Client ID")


@pytest.mark.asyncio
async def test_get_token_raises_correct_exception_when_401():
    klass = OneDriveDataSource

    config = DataSourceConfiguration(config=klass.get_default_configuration())
    message = "Unauthorized"
    token = AccessToken(configuration=config)

    with patch.object(
        AccessToken,
        "_set_access_token",
        side_effect=token_retrieval_errors(message, 401),
    ):
        with pytest.raises(TokenRetrievalError) as e:
            await token.get()

        assert e.match("Client Secret")


@pytest.mark.asyncio
async def test_get_token_raises_correct_exception_when_any_other_status():
    klass = OneDriveDataSource

    config = DataSourceConfiguration(config=klass.get_default_configuration())
    message = "Internal server error"
    token = AccessToken(configuration=config)

    with patch.object(
        AccessToken,
        "_set_access_token",
        side_effect=token_retrieval_errors(message, 500),
    ):
        with pytest.raises(TokenRetrievalError) as e:
            await token.get()

        assert e.match(message)


@pytest.mark.asyncio
@patch("connectors.utils.apply_retry_strategy", AsyncMock())
async def test_get_with_429_status():
    initial_response = ClientResponseError(None, None)
    initial_response.status = 429
    initial_response.message = "rate-limited"
    initial_response.headers = {"Retry-After": 0.1}

    retried_response = AsyncMock()
    payload = {"value": "Test rate limit"}

    retried_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(payload))
    async with create_source(OneDriveDataSource) as source:
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch(
                "aiohttp.ClientSession.get",
                side_effect=[initial_response, retried_response],
            ):
                async for response in source.get_client.get(
                    url="http://localhost:1000/sample"
                ):
                    result = await response.json()

    assert result == payload


@pytest.mark.asyncio
@patch("connectors.utils.apply_retry_strategy", AsyncMock())
async def test_get_with_429_status_without_retry_after_header():
    initial_response = ClientResponseError(None, None)
    initial_response.status = 429
    initial_response.message = "rate-limited"

    retried_response = AsyncMock()
    payload = {"value": "Test rate limit"}

    retried_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(payload))
    with patch("connectors.sources.onedrive.DEFAULT_RETRY_SECONDS", 0.3):
        async with create_source(OneDriveDataSource) as source:
            with patch.object(AccessToken, "get", return_value="abc"):
                with patch(
                    "aiohttp.ClientSession.get",
                    side_effect=[initial_response, retried_response],
                ):
                    async for response in source.get_client.get(
                        url="http://localhost:1000/sample"
                    ):
                        result = await response.json()

        assert result == payload


@pytest.mark.asyncio
async def test_get_owned_files():
    async with create_source(OneDriveDataSource) as source:
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(RESPONSE_FILES)
        )
        response = []
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch("aiohttp.ClientSession.get", return_value=async_response):
                with patch.object(
                    OneDriveClient,
                    "get_user_id",
                    return_value=AsyncIterator([EXPECTED_USERS]),
                ) as user_id:
                    async for file in source.get_client.get_owned_files(user_id):
                        response.append(file)

        assert response == EXPECTED_FILES


@pytest.mark.asyncio
async def test_get_user_id():
    async with create_source(OneDriveDataSource) as source:
        response = []
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(RESPONSE_USERS)
        )
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch("aiohttp.ClientSession.get", return_value=async_response):
                async for user_id in source.get_client.get_user_id():
                    response.append(user_id)

            assert response == EXPECTED_USERS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attachment, user_id, expected_content",
    [
        (MOCK_ATTACHMENT, "123", EXPECTED_CONTENT),
        (MOCK_ATTACHMENT_WITHOUT_EXTENSION, "123", None),
        (MOCK_ATTACHMENT_WITH_LARGE_DATA, "235", None),
        (MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION, "444", None),
    ],
)
async def test_get_content_when_is_downloadable_is_true(
    attachment, user_id, expected_content
):
    async with create_source(OneDriveDataSource) as source:
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch("aiohttp.ClientSession.get", return_value=get_stream_reader()):
                with patch(
                    "aiohttp.StreamReader.iter_chunked",
                    return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
                ):
                    response = await source.get_content(
                        attachment=attachment,
                        user_id=user_id,
                        doit=True,
                    )
                    assert response == expected_content


@patch.object(OneDriveClient, "get_user_id", return_value=AsyncIterator(EXPECTED_USERS))
@patch.object(
    OneDriveClient,
    "get_owned_files",
    side_effect=[
        (AsyncIterator(EXPECTED_USER1_FILES)),
        (AsyncIterator(EXPECTED_USER2_FILES)),
    ],
)
@pytest.mark.asyncio
async def test_get_docs(users_patch, files_patch):
    async with create_source(OneDriveDataSource) as source:
        expected_responses = [*EXPECTED_USER1_FILES, *EXPECTED_USER2_FILES]
        source.get_content = AsyncMock(return_value=EXPECTED_CONTENT)

        documents, downloads = [], []
        async for item, content in source.get_docs():
            documents.append(item)

            if content:
                downloads.append(content)

        assert documents == expected_responses

        assert len(downloads) == 2
