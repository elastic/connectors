#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Dropbox source class methods"""
import json
from unittest import mock
from unittest.mock import ANY, AsyncMock, Mock, patch

import aiohttp
import pytest
from aiohttp import StreamReader
from aiohttp.client_exceptions import ClientResponseError, ServerDisconnectedError
from freezegun import freeze_time

from connectors.filtering.validation import SyncRuleValidationResult
from connectors.protocol import Filter
from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.dropbox import (
    DropBoxAdvancedRulesValidator,
    DropboxClient,
    DropboxDataSource,
    InvalidClientCredentialException,
    InvalidPathException,
    InvalidRefreshTokenException,
)
from tests.commons import AsyncIterator
from tests.sources.support import AsyncSourceContextManager, create_source

PATH = "/"
DUMMY_VALUES = "abc#123"
ADVANCED_SNIPPET = "advanced_snippet"

HOST_URLS = {
    "ACCESS_TOKEN_HOST_URL": "https://api.dropboxapi.com/",
    "FILES_FOLDERS_HOST_URL": "https://api.dropboxapi.com/2/",
    "DOWNLOAD_HOST_URL": "https://content.dropboxapi.com/2/",
}
PING = "users/get_current_account"

MOCK_CURRENT_USER = {
    "account_id": "acc_id:1234",
    "name": {
        "given_name": "John",
        "surname": "Wilber",
        "display_name": "John Wilber",
        "abbreviated_name": "JW",
    },
    "email": "john.wilber@abcd.com",
    "country": "US",
}

MOCK_CHECK_PATH = {
    ".tag": "folder",
    "name": "shared",
    "path_lower": "/shared",
    "path_display": "/shared",
    "id": "id:abcd",
    "shared_folder_id": "1234",
}

MOCK_ACCESS_TOKEN = {"access_token": "test2344", "expires_in": "1234555"}
MOCK_ACCESS_TOKEN_FOR_INVALID_REFRESH_TOKEN = {"error": "invalid_grant"}
MOCK_ACCESS_TOKEN_FOR_INVALID_APP_KEY = {
    "error": "invalid_client: Invalid client_id or client_secret"
}

MOCK_FILES_FOLDERS = {
    "entries": [
        {
            ".tag": "folder",
            "name": "dummy folder",
            "path_lower": "/test/dummy folder",
            "path_display": "/test/dummy folder",
            "id": "id:1",
        },
    ],
    "cursor": "abcd#1234",
    "has_more": True,
}

MOCK_FILES_FOLDERS_CONTINUE = {
    "entries": [
        {
            ".tag": "file",
            "name": "index.py",
            "path_lower": "/test/dummy folder/index.py",
            "path_display": "/test/dummy folder/index.py",
            "id": "id:2",
            "client_modified": "2023-01-01T06:06:06Z",
            "server_modified": "2023-01-01T06:06:06Z",
            "size": 200,
            "is_downloadable": True,
        },
    ],
    "cursor": None,
    "has_more": False,
}

EXPECTED_FILES_FOLDERS = [
    {
        "_id": "id:1",
        "type": "Folder",
        "name": "dummy folder",
        "file_path": "/test/dummy folder",
        "size": 0,
        "_timestamp": "2023-01-01T06:06:06+00:00",
    },
    {
        "_id": "id:2",
        "type": "File",
        "name": "index.py",
        "file_path": "/test/dummy folder/index.py",
        "size": 200,
        "_timestamp": "2023-01-01T06:06:06Z",
    },
]

MOCK_SHARED_FILES = {
    "entries": [
        {
            "access_type": {".tag": "viewer"},
            "name": "index1.py",
            "id": "id:1",
            "time_invited": "2023-01-01T06:06:06Z",
            "preview_url": "https://www.dropbox.com/scl/fi/a1xtoxyu0ux73pd7e77ul/index1.py?dl=0",
        },
    ],
    "cursor": "abcd#1234",
}

MOCK_SHARED_FILES_CONTINUE = {
    "entries": [
        {
            "access_type": {".tag": "viewer"},
            "name": "index2.py",
            "id": "id:2",
            "time_invited": "2023-01-01T06:06:06Z",
            "preview_url": "https://www.dropbox.com/scl/fi/a1xtoxyu0ux73pd7e77ul/index2.py?dl=0",
        },
    ],
    "cursor": None,
}

MOCK_RECEIVED_FILE_METADATA_1 = {
    "name": "index1.py",
    "id": "id:1",
    "client_modified": "2023-01-01T06:06:06Z",
    "server_modified": "2023-01-01T06:06:06Z",
    "size": 200,
    "preview_type": "text",
    "url": "https://www.dropbox.com/scl/fi/a1xtoxyu0ux73pd7e77ul/index1.py?dl=0",
}

MOCK_RECEIVED_FILE_METADATA_2 = {
    "name": "index2.py",
    "id": "id:2",
    "client_modified": "2023-01-01T06:06:06Z",
    "server_modified": "2023-01-01T06:06:06Z",
    "size": 200,
    "preview_type": "text",
    "url": "https://www.dropbox.com/scl/fi/a1xtoxyu0ux73pd7e77ul/index2.py?dl=0",
}

EXPECTED_SHARED_FILES = [
    {
        "_id": "id:1",
        "type": "File",
        "name": "index1.py",
        "url": "https://www.dropbox.com/scl/fi/a1xtoxyu0ux73pd7e77ul/index1.py?dl=0",
        "size": 200,
        "_timestamp": "2023-01-01T06:06:06Z",
    },
    {
        "_id": "id:2",
        "type": "File",
        "name": "index2.py",
        "url": "https://www.dropbox.com/scl/fi/a1xtoxyu0ux73pd7e77ul/index2.py?dl=0",
        "size": 200,
        "_timestamp": "2023-01-01T06:06:06Z",
    },
]

MOCK_ATTACHMENT = {
    "id": "id:1",
    "name": "dummy_file.txt",
    "server_modified": "2023-01-01T06:06:06Z",
    "size": 200,
    "url": "https://www.dropbox.com/scl/fi/a1xtoxyu0ux73pd7e77ul/dummy_file.txt?dl=0",
    "is_downloadable": True,
    "path_display": "/test/dummy_file.txt",
}

MOCK_PAPER_FILE = {
    "id": "id:1",
    "name": "dummy_file.paper",
    "server_modified": "2023-01-01T06:06:06Z",
    "size": 200,
    "url": "https://www.dropbox.com/scl/fi/a1xtoxyu0ux73pd7e77ul/dummy_file.paper?dl=0",
    "is_downloadable": False,
    "path_display": "/test/dummy_file.paper",
}

SKIPPED_ATTACHMENT = {
    "id": "id:1",
    "name": "dummy_file.txt",
    "server_modified": "2023-01-01T06:06:06Z",
    "size": 200,
    "url": "https://www.dropbox.com/scl/fi/a1xtoxyu0ux73pd7e77ul/dummy_file.txt?dl=0",
    "is_downloadable": False,
    "path_display": "/test/dummy_file.txt",
}

MOCK_ATTACHMENT_WITHOUT_EXTENSION = {
    "id": "id:1",
    "name": "dummy_file",
    "server_modified": "2023-01-01T06:06:06Z",
    "size": 200,
    "url": "https://www.dropbox.com/scl/fi/a1xtoxyu0ux73pd7e77ul/dummy_file?dl=0",
    "is_downloadable": False,
    "path_display": "/test/dummy_file",
}

MOCK_ATTACHMENT_WITH_LARGE_DATA = {
    "id": "id:1",
    "name": "dummy_file.txt",
    "server_modified": "2023-01-01T06:06:06Z",
    "size": 23000000,
    "url": "https://www.dropbox.com/scl/fi/a1xtoxyu0ux73pd7e77ul/dummy_file.txt?dl=0",
    "is_downloadable": True,
    "path_display": "/test/dummy_file.txt",
}

MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION = {
    "id": "id:1",
    "name": "dummy_file.xyz",
    "server_modified": "2023-01-01T06:06:06Z",
    "size": 23000000,
    "url": "https://www.dropbox.com/scl/fi/a1xtoxyu0ux73pd7e77ul/dummy_file.xyz?dl=0",
    "is_downloadable": True,
    "path_display": "/test/dummy_file.xyz",
}

RESPONSE_CONTENT = "# This is the dummy file"
EXPECTED_CONTENT = {
    "_id": "id:1",
    "_timestamp": "2023-01-01T06:06:06Z",
    "_attachment": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl",
}

MOCK_SEARCH_FILE_1 = {
    "has_more": False,
    "matches": [
        {
            "match_type": {".tag": "filename_and_content"},
            "metadata": {
                ".tag": "metadata",
                "metadata": {
                    ".tag": "file",
                    "client_modified": "2023-01-01T06:06:06Z",
                    "content_hash": "abc123",
                    "id": "id:bJ86SIuuyXkAAAAAAAAAEQ",
                    "is_downloadable": True,
                    "name": "500_Copy.py",
                    "path_display": "/500_files/500_Copy.py",
                    "path_lower": "/500_files/500_Copy.py",
                    "rev": "015fbe2ba5a15440000000214a950e0",
                    "server_modified": "2023-01-01T06:06:06Z",
                    "size": 512000,
                },
            },
        }
    ],
}

MOCK_SEARCH_FILE_2 = {
    "has_more": False,
    "matches": [
        {
            "match_type": {".tag": "filename_and_content"},
            "metadata": {
                ".tag": "metadata",
                "metadata": {
                    ".tag": "file",
                    "client_modified": "2023-01-01T06:06:06Z",
                    "content_hash": "abc321",
                    "id": "id:bJ86SIuuyXkAAAAAAAAAEQ",
                    "is_downloadable": True,
                    "name": "dummy_file.txt",
                    "preview_url": "https://www.dropbox.com/scl/fi/xyz456/dummy_file.txt?dl=0",
                    "rev": "015fbe2ba5a15440000000214a950e0",
                    "server_modified": "2023-01-01T06:06:06Z",
                    "size": 200,
                },
            },
        }
    ],
}

MOCK_SEARCH_FILE_3 = {
    ".tag": "file",
    "client_modified": "2023-01-01T06:06:06Z",
    "content_hash": "pqr123",
    "id": "id:bJ86SIuuyXkAAAAAAAAAEQ",
    "is_downloadable": True,
    "name": "dummy_file.txt",
    "url": "https://www.dropbox.com/scl/fi/pqr123/dummy_file.txt?dl=0",
    "rev": "015fbe2ba5a15440000000214a950e0",
    "server_modified": "2023-01-01T06:06:06Z",
    "size": 200,
}


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


@pytest.mark.asyncio
async def test_configuration():
    """Tests the get configurations method of the Dropbox source class."""
    config = DataSourceConfiguration(
        config=DropboxDataSource.get_default_configuration()
    )
    assert config["path"] == PATH
    assert config["app_key"] == DUMMY_VALUES
    assert config["app_secret"] == DUMMY_VALUES
    assert config["refresh_token"] == DUMMY_VALUES


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    ["path", "app_key", "app_secret", "refresh_token"],
)
async def test_validate_configuration_with_empty_fields_then_raise_exception(field):
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.configuration.set_field(name=field, value="")

        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_with_valid_path():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.configuration.set_field(name="path", value="/shared")

        with patch.object(
            aiohttp.ClientSession,
            "post",
            return_value=JSONAsyncMock(json=MOCK_CHECK_PATH, status=200),
        ):
            await source.validate_config()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_validate_configuration_with_invalid_path_then_raise_exception(
    mock_apply_retry_strategy,
):
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()
        source.dropbox_client.path = "/abc"

        with patch.object(
            aiohttp.ClientSession,
            "post",
            side_effect=ClientResponseError(
                status=409,
                request_info=aiohttp.RequestInfo(
                    real_url="", method=None, headers=None, url=""
                ),
                history=None,
            ),
        ):
            with pytest.raises(
                InvalidPathException, match="Configured Path: /abc is invalid"
            ):
                with pytest.raises(
                    ConfigurableFieldValueError,
                    match="Configured Path: /abc is invalid",
                ):
                    await source.validate_config()


@pytest.mark.asyncio
async def test_set_access_token():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        with patch.object(
            aiohttp.ClientSession,
            "post",
            return_value=get_json_mock(mock_response=MOCK_ACCESS_TOKEN, status=200),
        ):
            await source.dropbox_client._set_access_token()
            assert source.dropbox_client.access_token == "test2344"


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_set_access_token_with_incorrect_app_key_then_raise_exception(
    mock_apply_retry_strategy,
):
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()

        with patch.object(
            aiohttp.ClientSession,
            "post",
            return_value=get_json_mock(
                mock_response=MOCK_ACCESS_TOKEN_FOR_INVALID_APP_KEY, status=400
            ),
        ):
            with pytest.raises(
                InvalidClientCredentialException,
                match="Configured App Key or App Secret is invalid.",
            ):
                await source.dropbox_client._set_access_token()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_set_access_token_with_incorrect_refresh_token_then_raise_exception(
    mock_apply_retry_strategy,
):
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()

        with patch.object(
            aiohttp.ClientSession,
            "post",
            return_value=get_json_mock(
                mock_response=MOCK_ACCESS_TOKEN_FOR_INVALID_REFRESH_TOKEN, status=400
            ),
        ):
            with pytest.raises(
                InvalidRefreshTokenException,
                match="Configured Refresh Token is invalid.",
            ):
                await source.dropbox_client._set_access_token()


def test_tweak_bulk_options():
    source = create_source(DropboxDataSource)

    source.concurrent_downloads = 10
    options = {"concurrent_downloads": 5}

    source.tweak_bulk_options(options)
    assert options["concurrent_downloads"] == 10


@pytest.mark.asyncio
async def test_close_with_client_session():
    source = create_source(DropboxDataSource)
    _ = source.dropbox_client._get_session

    await source.close()
    assert hasattr(source.dropbox_client.__dict__, "_get_session") is False


@pytest.mark.asyncio
async def test_ping():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client._set_access_token = AsyncMock()
        with patch.object(
            aiohttp.ClientSession,
            "post",
            return_value=get_json_mock(MOCK_CURRENT_USER, 200),
        ):
            await source.ping()


@pytest.mark.asyncio
@patch("connectors.sources.dropbox.RETRY_INTERVAL", 0)
async def test_api_call_negative():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.retry_count = 4
        source.dropbox_client._set_access_token = AsyncMock()

        with patch.object(
            aiohttp.ClientSession, "post", side_effect=Exception("Something went wrong")
        ):
            with pytest.raises(Exception):
                await anext(
                    source.dropbox_client.api_call(
                        base_url=HOST_URLS["FILES_FOLDERS_HOST_URL"],
                        url_name=PING,
                        data=json.dumps(None),
                    )
                )

        with patch.object(
            aiohttp.ClientSession, "post", side_effect=ServerDisconnectedError()
        ):
            with pytest.raises(Exception):
                await anext(
                    source.dropbox_client.api_call(
                        base_url=HOST_URLS["FILES_FOLDERS_HOST_URL"],
                        url_name=PING,
                        data=json.dumps(None),
                    )
                )


@pytest.mark.asyncio
async def test_api_call():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client._set_access_token = AsyncMock()

        with patch.object(
            aiohttp.ClientSession,
            "post",
            return_value=get_json_mock(MOCK_CURRENT_USER, 200),
        ):
            EXPECTED_RESPONSE = {
                "account_id": "acc_id:1234",
                "name": {
                    "given_name": "John",
                    "surname": "Wilber",
                    "display_name": "John Wilber",
                    "abbreviated_name": "JW",
                },
                "email": "john.wilber@abcd.com",
                "country": "US",
            }
            response = await anext(
                source.dropbox_client.api_call(
                    base_url=HOST_URLS["FILES_FOLDERS_HOST_URL"],
                    url_name=PING,
                    data=json.dumps(None),
                )
            )
            actual_response = await response.json()
            assert actual_response == EXPECTED_RESPONSE


@pytest.mark.asyncio
async def test_paginated_api_call_when_skipping_api_call():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.retry_count = 1
        source.dropbox_client._set_access_token = AsyncMock()

        with patch.object(
            source.dropbox_client,
            "api_call",
            side_effect=Exception("Something went wrong"),
        ):
            async for response in source.dropbox_client._paginated_api_call(
                base_url=HOST_URLS["FILES_FOLDERS_HOST_URL"],
                breaking_field="xyz",
                continue_endpoint="shared_file",
                data={"data": "xyz"},
                url_name="url_name",
            ):
                assert response is None


@pytest.mark.asyncio
async def test_set_access_token_when_token_expires_at_is_str():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.token_expiration_time = "2023-02-10T09:02:23.629821"
        mock_token = {"access_token": "test2344", "expires_in": "1234555"}
        async_response_token = get_json_mock(mock_token, 200)

        with patch.object(
            aiohttp.ClientSession, "post", return_value=async_response_token
        ):
            actual_response = await source.dropbox_client._set_access_token()
            assert actual_response is None


@pytest.fixture
def patch_default_wait_multiplier():
    with mock.patch("connectors.sources.dropbox.RETRY_INTERVAL", 0):
        yield


@pytest.mark.asyncio
@mock.patch("connectors.sources.dropbox.RETRY_INTERVAL", 0)
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_api_call_when_token_is_expired(mock_apply_retry_strategy):
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()

        with patch.object(
            aiohttp.ClientSession,
            "post",
            side_effect=[
                ClientResponseError(
                    status=401,
                    request_info=aiohttp.RequestInfo(
                        real_url="", method=None, headers=None, url=""
                    ),
                    history=None,
                    message="Unauthorized",
                ),
                get_json_mock(MOCK_ACCESS_TOKEN, 200),
                get_json_mock(MOCK_FILES_FOLDERS, 200),
            ],
        ):
            actual_response = await anext(
                source.dropbox_client.api_call(
                    base_url=HOST_URLS["FILES_FOLDERS_HOST_URL"],
                    url_name=PING,
                    data=json.dumps(None),
                )
            )
            actual_response = await actual_response.json()
            assert actual_response == MOCK_FILES_FOLDERS


@pytest.mark.asyncio
async def test_api_call_when_status_429_exception():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client._set_access_token = AsyncMock()

        with patch.object(
            aiohttp.ClientSession,
            "post",
            side_effect=[
                ClientResponseError(
                    status=429,
                    headers={"Retry-After": 0},
                    request_info=aiohttp.RequestInfo(
                        real_url="", method=None, headers=None, url=""
                    ),
                    history=(),
                ),
                get_json_mock(MOCK_FILES_FOLDERS, 200),
            ],
        ):
            _ = source.dropbox_client._get_session
            response = await anext(
                source.dropbox_client.api_call(
                    base_url=HOST_URLS["FILES_FOLDERS_HOST_URL"],
                    url_name=PING,
                    data=json.dumps(None),
                )
            )
            actual_response = await response.json()
            assert actual_response == MOCK_FILES_FOLDERS


@pytest.mark.asyncio
@patch("connectors.sources.dropbox.DEFAULT_RETRY_AFTER", 0)
async def test_api_call_when_status_429_exception_without_retry_after_header():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.retry_count = 1

        source.dropbox_client._set_access_token = AsyncMock()

        with patch.object(
            aiohttp.ClientSession,
            "post",
            side_effect=ClientResponseError(
                status=429,
                headers={},
                request_info=aiohttp.RequestInfo(
                    real_url="", method=None, headers=None, url=""
                ),
                history=(),
            ),
        ):
            _ = source.dropbox_client._get_session
            with pytest.raises(ClientResponseError):
                await anext(
                    source.dropbox_client.api_call(
                        base_url=HOST_URLS["FILES_FOLDERS_HOST_URL"],
                        url_name=PING,
                        data=json.dumps(None),
                    )
                )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attachment, is_shared, expected_content",
    [
        (MOCK_ATTACHMENT, False, EXPECTED_CONTENT),
        (MOCK_PAPER_FILE, False, EXPECTED_CONTENT),
        (MOCK_ATTACHMENT, True, EXPECTED_CONTENT),
        (MOCK_ATTACHMENT_WITHOUT_EXTENSION, False, None),
        (MOCK_ATTACHMENT_WITH_LARGE_DATA, False, None),
        (MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION, False, None),
        (SKIPPED_ATTACHMENT, False, None),
    ],
)
async def test_get_content_when_is_downloadable_is_true(
    attachment, is_shared, expected_content
):
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client._set_access_token = AsyncMock()

        with mock.patch("aiohttp.ClientSession.post", return_value=get_stream_reader()):
            with mock.patch(
                "aiohttp.StreamReader.iter_chunked",
                return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
            ):
                response = await source.get_content(
                    attachment=attachment,
                    is_shared=is_shared,
                    doit=True,
                )
                assert response == expected_content


@pytest.mark.asyncio
@freeze_time("2023-01-01T06:06:06")
async def test_fetch_files_folders():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.path = "/"

        actual_response = []
        with patch.object(
            source.dropbox_client,
            "api_call",
            side_effect=[
                AsyncIterator([JSONAsyncMock(MOCK_FILES_FOLDERS, status=200)]),
                AsyncIterator([JSONAsyncMock(MOCK_FILES_FOLDERS_CONTINUE, status=200)]),
            ],
        ):
            async for document, _ in source._fetch_files_folders("/"):
                actual_response.append(document)

        assert actual_response == EXPECTED_FILES_FOLDERS


@pytest.mark.asyncio
@freeze_time("2023-01-01T06:06:06")
async def test_fetch_shared_files():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.path = "/"

        actual_response = []
        with patch.object(
            source.dropbox_client,
            "api_call",
            side_effect=[
                AsyncIterator([JSONAsyncMock(MOCK_SHARED_FILES, status=200)]),
                AsyncIterator(
                    [JSONAsyncMock(MOCK_RECEIVED_FILE_METADATA_1, status=200)]
                ),
                AsyncIterator([JSONAsyncMock(MOCK_SHARED_FILES_CONTINUE, status=200)]),
                AsyncIterator(
                    [JSONAsyncMock(MOCK_RECEIVED_FILE_METADATA_2, status=200)]
                ),
            ],
        ):
            async for document, _ in source._fetch_shared_files():
                actual_response.append(document)

        assert actual_response == EXPECTED_SHARED_FILES


@pytest.mark.asyncio
@freeze_time("2023-01-01T06:06:06")
async def test_search_files():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        rule = {
            "query": "copy",
            "options": {
                "path": "/500_files",
                "file_status": "active",
            },
        }

        actual_response = []
        with patch.object(
            source.dropbox_client,
            "api_call",
            side_effect=[
                AsyncIterator([JSONAsyncMock(MOCK_SEARCH_FILE_1, status=200)]),
                AsyncIterator([JSONAsyncMock(MOCK_SEARCH_FILE_2, status=200)]),
            ],
        ):
            async for document, _ in source.advanced_sync(rule=rule):
                actual_response.append(document)

        assert actual_response == [
            {
                "_id": "id:bJ86SIuuyXkAAAAAAAAAEQ",
                "type": "File",
                "name": "500_Copy.py",
                "file_path": "/500_files/500_Copy.py",
                "size": 512000,
                "_timestamp": "2023-01-01T06:06:06Z",
            }
        ]


@pytest.mark.asyncio
@freeze_time("2023-01-01T06:06:06")
@patch.object(
    DropboxDataSource,
    "_fetch_files_folders",
    side_effect=AsyncIterator(
        [
            (EXPECTED_FILES_FOLDERS[0], "files-folders"),
            (EXPECTED_FILES_FOLDERS[1], "files-folders"),
        ],
    ),
)
@patch.object(
    DropboxDataSource,
    "_fetch_shared_files",
    return_value=AsyncIterator(
        [
            (EXPECTED_SHARED_FILES[0], "shared_files"),
            (EXPECTED_SHARED_FILES[1], "shared_files"),
        ],
    ),
)
async def test_get_docs(files_folders_patch, shared_files_patch):
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        expected_responses = [*EXPECTED_FILES_FOLDERS, *EXPECTED_SHARED_FILES]
        source.get_content = Mock(return_value=EXPECTED_CONTENT)

        documents = []
        async for item, _ in source.get_docs():
            documents.append(item)

        assert documents == expected_responses


@pytest.mark.parametrize(
    "advanced_rules, expected_validation_result",
    [
        (
            [
                {
                    "query": "copy",
                    "options": {
                        "path": "/invalid_path",
                        "file_status": {".tag": "active"},
                    },
                }
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        )
    ],
)
@pytest.mark.asyncio
async def test_advanced_rules_validation_with_invalid_repos(
    advanced_rules, expected_validation_result
):
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.check_path = AsyncMock(side_effect=InvalidPathException())

        validation_result = await DropBoxAdvancedRulesValidator(source).validate(
            advanced_rules
        )

        assert validation_result == expected_validation_result


@pytest.mark.parametrize(
    "filtering",
    [
        Filter(
            {
                ADVANCED_SNIPPET: {
                    "value": [
                        {
                            "query": "copy",
                            "options": {
                                "path": "/500_files",
                                "file_status": {
                                    ".tag": "active",
                                },
                            },
                        },
                        {
                            "query": "dummy",
                            "options": {
                                "file_extensions": ["txt"],
                            },
                        },
                        {
                            "query": "manager",
                            "options": {
                                "file_categories": [{".tag": "paper"}, {".tag": "pdf"}],
                            },
                        },
                    ]
                }
            }
        ),
    ],
)
@patch.object(
    DropboxClient,
    "search_files_folders",
    side_effect=AsyncIterator(
        [
            MOCK_SEARCH_FILE_1,
            MOCK_SEARCH_FILE_2,
        ],
    ),
)
@patch.object(
    DropboxClient,
    "api_call",
    side_effect=AsyncIterator(
        [JSONAsyncMock(MOCK_SEARCH_FILE_3, 200)],
    ),
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules(
    received_files_patch, files_folders_patch, filtering
):
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.get_content = Mock(return_value=EXPECTED_CONTENT)

        documents = []
        async for item, _ in source.get_docs(filtering):
            documents.append(item)

        assert documents == [
            {
                "_id": "id:bJ86SIuuyXkAAAAAAAAAEQ",
                "type": "File",
                "name": "500_Copy.py",
                "file_path": "/500_files/500_Copy.py",
                "size": 512000,
                "_timestamp": "2023-01-01T06:06:06Z",
            },
            {
                "_id": "id:bJ86SIuuyXkAAAAAAAAAEQ",
                "type": "File",
                "name": "dummy_file.txt",
                "url": "https://www.dropbox.com/scl/fi/pqr123/dummy_file.txt?dl=0",
                "size": 200,
                "_timestamp": "2023-01-01T06:06:06Z",
            },
        ]
