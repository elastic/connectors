#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Dropbox source class methods"""
import json
from contextlib import asynccontextmanager
from unittest import mock
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

import aiohttp
import pytest
from aiohttp import StreamReader
from aiohttp.client_exceptions import ClientResponseError, ServerDisconnectedError
from freezegun import freeze_time

from connectors.filtering.validation import SyncRuleValidationResult
from connectors.protocol import Filter
from connectors.source import ConfigurableFieldValueError
from connectors.sources.dropbox import (
    DropBoxAdvancedRulesValidator,
    DropboxClient,
    DropboxDataSource,
    InvalidClientCredentialException,
    InvalidPathException,
    InvalidRefreshTokenException,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source

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

MOCK_AUTHENTICATED_ADMIN = {
    "admin_profile": {"account_id": "dbid:123", "team_member_id": "dbmid:123-456"}
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

MOCK_MEMBERS = {
    "members": [
        {
            "profile": {
                "team_member_id": "dbmid:456",
                "account_id": "dbid:456",
                "email": "dummy@gmail.com",
                "status": {".tag": "suspended"},
                "name": {
                    "display_name": "Dummy",
                },
                "joined_on": "2023-10-11T11:52:58Z",
                "groups": ["g:456"],
            }
        },
    ],
    "cursor": "abcd#1234",
    "has_more": True,
}

MOCK_LIST_FILE_FOLDER_PERMISSION = {
    "groups": [
        {
            "group": {
                "group_id": "g:e2db7665347abcd600000000001a2b3c",
                "group_name": "Test group",
            },
        }
    ],
    "invitees": [
        {
            "invitee": {".tag": "email", "email": "jessica@example.com"},
        }
    ],
    "users": [
        {
            "user": {
                "account_id": "dbid:AAH4f99T0taONIb-OurWxbNQ6ywGRopQngc",
                "display_name": "Robert Smith",
                "email": "bob@example.com",
                "team_member_id": "dbmid:abcd1234",
            }
        }
    ],
    "cursor": "abcd#1234",
    "has_more": True,
}

MOCK_LIST_FILE_BATCHING_PERMISSION = [
    {
        "file": 1,
        "result": {
            ".tag": "result",
            "members": {
                "users": [],
                "groups": [
                    {
                        "group": {
                            "group_id": "g:2f1b9ae8edc5261b00000000000000fe",
                            "member_count": 2,
                        }
                    },
                    {
                        "group": {
                            "group_id": "g:2f1b9ae8edc5261b0000000000000003",
                            "member_count": 3,
                        }
                    },
                ],
                "invitees": [],
            },
            "member_count": 2,
        },
    }
]

MOCK_LIST_FILE_FOLDER_PERMISSION_CONTINUE = {
    "groups": [
        {
            "group": {
                "group_id": "g:123",
                "group_name": "Test group",
            },
        }
    ],
    "invitees": [
        {
            "invitee": {".tag": "email", "email": "dummy@example.com"},
        }
    ],
    "users": [
        {
            "user": {
                "account_id": "dbid:678",
                "display_name": "Robert Smith",
                "email": "dummy2@example.com",
                "team_member_id": "dbmid:abcd1234",
            }
        }
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

MOCK_MEMBERS_CONTINUE = {
    "members": [
        {
            "profile": {
                "team_member_id": "dbmid:123",
                "account_id": "dbid:123",
                "email": "dummy1@gmail.com",
                "status": {".tag": "suspended"},
                "name": {
                    "display_name": "Dummy1",
                },
                "joined_on": "2023-10-11T11:52:58Z",
                "groups": ["g:123"],
            }
        },
    ],
    "cursor": None,
    "has_more": False,
}

MOCK_TEAM_FOLDER_LIST = {
    "team_folders": [
        {
            "team_folder_id": "123",
            "name": "dummy1",
            "status": {".tag": "active"},
        }
    ],
    "cursor": "cursor:123",
    "has_more": True,
}

MOCK_TEAM_FOLDER_LIST_CONTINUE = {
    "team_folders": [
        {
            "team_folder_id": "456",
            "name": "dummy2",
            "status": {".tag": "active"},
        }
    ],
    "cursor": "cursor:123",
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


EXPECTED_FILE_PERMISSION = [
    "user_id:dbid:AAH4f99T0taONIb-OurWxbNQ6ywGRopQngc",
    "email:jessica@example.com",
    "group:g:e2db7665347abcd600000000001a2b3c",
    "user_id:456",
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

EXPECTED_DOCUMENT_TUPLE = [
    {
        "_id": "id:2",
        "type": "File",
        "name": "index.py",
        "file_path": "/test/dummy folder/index.py",
        "size": 200,
        "_timestamp": "2023-01-01T06:06:06Z",
        "_allow_access_control": [
            "user_id:dbid:AAH4f99T0taONIb-OurWxbNQ6ywGRopQngc",
            "user_id:dbid:123",
        ],
    }
]

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
EXPECTED_CONTENT_EXTRACTED = {
    "_id": "id:1",
    "_timestamp": "2023-01-01T06:06:06Z",
    "body": RESPONSE_CONTENT,
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

EXPECTED_GET_DOCS_WITH_DLS = [
    {
        "_id": "id:2",
        "type": "File",
        "name": "index.py",
        "file_path": "/test/dummy folder/index.py",
        "size": 200,
        "_timestamp": "2023-01-01T06:06:06Z",
        "_allow_access_control": [
            "user_id:dbid:AAH4f99T0taONIb-OurWxbNQ6ywGRopQngc",
            "user_id:dbid:123",
        ],
    }
]

FORMATTED_DOCUMENT = {
    "_id": "id:2",
    "type": "File",
    "name": "index.py",
    "file_path": "/test/dummy folder/index.py",
    "size": 200,
    "_timestamp": "2023-01-01T06:06:06Z",
    "_allow_access_control": [
        "user_id:dbid:AAH4f99T0taONIb-OurWxbNQ6ywGRopQngc",
        "user_id:dbid:123",
    ],
}

BATCH_RESPONSE = [
    {
        "file": "123",
        "result": {
            ".tag": "result",
            "members": {
                "users": [
                    {
                        "access_type": {".tag": "owner"},
                        "permissions": [],
                        "platform_type": {".tag": "unknown"},
                        "time_last_seen": "2016-01-20T00:00:00Z",
                        "user": {
                            "account_id": "dbid:AAH4f99T0taONIb-OurWxbNQ6ywGRopQngc",
                            "display_name": "Robert Smith",
                            "email": "bob@example.com",
                            "team_member_id": "dbmid:abcd1234",
                        },
                    }
                ],
                "groups": [],
                "invitees": [],
            },
            "member_count": 1,
        },
    }
]


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


def setup_dropbox(source):
    # Set up default config with default values
    source.configuration.get_field("app_key").value = "abc#123"
    source.configuration.get_field("app_secret").value = "abc#123"
    source.configuration.get_field("refresh_token").value = "abc#123"


@asynccontextmanager
async def create_dropbox_source(
    use_text_extraction_service=False,
    mock_access_token=True,
):
    async with create_source(
        DropboxDataSource,
        app_key="abc#123",
        app_secret="abc#123",
        refresh_token="abc#123",
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        if mock_access_token:
            source.dropbox_client._set_access_token = AsyncMock()
        yield source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    ["app_key", "app_secret", "refresh_token"],
)
async def test_validate_configuration_with_empty_fields_then_raise_exception(field):
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source.dropbox_client.configuration.get_field(field).value = ""

        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_with_valid_path():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source.dropbox_client.configuration.get_field("path").value = "/shared"

        with patch.object(
            aiohttp.ClientSession,
            "post",
            return_value=JSONAsyncMock(json=MOCK_CHECK_PATH, status=200),
        ):
            await source.validate_config()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_validate_configuration_with_invalid_path_then_raise_exception():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
                await source.validate_config()


@pytest.mark.asyncio
async def test_set_access_token():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        with patch.object(
            aiohttp.ClientSession,
            "post",
            return_value=get_json_mock(mock_response=MOCK_ACCESS_TOKEN, status=200),
        ):
            await source.dropbox_client._set_access_token()
            assert source.dropbox_client.access_token == "test2344"


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_set_access_token_with_incorrect_app_key_then_raise_exception():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)

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
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_set_access_token_with_incorrect_refresh_token_then_raise_exception():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)

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


@pytest.mark.asyncio
async def test_tweak_bulk_options():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source.concurrent_downloads = 10
        options = {"concurrent_downloads": 5}

        source.tweak_bulk_options(options)
        assert options["concurrent_downloads"] == 10


@pytest.mark.asyncio
async def test_close_with_client_session():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        _ = source.dropbox_client._get_session

        await source.close()
        assert hasattr(source.dropbox_client.__dict__, "_get_session") is False


@pytest.mark.asyncio
async def test_ping():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_api_call_when_token_is_expired():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)

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
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
async def test_get_content_when_is_downloadable_is_true_with_extraction_service():
    with patch(
        "connectors.content_extraction.ContentExtraction.extract_text",
        return_value=RESPONSE_CONTENT,
    ), patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
    ):
        async with create_dropbox_source(use_text_extraction_service=True) as source:
            with mock.patch(
                "aiohttp.ClientSession.post", return_value=get_stream_reader()
            ):
                with mock.patch(
                    "aiohttp.StreamReader.iter_chunked",
                    return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
                ):
                    response = await source.get_content(
                        attachment=MOCK_ATTACHMENT,
                        is_shared=True,
                        doit=True,
                    )
                    assert response == EXPECTED_CONTENT_EXTRACTED


@pytest.mark.asyncio
@freeze_time("2023-01-01T06:06:06")
async def test_fetch_files_folders():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
            async for document, _ in source._fetch_files_folders("/", folder_id="123"):
                actual_response.append(document)
        assert actual_response == EXPECTED_FILES_FOLDERS


@pytest.mark.asyncio
@freeze_time("2023-01-01T06:06:06")
async def test_fetch_shared_files():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
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


@pytest.mark.asyncio
async def test_get_team_folder_id():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source.dropbox_client.path = "/"

        actual_response = []
        with patch.object(
            source.dropbox_client,
            "api_call",
            side_effect=[
                AsyncIterator([JSONAsyncMock(MOCK_TEAM_FOLDER_LIST, status=200)]),
                AsyncIterator(
                    [JSONAsyncMock(MOCK_TEAM_FOLDER_LIST_CONTINUE, status=200)]
                ),
            ],
        ):
            async for document in source.get_team_folder_id():
                actual_response.append(document)
        assert actual_response == ["123", "456"]


@pytest.mark.asyncio
async def test_get_access_control():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source._dls_enabled = MagicMock(return_value=True)
        source.dropbox_client.path = "/"
        with patch.object(
            source.dropbox_client,
            "api_call",
            side_effect=[
                AsyncIterator([JSONAsyncMock(MOCK_MEMBERS, status=200)]),
                AsyncIterator([JSONAsyncMock(MOCK_MEMBERS_CONTINUE, status=200)]),
            ],
        ):
            async for document in source.get_access_control():
                assert document["_id"] in ["dummy@gmail.com", "dummy1@gmail.com"]


@pytest.mark.asyncio
async def test_ping_dls_enabled():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source._dls_enabled = MagicMock(return_value=True)
        source.dropbox_client._set_access_token = AsyncMock()
        with patch.object(
            aiohttp.ClientSession,
            "post",
            return_value=get_json_mock(MOCK_AUTHENTICATED_ADMIN, 200),
        ):
            await source.ping()


@pytest.mark.asyncio
async def test_get_permission_list_for_file():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source._dls_enabled = MagicMock(return_value=True)
        source.dropbox_client._set_access_token = AsyncMock()
        actual_response = []
        with patch.object(
            source.dropbox_client,
            "api_call",
            side_effect=[
                AsyncIterator(
                    [JSONAsyncMock(MOCK_LIST_FILE_FOLDER_PERMISSION, status=200)]
                ),
                AsyncIterator(
                    [
                        JSONAsyncMock(
                            MOCK_LIST_FILE_FOLDER_PERMISSION_CONTINUE, status=200
                        )
                    ]
                ),
            ],
        ):
            actual_response = await source.get_permission_list(
                item_type="file", item={"id": 123}, account_id=456
            )
        assert actual_response == EXPECTED_FILE_PERMISSION


@pytest.mark.asyncio
async def test_get_permission_list_for_folder():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source._dls_enabled = MagicMock(return_value=True)
        source.dropbox_client._set_access_token = AsyncMock()
        actual_response = []
        with patch.object(
            source.dropbox_client,
            "api_call",
            side_effect=[
                AsyncIterator(
                    [JSONAsyncMock(MOCK_LIST_FILE_FOLDER_PERMISSION, status=200)]
                ),
                AsyncIterator(
                    [
                        JSONAsyncMock(
                            MOCK_LIST_FILE_FOLDER_PERMISSION_CONTINUE, status=200
                        )
                    ]
                ),
            ],
        ):
            actual_response = await source.get_permission_list(
                item_type="Folder", item={"shared_folder_id": 123}, account_id=456
            )
        assert actual_response == EXPECTED_FILE_PERMISSION


@pytest.mark.asyncio
async def test_get_account_details():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source.dropbox_client._set_access_token = AsyncMock()
        actual_response = []
        with patch.object(
            aiohttp.ClientSession,
            "post",
            return_value=get_json_mock(MOCK_AUTHENTICATED_ADMIN, 200),
        ):
            actual_response = await source.get_account_details()
            assert actual_response == ("dbid:123", "dbmid:123-456")


async def create_fake_coroutine(data):
    return data


@pytest.mark.asyncio
async def test_get_docs_for_dls():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source._dls_enabled = MagicMock(return_value=True)
        source.add_document_to_list = Mock(
            return_value=AsyncIterator([(FORMATTED_DOCUMENT, None)])
        )
        source.get_account_details = Mock(
            return_value=create_fake_coroutine(["dbid:123", "dbmid:123-456"])
        )
        source.get_team_folder_id = Mock(return_value=AsyncIterator(["dbid:123"]))
        documents = []
        async for item, _ in source.get_docs():
            documents.append(item)
        assert documents == EXPECTED_GET_DOCS_WITH_DLS


@pytest.mark.asyncio
async def test_remote_validation_with_dls():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source._dls_enabled = MagicMock(return_value=True)
        source.dropbox_client.path = "/abc"
        source.get_account_details = Mock(
            return_value=create_fake_coroutine(["dbid:123", "dbmid:123-456"])
        )
        source.get_team_folder_id = Mock(return_value=AsyncIterator(["dbid:123"]))
        source.dropbox_client.check_path = JSONAsyncMock(
            json=MOCK_CHECK_PATH, status=200
        )
        await source._remote_validation()


@pytest.mark.asyncio
async def test_add_document_to_list():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source.include_inherited_users_and_groups = True
        source._fetch_files_folders = Mock(
            return_value=AsyncIterator(
                [
                    (
                        FORMATTED_DOCUMENT,
                        {"size": 1000000000000, "id": 1, "name": "abc.png"},
                    )
                ]
            )
        )
        source.get_permission_list = Mock(
            return_value=create_fake_coroutine(["dbid:123", "dbmid:123-456"])
        )
        documents = []
        async for item, _ in source.add_document_to_list(
            func=source._fetch_files_folders, account_id=1, folder_id=2
        ):
            documents.append(item)
        assert documents == EXPECTED_DOCUMENT_TUPLE


@pytest.mark.asyncio
async def test_add_document_to_list_with_exclude_inherited_users_and_groups():
    async with create_source(DropboxDataSource) as source:
        setup_dropbox(source)
        source._fetch_files_folders = Mock(
            return_value=AsyncIterator(
                [
                    (
                        FORMATTED_DOCUMENT,
                        {"size": 1000000000000, "id": 1, "name": "abc.png"},
                    )
                ]
            )
        )
        source.dropbox_client.list_file_permission_with_batching = Mock(
            return_value=AsyncIterator([MOCK_LIST_FILE_BATCHING_PERMISSION])
        )
        documents = []
        async for item, _ in source.add_document_to_list(
            func=source._fetch_files_folders, account_id=1, folder_id=2
        ):
            documents.append(item)
        assert documents == EXPECTED_DOCUMENT_TUPLE
