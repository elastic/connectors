#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the OneDrive source class methods"""
from contextlib import asynccontextmanager
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

import pytest
from aiohttp import StreamReader
from aiohttp.client_exceptions import ClientPayloadError, ClientResponseError

from connectors.filtering.validation import SyncRuleValidationResult
from connectors.protocol import Filter
from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.onedrive import (
    AccessToken,
    InternalServerError,
    NotFound,
    OneDriveAdvancedRulesValidator,
    OneDriveClient,
    OneDriveDataSource,
    TokenRetrievalError,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source

ADVANCED_SNIPPET = "advanced_snippet"
EXPECTED_USERS = [
    {
        "mail": "AdeleV@w076v.onmicrosoft.com",
        "id": "3fada5d6-125c-4aaa-bc23-09b5d301b2a7",
        "createdDateTime": "2022-06-11T14:47:53Z",
        "userPrincipalName": "AdeleV@w076v.onmicrosoft.com",
        "transitiveMemberOf": [
            {
                "@odata.type": "#microsoft.graph.group",
                "id": "e35d6159-f6c1-462c-a60c-11f29880e9c0",
            }
        ],
    },
    {
        "mail": "AlexW@w076v.onmicrosoft.com",
        "id": "8e083933-1720-4d2f-80d0-3976669b40ee",
        "createdDateTime": "2022-06-11T14:47:53Z",
        "userPrincipalName": "AlexW@w076v.onmicrosoft.com",
        "transitiveMemberOf": [
            {
                "@odata.type": "#microsoft.graph.group",
                "id": "5010ad09-7ad0-48e7-8047-1ae0f6117a17",
            }
        ],
    },
]
RESPONSE_USERS = {
    "value": [
        {
            "mail": "AdeleV@w076v.onmicrosoft.com",
            "id": "3fada5d6-125c-4aaa-bc23-09b5d301b2a7",
            "createdDateTime": "2022-06-11T14:47:53Z",
            "userPrincipalName": "AdeleV@w076v.onmicrosoft.com",
            "transitiveMemberOf": [
                {
                    "@odata.type": "#microsoft.graph.group",
                    "id": "e35d6159-f6c1-462c-a60c-11f29880e9c0",
                }
            ],
        },
        {
            "mail": "AlexW@w076v.onmicrosoft.com",
            "id": "8e083933-1720-4d2f-80d0-3976669b40ee",
            "createdDateTime": "2022-06-11T14:47:53Z",
            "userPrincipalName": "AlexW@w076v.onmicrosoft.com",
            "transitiveMemberOf": [
                {
                    "@odata.type": "#microsoft.graph.group",
                    "id": "5010ad09-7ad0-48e7-8047-1ae0f6117a17",
                }
            ],
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
            "parentReference": {"path": "root"},
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
            "parentReference": {"path": "root"},
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
            "parentReference": {"path": "root"},
            "file": {
                "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            },
        },
    ]
}

EXPECTED_FILES = [
    {
        "createdDateTime": "2023-05-01T09:09:19Z",
        "eTag": '"{FF3F899A-2CBB-4D06-AE16-BBBBF5C35C4E},1"',
        "id": "01DABHRNU2RE777OZMAZG24FV3XP24GXCO",
        "lastModifiedDateTime": "2023-05-01T09:09:19Z",
        "name": "folder1",
        "webUrl": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents/folder1",
        "cTag": '"c:{FF3F899A-2CBB-4D06-AE16-BBBBF5C35C4E},0"',
        "parentReference": {"path": "root"},
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
        "parentReference": {"path": "root"},
        "file": {
            "mimeType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        },
    },
]

RESPONSE_USER1_FILES = [
    {
        "@microsoft.graph.downloadUrl": "https://example-download-url-item-3",
        "createdDateTime": "2023-05-01T09:09:19Z",
        "id": "01DABHRNU2RE777OZMAZG24FV3XP24GXCO",
        "lastModifiedDateTime": "2023-05-01T09:09:19Z",
        "name": "folder3",
        "eTag": '"{2E301580-9B39-4D32-A6D4-E34680133F84},3"',
        "cTag": '"c:{2E301580-9B39-4D32-A6D4-E34680133F84},3"',
        "webUrl": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents/folder1",
        "parentReference": {"path": "/drive/root:/hello"},
        "size": 10484,
    },
    {
        "@microsoft.graph.downloadUrl": "https://example-download-url-item-1",
        "createdDateTime": "2023-05-01T09:09:31Z",
        "id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
        "lastModifiedDateTime": "2023-05-01T09:10:21Z",
        "name": "doit.py",
        "eTag": '"{2E301580-9Y39-4D32-A6D4-E34680133WE8},3"',
        "cTag": '"c:{2E301580-9Y39-4D32-A6D4-E34680133WE8},3"',
        "webUrl": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=doit.py&action=default&mobileredirect=true",
        "parentReference": {"path": "/drive/root:/anc"},
        "size": 10484,
        "file": {"mimeType": "application/python"},
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

RESPONSE_USER2_FILES = [
    {
        "createdDateTime": "2023-05-01T09:09:19Z",
        "id": "01DABHRNU2RE777OZMAZG24FV3XP24GXCO",
        "lastModifiedDateTime": "2023-05-01T09:09:19Z",
        "name": "folder4",
        "eTag": '"{2E301580-9B39-4D32-A6D4-E34680133F84},3"',
        "cTag": '"c:{2E301580-9B39-4D32-A6D4-E34680133F84},3"',
        "webUrl": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents/folder4",
        "parentReference": {"path": "/drive/root:/dummy"},
        "size": 10484,
    },
    {
        "@microsoft.graph.downloadUrl": "https://example-download-url-item-3",
        "createdDateTime": "2023-05-01T09:09:31Z",
        "id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
        "lastModifiedDateTime": "2023-05-01T09:10:21Z",
        "name": "mac.txt",
        "eTag": '"{2E301580-9Y39-4D32-A6D4-E34680133WE8},3"',
        "cTag": '"c:{2E301580-9Y39-4D32-A6D4-E34680133WE8},3"',
        "webUrl": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/mac.txt?sourcedoc=34680133F84%7&file=mac.txt&action=default&mobileredirect=true",
        "parentReference": {"path": "/drive/root:/hello/world"},
        "size": 10484,
        "file": {"mimeType": "plain/text"},
    },
]

EXPECTED_USER2_FILES = [
    {
        "created_at": "2023-05-01T09:09:19Z",
        "_id": "01DABHRNU2RE777OZMAZG24FV3XP24GXCO",
        "_timestamp": "2023-05-01T09:09:19Z",
        "title": "folder4",
        "type": "folder",
        "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents/folder4",
        "size": 10484,
    },
    {
        "created_at": "2023-05-01T09:09:31Z",
        "_id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
        "_timestamp": "2023-05-01T09:10:21Z",
        "title": "mac.txt",
        "type": "file",
        "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/mac.txt?sourcedoc=34680133F84%7&file=mac.txt&action=default&mobileredirect=true",
        "size": 10484,
    },
]

MOCK_ATTACHMENT = {
    "created_at": "2023-05-01T09:09:31Z",
    "_id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
    "_timestamp": "2023-05-01T09:10:21Z",
    "title": "Document.docx",
    "type": "file",
    "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=Document.docx&action=default&mobileredirect=true",
    "size": 10484,
}

MOCK_ATTACHMENT_WITHOUT_EXTENSION = {
    "created_at": "2023-05-01T09:09:31Z",
    "_id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
    "_timestamp": "2023-05-01T09:10:21Z",
    "title": "Document",
    "type": "file",
    "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=Document&action=default&mobileredirect=true",
    "size": 10484,
}

MOCK_ATTACHMENT_WITH_LARGE_DATA = {
    "created_at": "2023-05-01T09:09:31Z",
    "_id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
    "_timestamp": "2023-05-01T09:10:21Z",
    "title": "Document.docx",
    "type": "file",
    "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=Document.docx&action=default&mobileredirect=true",
    "size": 23000000,
}

MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION = {
    "created_at": "2023-05-01T09:09:31Z",
    "_id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
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
EXPECTED_CONTENT_EXTRACTED = {
    "_id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
    "_timestamp": "2023-05-01T09:10:21Z",
    "body": RESPONSE_CONTENT,
}

EXPECTED_FILES_FOLDERS = {}

RESPONSE_PERMISSION1 = {
    "grantedToV2": {
        "user": {
            "id": "user_id_1",
        }
    },
    "grantedToIdentitiesV2": [
        {
            "user": {
                "id": "user_id_2",
            },
            "group": {
                "id": "group_id_1",
            },
        },
    ],
}
RESPONSE_PERMISSION2 = {
    "grantedToV2": {
        "user": {
            "id": "user_id_3",
        }
    },
    "grantedToIdentitiesV2": [
        {
            "user": {
                "id": "user_id_4",
            },
            "group": {
                "id": "group_id_2",
            },
        }
    ],
}
EXPECTED_USER1_FILES_PERMISSION = [
    {
        "type": "folder",
        "title": "folder3",
        "_id": "01DABHRNU2RE777OZMAZG24FV3XP24GXCO",
        "_timestamp": "2023-05-01T09:09:19Z",
        "created_at": "2023-05-01T09:09:19Z",
        "size": 10484,
        "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents/folder1",
        "_allow_access_control": [
            "group:group_id_1",
            "user_id:user_id_1",
            "user_id:user_id_2",
        ],
    },
    {
        "type": "file",
        "title": "doit.py",
        "_id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
        "_timestamp": "2023-05-01T09:10:21Z",
        "created_at": "2023-05-01T09:09:31Z",
        "size": 10484,
        "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=doit.py&action=default&mobileredirect=true",
        "_allow_access_control": [
            "group:group_id_1",
            "user_id:user_id_1",
            "user_id:user_id_2",
        ],
    },
]
EXPECTED_USER2_FILES_PERMISSION = [
    {
        "type": "folder",
        "title": "folder4",
        "_id": "01DABHRNU2RE777OZMAZG24FV3XP24GXCO",
        "_timestamp": "2023-05-01T09:09:19Z",
        "created_at": "2023-05-01T09:09:19Z",
        "size": 10484,
        "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents/folder4",
        "_allow_access_control": [
            "group:group_id_2",
            "user_id:user_id_3",
            "user_id:user_id_4",
        ],
    },
    {
        "type": "file",
        "title": "mac.txt",
        "_id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
        "_timestamp": "2023-05-01T09:10:21Z",
        "created_at": "2023-05-01T09:09:31Z",
        "size": 10484,
        "url": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/mac.txt?sourcedoc=34680133F84%7&file=mac.txt&action=default&mobileredirect=true",
        "_allow_access_control": [
            "group:group_id_2",
            "user_id:user_id_3",
            "user_id:user_id_4",
        ],
    },
]

RESPONSE_GROUPS = [{"name": "group1", "id": "123"}, {"name": "group2", "id": "234"}]

BATCHED_RESPONSE = {
    "responses": [
        {
            "id": "123",
            "body": {
                "value": [
                    {
                        "@microsoft.graph.downloadUrl": "https://example-download-url-item-3",
                        "createdDateTime": "2023-05-01T09:09:19Z",
                        "id": "01DABHRNU2RE777OZMAZG24FV3XP24GXCO",
                        "lastModifiedDateTime": "2023-05-01T09:09:19Z",
                        "name": "folder3",
                        "eTag": '"{2E301580-9B39-4D32-A6D4-E34680133F84},3"',
                        "cTag": '"c:{2E301580-9B39-4D32-A6D4-E34680133F84},3"',
                        "webUrl": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents/folder1",
                        "parentReference": {"path": "/drive/root:/hello"},
                        "size": 10484,
                    },
                ]
            },
        },
        {
            "id": "231",
            "body": {
                "@odata.nextLink": "https://graph.microsoft.com/v1.0/users/231/delta/next-page-token",
                "value": [
                    {
                        "@microsoft.graph.downloadUrl": "https://example-download-url-item-1",
                        "createdDateTime": "2023-05-01T09:09:31Z",
                        "id": "01DABHRNUACUYC4OM3GJG2NVHDI2ABGP4E",
                        "lastModifiedDateTime": "2023-05-01T09:10:21Z",
                        "name": "doit.py",
                        "eTag": '"{2E301580-9Y39-4D32-A6D4-E34680133WE8},3"',
                        "cTag": '"c:{2E301580-9Y39-4D32-A6D4-E34680133WE8},3"',
                        "webUrl": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/_layouts/15/Doc.aspx?sourcedoc=34680133F84%7&file=doit.py&action=default&mobileredirect=true",
                        "parentReference": {"path": "/drive/root:/anc"},
                        "size": 10484,
                        "file": {"mimeType": "application/python"},
                    },
                ],
            },
        },
    ]
}

NEXT_BATCH_RESPONSE = {
    "responses": [
        {
            "id": "231",
            "body": {
                "value": [
                    {
                        "createdDateTime": "2023-05-01T09:09:19Z",
                        "id": "01DABHRNU2RE777OZMAZG24FV3XP24GXCO",
                        "lastModifiedDateTime": "2023-05-01T09:09:19Z",
                        "name": "folder4",
                        "eTag": '"{2E301580-9B39-4D32-A6D4-E34680133F84},3"',
                        "cTag": '"c:{2E301580-9B39-4D32-A6D4-E34680133F84},3"',
                        "webUrl": "https://w076v-my.sharepoint.com/personal/adel_w076v_onmicrosoft_com/Documents/folder4",
                        "parentReference": {"path": "/drive/root:/dummy"},
                        "size": 10484,
                    },
                ]
            },
        },
    ]
}


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

    assert config["client_id"] == ""


@asynccontextmanager
async def create_onedrive_source(use_text_extraction_service=False):
    async with create_source(
        OneDriveDataSource,
        client_id="foo",
        client_secret="bar",
        tenant_id="faa",
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source


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
    async with create_onedrive_source() as source:
        source.client.access_token = "dummy"

        await source.close()

        assert not hasattr(source.client.__dict__, "session")


@pytest.mark.asyncio
async def test_set_access_token():
    async with create_onedrive_source() as source:
        mock_token = {"access_token": "msgraphtoken", "expires_in": "1234555"}
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(mock_token, 200)
        )

        with patch("aiohttp.request", return_value=async_response):
            await source.client.token._set_access_token()

            assert source.client.token.access_token == "msgraphtoken"


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    async with create_onedrive_source() as source:
        DUMMY_RESPONSE = {}
        source.client.get = AsyncIterator([[DUMMY_RESPONSE]])

        await source.ping()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_for_failed_connection_exception(mock_get):
    async with create_onedrive_source() as source:
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
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_with_429_status():
    initial_response = ClientResponseError(None, None)
    initial_response.status = 429
    initial_response.message = "rate-limited"
    initial_response.headers = {"Retry-After": 0.1}

    retried_response = AsyncMock()
    payload = {"value": "Test rate limit"}

    retried_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(payload))
    async with create_onedrive_source() as source:
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch(
                "aiohttp.ClientSession.get",
                side_effect=[initial_response, retried_response],
            ):
                async for response in source.client.get(
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
    with patch("connectors.sources.onedrive.DEFAULT_RETRY_SECONDS", 0.3):
        async with create_onedrive_source() as source:
            with patch.object(AccessToken, "get", return_value="abc"):
                with patch(
                    "aiohttp.ClientSession.get",
                    side_effect=[initial_response, retried_response],
                ):
                    async for response in source.client.get(
                        url="http://localhost:1000/sample"
                    ):
                        result = await response.json()

        assert result == payload


@pytest.mark.asyncio
async def test_get_with_404_status():
    error = ClientResponseError(None, None)
    error.status = 404

    async with create_onedrive_source() as source:
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch(
                "aiohttp.ClientSession.get",
                side_effect=error,
            ):
                with pytest.raises(NotFound):
                    async for response in source.client.get(
                        url="http://localhost:1000/err"
                    ):
                        await response.json()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_with_500_status():
    error = ClientResponseError(None, None)
    error.status = 500

    async with create_onedrive_source() as source:
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch(
                "aiohttp.ClientSession.get",
                side_effect=error,
            ):
                with pytest.raises(InternalServerError):
                    async for response in source.client.get(
                        url="http://localhost:1000/err"
                    ):
                        await response.json()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_post_with_429_status():
    initial_response = ClientPayloadError(None, None)
    initial_response.status = 429
    initial_response.message = "rate-limited"
    initial_response.headers = {"Retry-After": 0.1}

    retried_response = AsyncMock()
    payload = {"value": "Test rate limit"}

    retried_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(payload))
    async with create_onedrive_source() as source:
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch(
                "aiohttp.ClientSession.post",
                side_effect=[initial_response, retried_response],
            ):
                async for response in source.client.post(
                    url="http://localhost:1000/sample"
                ):
                    result = await response.json()

    assert result == payload


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_post_with_429_status_without_retry_after_header():
    initial_response = ClientPayloadError(None, None)
    initial_response.status = 429
    initial_response.message = "rate-limited"

    retried_response = AsyncMock()
    payload = {"value": "Test rate limit"}

    retried_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(payload))
    with patch("connectors.sources.onedrive.DEFAULT_RETRY_SECONDS", 0.3):
        async with create_onedrive_source() as source:
            with patch.object(AccessToken, "get", return_value="abc"):
                with patch(
                    "aiohttp.ClientSession.post",
                    side_effect=[initial_response, retried_response],
                ):
                    async for response in source.client.post(
                        url="http://localhost:1000/sample"
                    ):
                        result = await response.json()

        assert result == payload


@pytest.mark.asyncio
async def test_list_groups():
    async with create_onedrive_source() as source:
        with patch.object(
            OneDriveClient,
            "paginated_api_call",
            return_value=AsyncIterator([RESPONSE_GROUPS]),
        ):
            response = []
            async for group in source.client.list_groups("user-1"):
                response.append(group)

        assert response == RESPONSE_GROUPS


@pytest.mark.asyncio
async def test_list_permissions():
    async with create_onedrive_source() as source:
        with patch.object(
            OneDriveClient,
            "paginated_api_call",
            return_value=AsyncIterator([[RESPONSE_PERMISSION1]]),
        ):
            async for permission in source.client.list_file_permission(
                "user-1", "file-1"
            ):
                assert permission == RESPONSE_PERMISSION1


@pytest.mark.asyncio
async def test_get_owned_files():
    async with create_onedrive_source() as source:
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(RESPONSE_FILES)
        )
        response = []
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch("aiohttp.ClientSession.get", return_value=async_response):
                with patch.object(
                    OneDriveClient,
                    "list_users",
                    return_value=AsyncIterator([EXPECTED_USERS]),
                ) as user:
                    async for file, _ in source.client.get_owned_files(user["id"]):
                        response.append(file)

        assert response == EXPECTED_FILES


@pytest.mark.asyncio
async def test_list_users():
    async with create_onedrive_source() as source:
        response = []
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(RESPONSE_USERS)
        )
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch("aiohttp.ClientSession.get", return_value=async_response):
                async for users in source.client.list_users():
                    for user in users:
                        response.append(user)

            assert response == EXPECTED_USERS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "file, download_url, expected_content",
    [
        (MOCK_ATTACHMENT, "https://content1", EXPECTED_CONTENT),
        (MOCK_ATTACHMENT_WITHOUT_EXTENSION, "https://content2", None),
        (MOCK_ATTACHMENT_WITH_LARGE_DATA, "https://content3", None),
        (MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION, "https://content4", None),
    ],
)
async def test_get_content_when_is_downloadable_is_true(
    file, download_url, expected_content
):
    async with create_onedrive_source() as source:
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch("aiohttp.ClientSession.get", return_value=get_stream_reader()):
                with patch(
                    "aiohttp.StreamReader.iter_chunked",
                    return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
                ):
                    response = await source.get_content(
                        file=file,
                        download_url=download_url,
                        doit=True,
                    )
                    assert response == expected_content


@pytest.mark.asyncio
async def test_get_content_with_extraction_service():
    with patch(
        "connectors.content_extraction.ContentExtraction.extract_text",
        return_value=RESPONSE_CONTENT,
    ), patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
    ):
        async with create_onedrive_source(use_text_extraction_service=True) as source:
            with patch.object(AccessToken, "get", return_value="abc"):
                with patch(
                    "aiohttp.ClientSession.get", return_value=get_stream_reader()
                ):
                    with patch(
                        "aiohttp.StreamReader.iter_chunked",
                        return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
                    ):
                        response = await source.get_content(
                            file=MOCK_ATTACHMENT,
                            download_url="https://content1",
                            doit=True,
                        )
                        assert response == EXPECTED_CONTENT_EXTRACTED


@pytest.mark.asyncio
async def test_get_data():
    async_response, next_page_response = AsyncMock(), AsyncMock()
    result = []
    expected_result = [
        [RESPONSE_USER1_FILES[0]],
        [RESPONSE_USER1_FILES[1]],
        [RESPONSE_USER2_FILES[0]],
    ]
    async_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(BATCHED_RESPONSE))
    next_page_response.__aenter__ = AsyncMock(
        return_value=JSONAsyncMock(NEXT_BATCH_RESPONSE)
    )
    async with create_onedrive_source() as source:
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch(
                "aiohttp.ClientSession.post",
                side_effect=[async_response, next_page_response],
            ):
                async for file, _ in source.get_data(
                    batched_users=[
                        {"name": "user1", "id": "123"},
                        {"name": "user1", "id": "231"},
                    ]
                ):
                    result.append(file)

    assert result == expected_result


@patch.object(
    OneDriveClient, "list_users", return_value=AsyncIterator([EXPECTED_USERS])
)
@patch.object(
    OneDriveDataSource,
    "get_data",
    side_effect=[
        (
            AsyncIterator(
                [
                    (RESPONSE_USER1_FILES, "11"),
                    (RESPONSE_USER2_FILES, "12"),
                ],
            )
        )
    ],
)
@pytest.mark.asyncio
async def test_get_docs(users_patch, files_patch):
    async with create_onedrive_source() as source:
        expected_responses = [*EXPECTED_USER1_FILES, *EXPECTED_USER2_FILES]
        source.get_content = AsyncMock(return_value=EXPECTED_CONTENT)

        documents, downloads = [], []
        async for item, content in source.get_docs():
            documents.append(item)

            if content:
                downloads.append(content)

        assert documents == expected_responses

        assert len(downloads) == 2


@pytest.mark.parametrize(
    "advanced_rules, expected_validation_result",
    [
        (
            # valid: empty array should be valid
            [],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: empty object should also be valid -> default value in Kibana
            {},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: one custom query
            [
                {
                    "owners": ["a1@onmicrosoft.com", "b1@onmicrosoft.com"],
                    "parentPathPattern": "/drive/root:/folder1/folder2",
                }
            ],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: two custom queries
            [
                {
                    "owners": ["a1@onmicrosoft.com"],
                    "skipFilesWithExtensions": [".py"],
                },
                {
                    "owners": ["b1@onmicrosoft.com", "c1@onmicrosoft.com"],
                    "parentPathPattern": "/drive/root:/hello/*",
                },
            ],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # invalid: value empty
            [
                {"owners": ["a1@onmicrosoft.com"], "parentPathPattern": ""},
                {"owners": ["a1@onmicrosoft.com"]},
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: unallowed key
            [{"owners": ["a1@onmicrosoft.com"], "path": "/drive/root:/a/**"}],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: invalid format in owners
            {"owners": ["abccom.in"]},
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: array of arrays -> wrong type
            [
                {
                    "owners": ["a1@onmicrosoft.com"],
                    "skipFilesWithExtensions": [[".pdf", ".exe"], [".docs"]],
                }
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_advanced_rules_validation(advanced_rules, expected_validation_result):
    async with create_onedrive_source() as source:
        validation_result = await OneDriveAdvancedRulesValidator(source).validate(
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
                            "owners": ["AdeleV@w076v.onmicrosoft.com"],
                            "skipFilesWithExtensions": [".py"],
                        },
                        {
                            "owners": ["AlexW@w076v.onmicrosoft.com"],
                            "parentPathPattern": "/drive/root:/hello/*",
                        },
                    ],
                }
            }
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules(filtering):
    async with create_onedrive_source() as source:
        with patch.object(AccessToken, "get", return_value="abc"):
            with patch.object(
                OneDriveClient,
                "list_users",
                return_value=AsyncIterator([EXPECTED_USERS]),
            ):
                async_response_user1 = AsyncMock()
                async_response_user1.__aenter__ = AsyncMock(
                    return_value=JSONAsyncMock({"value": RESPONSE_USER1_FILES})
                )

                async_response_user2 = AsyncMock()
                async_response_user2.__aenter__ = AsyncMock(
                    return_value=JSONAsyncMock({"value": RESPONSE_USER2_FILES})
                )

                with patch(
                    "aiohttp.ClientSession.get",
                    side_effect=[async_response_user1, async_response_user2],
                ):
                    documents = []
                    expected_responses = [
                        EXPECTED_USER1_FILES[0],
                        EXPECTED_USER2_FILES[1],
                    ]
                    async for item, _ in source.get_docs(filtering):
                        documents.append(item)

        assert documents == expected_responses


@pytest.mark.asyncio
async def test_get_access_control_dls_disabled():
    async with create_onedrive_source() as source:
        source._dls_enabled = MagicMock(return_value=False)

        acl = []
        async for access_control in source.get_access_control():
            acl.append(access_control)

        assert len(acl) == 0


@pytest.mark.asyncio
async def test_get_access_control_dls_enabled():
    expected_user_access_control = [
        [
            "email:AdeleV@w076v.onmicrosoft.com",
            "group:e35d6159-f6c1-462c-a60c-11f29880e9c0",
            "user:AdeleV@w076v.onmicrosoft.com",
            "user_id:3fada5d6-125c-4aaa-bc23-09b5d301b2a7",
        ],
        [
            "email:AlexW@w076v.onmicrosoft.com",
            "group:5010ad09-7ad0-48e7-8047-1ae0f6117a17",
            "user:AlexW@w076v.onmicrosoft.com",
            "user_id:8e083933-1720-4d2f-80d0-3976669b40ee",
        ],
    ]

    async with create_onedrive_source() as source:
        source._dls_enabled = MagicMock(return_value=True)

        with patch.object(AccessToken, "get", return_value="abc"):
            with patch.object(
                OneDriveClient,
                "list_users",
                return_value=AsyncIterator([EXPECTED_USERS]),
            ):
                user_access_control = []
                async for user_doc in source.get_access_control():
                    user_doc["query"]["template"]["params"]["access_control"].sort()
                    user_access_control.append(
                        user_doc["query"]["template"]["params"]["access_control"]
                    )

                assert expected_user_access_control == user_access_control


@patch.object(
    OneDriveClient, "list_users", return_value=AsyncIterator([EXPECTED_USERS])
)
@patch.object(
    OneDriveDataSource,
    "get_data",
    side_effect=[
        (
            AsyncIterator(
                [
                    (RESPONSE_USER1_FILES, "11"),
                    (RESPONSE_USER2_FILES, "12"),
                ]
            )
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_without_dls_enabled(users_patch, files_patch):
    async with create_onedrive_source() as source:
        source._dls_enabled = MagicMock(return_value=False)

        expected_responses = [*EXPECTED_USER1_FILES, *EXPECTED_USER2_FILES]
        source.get_content = AsyncMock(return_value=EXPECTED_CONTENT)

        documents, downloads = [], []
        async for item, content in source.get_docs():
            documents.append(item)

            if content:
                downloads.append(content)

        assert documents == expected_responses

        assert len(downloads) == 2


@patch.object(
    OneDriveClient, "list_users", return_value=AsyncIterator([EXPECTED_USERS])
)
@patch.object(
    OneDriveDataSource,
    "get_data",
    side_effect=[
        (
            AsyncIterator(
                [
                    (RESPONSE_USER1_FILES, "11"),
                    (RESPONSE_USER2_FILES, "12"),
                ]
            )
        ),
    ],
)
@patch.object(
    OneDriveClient,
    "list_file_permission",
    side_effect=[
        (AsyncIterator([RESPONSE_PERMISSION1])),
        (AsyncIterator([RESPONSE_PERMISSION1])),
        (AsyncIterator([RESPONSE_PERMISSION2])),
        (AsyncIterator([RESPONSE_PERMISSION2])),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_dls_enabled(users_patch, files_patch, permissions_patch):
    async with create_onedrive_source() as source:
        source._dls_enabled = MagicMock(return_value=True)

        expected_responses = [
            *EXPECTED_USER1_FILES_PERMISSION,
            *EXPECTED_USER2_FILES_PERMISSION,
        ]
        source.get_content = AsyncMock(side_effect=[EXPECTED_CONTENT, EXPECTED_CONTENT])

        documents, downloads = [], []
        async for item, content in source.get_docs():
            item.get("_allow_access_control", []).sort()
            documents.append(item)

            if content:
                downloads.append(content)

        assert documents == expected_responses

        assert len(downloads) == 2
