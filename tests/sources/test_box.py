#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Box source class methods"""
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest
from aiohttp import StreamReader
from aiohttp.client_exceptions import ClientResponseError

from connectors.source import ConfigurableFieldValueError
from connectors.sources.box import BoxDataSource, NotFound, TokenError
from tests.commons import AsyncIterator
from tests.sources.support import create_source

MOCK_RESPONSE = {
    "total_count": 2,
    "entries": [
        {
            "type": "file",
            "id": "1273609717220",
            "etag": "0",
            "name": "dummy.pdf",
            "modified_at": "2023-08-04T03:17:55-07:00",
            "size": 1875887,
        },
        {
            "type": "folder",
            "id": "220376481442",
            "etag": "0",
            "name": "My Box Notes",
            "modified_at": "2023-08-18T00:59:46-07:00",
            "size": 1501,
        },
    ],
    "offset": 0,
    "limit": 100,
    "order": [{"by": "type", "direction": "ASC"}, {"by": "name", "direction": "ASC"}],
}
EMPTY_RESPONSE = {
    "total_count": 1,
    "entries": [],
    "offset": 100,
    "limit": 100,
    "order": [{"by": "type", "direction": "ASC"}, {"by": "name", "direction": "ASC"}],
}
FOLDER_ITEMS = {
    "total_count": 1,
    "entries": [
        {
            "type": "file",
            "id": "1273609717123",
            "etag": "0",
            "name": "dummy.txt",
            "modified_at": "2023-08-04T03:17:55-07:00",
            "size": 1875887,
        },
    ],
    "offset": 0,
    "limit": 100,
    "order": [{"by": "type", "direction": "ASC"}, {"by": "name", "direction": "ASC"}],
}
MOCK_ATTACHMENT = {
    "type": "file",
    "id": "1273609717220",
    "etag": "0",
    "name": "dummy.pdf",
    "modified_at": "2023-08-04T03:17:55-07:00",
    "size": 1875887,
}
MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION = {
    "type": "file",
    "id": "1273609717220",
    "etag": "0",
    "name": "Get Started with Box.jpg",
    "modified_at": "2023-08-04T03:17:55-07:00",
    "size": 1875887,
}
MOCK_ATTACHMENT_WITH_LARGE_DATA = {
    "type": "file",
    "id": "1273609717220",
    "etag": "0",
    "name": "dummy.pdf",
    "modified_at": "2023-08-04T03:17:55-07:00",
    "size": 10485762,
}
MOCK_ATTACHMENT_WITHOUT_EXTENSION = {
    "type": "file",
    "id": "1273609717220",
    "etag": "0",
    "name": "Get Started with Box",
    "modified_at": "2023-08-04T03:17:55-07:00",
    "size": 1875887,
}
MOCK_RESPONSE_FETCH = [
    (
        {
            "type": "file",
            "etag": "0",
            "name": "dummy.pdf",
            "size": 1875887,
            "_id": "1273609717220",
            "_timestamp": "2023-08-04T03:17:55-07:00",
        },
        {
            "type": "file",
            "etag": "0",
            "name": "dummy.pdf",
            "size": 1875887,
            "id": "1273609717220",
            "modified_at": "2023-08-04T03:17:55-07:00",
        },
    ),
    (
        {
            "type": "folder",
            "etag": "0",
            "name": "My Box Notes",
            "size": 1501,
            "_id": "220376481442",
            "_timestamp": "2023-08-18T00:59:46-07:00",
        },
        None,
    ),
]
EXPECTED_CONTENT = {
    "_id": "1273609717220",
    "_timestamp": "2023-08-04T03:17:55-07:00",
    "_attachment": "SGVsbG8gV29ybGQuLi4=",
}


class JSONAsyncMock:
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


def get_json_mock(MOCK_RESPONSE, status):
    async_mock = AsyncMock()
    async_mock.__aenter__ = AsyncMock(
        return_value=JSONAsyncMock(json=MOCK_RESPONSE, status=status)
    )
    return async_mock


def side_effect_function(url, headers, params):
    if params is not None:
        if params.get("offset") == 0 and "/2.0/folders/220376481442/items" in url:
            return JSONAsyncMock(json=FOLDER_ITEMS, status=200)
        if params.get("offset") == 0:
            return JSONAsyncMock(json=MOCK_RESPONSE, status=200)
        elif params.get("offset") == 1000:
            return JSONAsyncMock(json=EMPTY_RESPONSE, status=200)
    return StreamReaderAsyncMock()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["client_id", "client_secret", "refresh_token"])
async def test_validate_config_missing_fields_then_raise(field):
    async with create_source(BoxDataSource) as source:
        source.configuration.set_field(name=field, value="")

        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_get():
    async with create_source(BoxDataSource) as source:
        source.client.token._set_access_token = AsyncMock()
        source.client.token.access_token = "abcd#123"
        access_token = await source.client.token.get()
        assert access_token == source.client.token.access_token


@pytest.mark.asyncio
@pytest.mark.parametrize("box_account", ["box_free", "box_enterprise"])
async def test_set_access_token(box_account):
    async with create_source(BoxDataSource) as source:
        source.client.token.is_enterprise = box_account
        mock_token = {
            "access_token": "abc#123",
            "expires_in": "1234555",
            "refresh_token": "xyz#123",
        }
        async_response = AsyncMock()
        async_response.__aenter__ = AsyncMock(
            return_value=JSONAsyncMock(mock_token, 200)
        )
        with patch("aiohttp.ClientSession.post", return_value=async_response):
            await source.client.token._set_access_token()
            assert source.client.token.access_token == "abc#123"


@pytest.mark.asyncio
async def test_set_access_token_when_tokenerror():
    async with create_source(BoxDataSource) as source:
        with patch("aiohttp.ClientSession.post", side_effect=Exception):
            with pytest.raises(TokenError):
                await source.client.token._set_access_token()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries")
@pytest.mark.parametrize(
    "status_code, exception",
    [
        (409, ClientResponseError),
        (401, ClientResponseError),
        (404, NotFound),
        (429, Exception),
    ],
)
async def test_get_clientresponseerror(
    mock_time_to_sleep_between_retries, status_code, exception
):
    async with create_source(BoxDataSource) as source:
        mock_time_to_sleep_between_retries.return_value = 0
        source.client.token.get = AsyncMock()
        source.client.token._set_access_token = AsyncMock()
        source.client._http_session.get = AsyncMock(
            side_effect=ClientResponseError(
                status=status_code,
                request_info=aiohttp.RequestInfo(
                    real_url="", method=None, headers=None, url=""
                ),
                history=None,
                headers={"retry-after": 0},
            )
        )
        with pytest.raises(exception):
            await source.client.get("/me", {})


@pytest.mark.asyncio
async def test_ping_with_successful_connection():
    async with create_source(BoxDataSource) as source:
        source.client.token.get = AsyncMock()
        source.client._http_session.get = AsyncMock(
            return_value=JSONAsyncMock(json={"username": "demo"}, status=200)
        )
        try:
            await source.ping()
        except Exception as exception:
            msg = "Ping should've been successful"
            raise AssertionError(msg) from exception


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries")
async def test_ping_with_unsuccessful_connection(mock_time_to_sleep_between_retries):
    async with create_source(BoxDataSource) as source:
        mock_time_to_sleep_between_retries.return_value = 0
        source.client.token.get = AsyncMock(side_effect=Exception())
        with pytest.raises(Exception):
            await source.ping()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attachment, doit, expected_content",
    [
        (MOCK_ATTACHMENT, True, EXPECTED_CONTENT),
        (MOCK_ATTACHMENT_WITHOUT_EXTENSION, True, None),
        (MOCK_ATTACHMENT_WITH_LARGE_DATA, True, None),
        (MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION, True, None),
        (MOCK_ATTACHMENT, False, None),
    ],
)
async def test_get_content(attachment, doit, expected_content):
    async with create_source(BoxDataSource) as source:
        source.client.token.get = AsyncMock()
        source.client._http_session.get = AsyncMock(
            return_value=StreamReaderAsyncMock()
        )
        with patch(
            "aiohttp.StreamReader.iter_chunked",
            return_value=AsyncIterator([bytes("Hello World...", "utf-8")]),
        ):
            response = await source.get_content(
                attachment=attachment,
                doit=doit,
            )
            assert response == expected_content


@pytest.mark.asyncio
async def test_get_consumer():
    MOCK_FOLDER_DOC = {
        "type": "folder",
        "etag": "0",
        "name": "My Box Notes",
        "size": 1501,
        "_id": "220376481442",
        "_timestamp": "2023-08-18T00:59:46-07:00",
    }

    MOCK_FILE_DOC = {
        "type": "file",
        "etag": "0",
        "name": "dummy.pdf",
        "size": 1875887,
        "_id": "1273609717220",
        "_timestamp": "2023-08-04T03:17:55-07:00",
    }
    async with create_source(BoxDataSource) as source:
        source.tasks = 2
        await source.queue.put((MOCK_FILE_DOC, None))
        await source.queue.put(("FINISHED"))
        await source.queue.put((MOCK_FOLDER_DOC, None))
        await source.queue.put(("FINISHED"))

        items = []
        async for item, _ in source._consumer():
            items.append(item)

        assert items == [MOCK_FILE_DOC, MOCK_FOLDER_DOC]


@pytest.mark.asyncio
async def test_fetch():
    actual_response = []
    expected_response = [
        {
            "type": "file",
            "etag": "0",
            "name": "dummy.pdf",
            "size": 1875887,
            "_id": "1273609717220",
            "_timestamp": "2023-08-04T03:17:55-07:00",
        },
        {
            "type": "folder",
            "etag": "0",
            "name": "My Box Notes",
            "size": 1501,
            "_id": "220376481442",
            "_timestamp": "2023-08-18T00:59:46-07:00",
        },
        "FINISHED",
    ]
    async with create_source(BoxDataSource) as source:
        source.client.token.get = AsyncMock()
        source.client._http_session.get = AsyncMock(side_effect=side_effect_function)
        await source._fetch("0")
        while not source.queue.empty():
            _, doc = await source.queue.get()
            if isinstance(doc, tuple):
                actual_response.append(doc[0])
            else:
                actual_response.append(doc)
        assert actual_response == expected_response


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries")
async def test_fetch_negative(mock_time_to_sleep_between_retries):
    async with create_source(BoxDataSource) as source:
        mock_time_to_sleep_between_retries.return_value = Mock()
        source.client.token.get = AsyncMock(side_effect=Exception())
        response = await source._fetch("0")
    assert response is None


@pytest.mark.asyncio
async def test_get_docs():
    actual_response = []
    expected_response = [
        {
            "type": "file",
            "etag": "0",
            "name": "dummy.pdf",
            "size": 1875887,
            "_id": "1273609717220",
            "_timestamp": "2023-08-04T03:17:55-07:00",
        },
        {
            "type": "folder",
            "etag": "0",
            "name": "My Box Notes",
            "size": 1501,
            "_id": "220376481442",
            "_timestamp": "2023-08-18T00:59:46-07:00",
        },
    ]
    async with create_source(BoxDataSource) as source:
        source._fetch = AsyncMock()
        await source.queue.put((expected_response[0], None))
        await source.queue.put((expected_response[1], None))
        await source.queue.put(("FINISHED"))

        async for doc, _ in source.get_docs():
            actual_response.append(doc)

    assert actual_response == expected_response
