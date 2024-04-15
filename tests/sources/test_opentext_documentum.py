#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Opentext Documentum source class methods"""
from contextlib import asynccontextmanager
from copy import copy
from unittest import mock
from unittest.mock import AsyncMock, Mock, patch

import pytest
from aiohttp import StreamReader
from aiohttp.client_exceptions import ClientResponseError

from connectors.source import ConfigurableFieldValueError
from connectors.sources.opentext_documentum import (
    InternalServerError,
    NotFound,
    OpentextDocumentumClient,
    OpentextDocumentumDataSource,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source

RESPONSE_CONTENT = "# This is the dummy file"

EXPECTED_CONTENT = {
    "_id": 10001,
    "_timestamp": "2023-02-01T01:02:20",
    "_attachment": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl",
}

MOCK_ATTACHMENT = {
    "id": 10001,
    "title": "test_file.txt",
    "created": "2023-02-01T01:02:20",
    "size": 200,
    "content": {"src": "IyBUaGlzIGlzIHRoZSBkdW1teSBmaWxl"},
    "updated": "2023-02-01T01:02:20",
}

MOCK_REPOSITORIES = {
    "id": "http://127.0.0.1:2099/repositories",
    "repository_name": "Repositories",
    "author": [{"name": "Alex Wilber"}],
    "updated": "2018-09-28T14:41:29.686+00:00",
    "page": 0,
    "items-per-page": 100,
    "total": 2,
    "links": [{"rel": "self", "href": "http://127.0.0.1:2099/repositories"}],
    "entries": [
        {
            "id": 10001,
            "title": "Repo_1",
            "summary": "Repository Summary for Repo_1",
            "updated": "2024-12-12T00:00:00Z",
            "published": "2018-09-28T14:41:29.738+00:00",
            "links": [
                {
                    "rel": "edit",
                    "href": "http://127.0.0.1:2099/repositories/",
                }
            ],
            "content": {
                "type": "application/vnd.emc.documentum+json",
                "src": "http://127.0.0.1:2099/repositories/",
            },
        },
        {
            "id": 10002,
            "title": "Repo_2",
            "summary": "Repository Summary for Repo_2",
            "updated": "2024-12-12T00:00:00Z",
            "published": "2018-09-28T14:41:29.738+00:00",
            "links": [
                {
                    "rel": "edit",
                    "href": "http://127.0.0.1:2099/repositories/",
                }
            ],
            "content": {
                "type": "application/vnd.emc.documentum+json",
                "src": "http://127.0.0.1:2099/repositories/",
            },
        },
    ],
}

EXPECTED_REPO_1 = {
    "_id": 10001,
    "_timestamp": "2024-12-12T00:00:00Z",
    "type": "Repository",
    "repository_name": "Repo_1",
    "summary": "Repository Summary for Repo_1",
    "authors": [{"name": "Alex Wilber"}],
}

EXPECTED_REPO_2 = {
    "_id": 10002,
    "_timestamp": "2024-12-12T00:00:00Z",
    "type": "Repository",
    "repository_name": "Repo_2",
    "summary": "Repository Summary for Repo_2",
    "authors": [{"name": "Alex Wilber"}],
}

MOCK_CABINETS = {
    "id": "http://127.0.0.1:2099/repositories/repository_name/cabinets",
    "title": "Cabinets",
    "author": [{"name": "Alex Wilber"}],
    "updated": "2018-09-28T14:41:29.686+00:00",
    "page": 0,
    "items-per-page": 100,
    "total": 1,
    "links": [
        {
            "rel": "self",
            "href": "http://127.0.0.1:2099/repositories/Test Repository/cabinets",
        }
    ],
    "entries": [
        {
            "id": 10003,
            "title": "Cabinet",
            "type": "cabinet",
            "updated": "2024-12-12T00:00:00Z",
            "definition": "Human Resources Cabinet",
            "properties": [
                {
                    "name": "Cabinet_",
                    "description": "Cabinet containing HR documents",
                }
            ],
        }
    ],
}

MOCK_RESPONSE_FOLDERS = {
    "_id": "http://127.0.0.1:2099/folders",
    "title": "Folders",
    "author": [{"name": "Alex Wilber"}],
    "updated": "2018-09-28T14:41:29.686+00:00",
    "page": 0,
    "items-per-page": 100,
    "total": 1,
    "links": [{"rel": "self", "href": "http://127.0.0.1:2099/folders"}],
    "entries": [
        {
            "id": 10004,
            "title": "root",
            "updated": "2024-12-12T00:00:00Z",
            "created": "2018-09-28T14:41:29.686+00:00",
            "parent_id": None,
        }
    ],
}

MOCK_RESPONSE_FOLDERS_RECURSIVE = {
    "_id": "http://127.0.0.1:2099/folders/10004/folders",
    "title": "Folders",
    "author": [{"name": "Alex Wilber"}],
    "updated": "2018-09-28T14:41:29.686+00:00",
    "page": 0,
    "items-per-page": 100,
    "total": 1,
    "links": [{"rel": "self", "href": "http://127.0.0.1:2099/folders/10004/folders"}],
    "entries": [
        {
            "id": 10007,
            "title": "folder-inside-root",
            "updated": "2024-12-12T00:00:00Z",
            "created": "2018-09-28T14:41:29.686+00:00",
            "parent_id": None,
        }
    ],
}

MOCK_RESPONSE_FILES = {
    "_id": "http://127.0.0.1:2099/folders/rootid/documents",
    "title": "Files",
    "author": [{"name": "Alex Wilber"}],
    "updated": "2018-09-28T14:41:29.686+00:00",
    "page": 0,
    "items-per-page": 100,
    "total": 1,
    "links": [
        {
            "rel": "self",
            "href": "http://127.0.0.1:2099/folders/rootid/documents",
        }
    ],
    "entries": [
        {
            "id": 10005,
            "title": "test_file.txt",
            "size": 256,
            "updated": "2024-12-12T00:00:00Z",
            "created": "2018-09-28T14:41:29.686+00:00",
            "parent_id": 10004,
            "content": {
                "src": "http://127.0.0.1:2099/dctm-rest/repositories/repository_name/nodes/test_file/content"
            },
        }
    ],
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


def get_stream_reader():
    async_mock = AsyncMock()
    async_mock.__aenter__ = AsyncMock(return_value=StreamReaderAsyncMock())
    return async_mock


def get_json_mock(mock_response):
    async_mock = AsyncMock()
    async_mock.__aenter__ = AsyncMock(return_value=JSONAsyncMock(mock_response))
    return async_mock


@asynccontextmanager
async def create_opentext_documentum(use_text_extraction_service=False):
    async with create_source(
        OpentextDocumentumDataSource,
        host_url="http://127.0.0.1:8080",
        username="admin",
        password="changeme",
        repositories="*",
        ssl_enabled=False,
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source


@pytest.mark.asyncio
async def test_ping():
    async with create_opentext_documentum() as source:
        with patch.object(
            OpentextDocumentumClient,
            "api_call",
            return_value=AsyncIterator([MOCK_REPOSITORIES]),
        ):
            await source.ping()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_when_failed_connection_raise_exception(mock_api_call):
    async with create_opentext_documentum() as source:
        with patch.object(
            OpentextDocumentumClient,
            "api_call",
            side_effect=Exception("Something went wrong"),
        ):
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_with_429_status_should_retry():
    initial_response = ClientResponseError(None, None)
    initial_response.status = 429
    initial_response.message = "rate-limited"
    initial_response.headers = {"Retry-After": 0.1}

    retried_response = AsyncMock()
    payload = {"value": "Test rate limit"}

    retried_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(payload))
    async with create_opentext_documentum() as source:
        with patch(
            "aiohttp.ClientSession.get",
            side_effect=[initial_response, retried_response],
        ):
            async for response in source.opentext_client.api_call(
                url="http://localhost:1000/sample"
            ):
                result = await response.json()

    assert result == payload


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_with_429_status_retry_after_header_string():
    initial_response = ClientResponseError(None, None)
    initial_response.status = 429
    initial_response.message = "rate-limited"
    initial_response.headers = {"Retry-After": "0.1"}

    retried_response = AsyncMock()
    payload = {"value": "Test rate limit"}

    retried_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(payload))
    with patch("connectors.sources.opentext_documentum.DEFAULT_RETRY_SECONDS", 0):
        async with create_opentext_documentum() as source:
            with patch(
                "aiohttp.ClientSession.get",
                side_effect=[initial_response, retried_response],
            ):
                async for response in source.opentext_client.api_call(
                    url="http://localhost:1000/sample"
                ):
                    result = await response.json()

        assert result == payload


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_with_429_status_without_retry_after_header_should_use_default_interval():
    initial_response = ClientResponseError(None, None)
    initial_response.status = 429
    initial_response.message = "rate-limited"

    retried_response = AsyncMock()
    payload = {"value": "Test rate limit"}

    retried_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(payload))
    with patch("connectors.sources.opentext_documentum.DEFAULT_RETRY_SECONDS", 0):
        async with create_opentext_documentum() as source:
            with patch(
                "aiohttp.ClientSession.get",
                side_effect=[initial_response, retried_response],
            ):
                async for response in source.opentext_client.api_call(
                    url="http://localhost:1000/sample"
                ):
                    result = await response.json()

        assert result == payload


@pytest.mark.asyncio
async def test_get_with_404_status_should_raise():
    error = ClientResponseError(None, None)
    error.status = 404

    async with create_opentext_documentum() as source:
        with patch(
            "aiohttp.ClientSession.get",
            side_effect=error,
        ):
            with pytest.raises(NotFound):
                async for response in source.opentext_client.api_call(
                    url="http://localhost:1000/err"
                ):
                    await response.json()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_with_500_status_should_raise():
    error = ClientResponseError(None, None)
    error.status = 500

    async with create_opentext_documentum() as source:
        with patch(
            "aiohttp.ClientSession.get",
            side_effect=error,
        ):
            with pytest.raises(InternalServerError):
                async for response in source.opentext_client.api_call(
                    url="http://localhost:1000/err"
                ):
                    await response.json()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["host_url", "username", "password", "repositories"])
async def test_validate_when_config_missing_fields_raise(field):
    async with create_opentext_documentum() as source:
        source.configuration.get_field(field).value = ""

        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_get_content():
    async with create_opentext_documentum() as source:
        with mock.patch("aiohttp.ClientSession.get", return_value=get_stream_reader()):
            with mock.patch(
                "aiohttp.StreamReader.iter_chunked",
                return_value=AsyncIterator([bytes(RESPONSE_CONTENT, "utf-8")]),
            ):
                response = await source.get_content(
                    attachment=MOCK_ATTACHMENT,
                    doit=True,
                )
                assert response == EXPECTED_CONTENT


async def test_remote_validation_when_invalid_repos_raise():
    with patch.object(
        OpentextDocumentumClient,
        "api_call",
        return_value=AsyncIterator([MOCK_REPOSITORIES]),
    ):
        async with create_opentext_documentum() as source:
            with pytest.raises(ConfigurableFieldValueError):
                source.repositories = ["Repo_1", "Repo_3"]
                await source._remote_validation()


@pytest.mark.asyncio
async def test_fetch_all_repositories():
    async with create_opentext_documentum() as source:
        with patch.object(
            OpentextDocumentumClient,
            "paginated_api_call",
            return_value=AsyncIterator([MOCK_REPOSITORIES]),
        ):
            repos = []
            expected_repos = [
                EXPECTED_REPO_1,
                EXPECTED_REPO_2,
            ]
            async for repository in source.fetch_repositories():
                repos.append(repository)
            assert repos == expected_repos


@pytest.mark.asyncio
async def test_fetch_repositories_with_no_repos_available():
    async with create_opentext_documentum() as source:
        mocked_repo = copy(MOCK_REPOSITORIES)
        mocked_repo["entries"] = []

        with patch(
            "aiohttp.ClientSession.get",
            return_value=get_json_mock(mocked_repo),
        ):
            repos = []
            async for repository in source.fetch_repositories():
                repos.append(repository)
            assert len(repos) == 0


@pytest.mark.asyncio
async def test_fetch_configured_repositories():
    async with create_opentext_documentum() as source:
        source.repositories = ["Repo_1"]
        mocked_repo = MOCK_REPOSITORIES["entries"][0]
        with patch.object(
            OpentextDocumentumClient,
            "paginated_api_call",
            return_value=AsyncIterator([mocked_repo]),
        ):
            async for repository in source.fetch_repositories():
                assert repository == {
                    "_id": 10001,
                    "_timestamp": "2024-12-12T00:00:00Z",
                    "type": "Repository",
                    "repository_name": "Repo_1",
                    "summary": "Repository Summary for Repo_1",
                }


@pytest.mark.asyncio
async def test_fetch_cabinets():
    async with create_opentext_documentum() as source:
        with patch.object(
            OpentextDocumentumClient,
            "paginated_api_call",
            return_value=AsyncIterator([MOCK_CABINETS]),
        ):
            async for cabinet in source.fetch_cabinets("Test Repository"):
                assert cabinet == {
                    "_id": 10003,
                    "_timestamp": "2024-12-12T00:00:00Z",
                    "type": "Cabinet",
                    "cabinet_name": "Cabinet",
                    "definition": "Human Resources Cabinet",
                    "authors": [{"name": "Alex Wilber"}],
                }


@pytest.mark.asyncio
async def test_fetch_folders():
    async with create_opentext_documentum() as source:
        with patch.object(
            OpentextDocumentumClient,
            "paginated_api_call",
            side_effect=[
                AsyncIterator([MOCK_RESPONSE_FOLDERS]),
                AsyncIterator([MOCK_RESPONSE_FOLDERS_RECURSIVE]),
            ],
        ):
            incoming_folders = []
            expected_folders = ["root", "folder-inside-root"]
            async for folder in source.fetch_folders_recursively("Test Repository"):
                incoming_folders.append(folder["title"])
            assert incoming_folders == expected_folders


@pytest.mark.asyncio
async def test_fetch_files():
    async with create_opentext_documentum() as source:
        with patch.object(
            OpentextDocumentumClient,
            "paginated_api_call",
            return_value=AsyncIterator([MOCK_RESPONSE_FILES]),
        ):
            async for file, _ in source.fetch_files("Test Repository", "123"):
                assert file == {
                    "_id": 10005,
                    "_timestamp": "2024-12-12T00:00:00Z",
                    "type": "File",
                    "title": "test_file.txt",
                    "size": 256,
                }


@pytest.mark.asyncio
async def test_get_docs():
    expected_responses_ids = [
        MOCK_REPOSITORIES.get("id"),
        MOCK_CABINETS.get("id"),
        MOCK_RESPONSE_FOLDERS.get("id"),
        MOCK_RESPONSE_FILES.get("id"),
    ]
    async with create_opentext_documentum() as source:
        with patch.object(
            OpentextDocumentumDataSource,
            "fetch_repositories",
            return_value=AsyncIterator([MOCK_REPOSITORIES]),
        ), patch.object(
            OpentextDocumentumDataSource,
            "fetch_cabinets",
            return_value=AsyncIterator([MOCK_CABINETS]),
        ), patch.object(
            OpentextDocumentumDataSource,
            "fetch_folders_recursively",
            return_value=AsyncIterator([MOCK_RESPONSE_FOLDERS]),
        ), patch.object(
            OpentextDocumentumDataSource,
            "fetch_files",
            return_value=AsyncIterator([(MOCK_RESPONSE_FILES, None)]),
        ):
            response_ids = []
            async for result, _ in source.get_docs():
                response_ids.append(result.get("id"))
            assert response_ids == expected_responses_ids
