from unittest.mock import AsyncMock, MagicMock, Mock, patch

import aiohttp
import pytest
from httpx import Response
from notion_client import APIResponseError

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.notion import NotFound, NotionClient, NotionDataSource
from tests.commons import AsyncIterator
from tests.sources.support import create_source

DATABASE = {
    "object": "database",
    "id": "a1b2c3d4-5678-90ab-cdef-1234567890ab",
    "created_time": "2021-07-13T16:48:00.000Z",
    "last_edited_time": "2021-07-13T16:48:00.000Z",
    "title": [
        {
            "type": "text",
            "text": {
                "content": "This is a test database.",
                "link": None,
            },
            "annotations": {
                "bold": False,
                "italic": False,
                "strikethrough": False,
                "underline": False,
                "code": False,
                "color": "default",
            },
            "plain_text": "This is a test database.",
            "href": None,
        }
    ],
}
BLOCK = {
    "object": "page",
    "id": "b3a9a3e8-5a8a-4ac0-9b52-9fb62772a9bd",
    "created_time": "2021-06-13T16:48:00.000Z",
    "last_edited_time": "2021-06-13T16:48:00.000Z",
    "properties": {
        "Name": {
            "id": "title",
            "type": "title",
            "title": [
                {
                    "type": "text",
                    "text": {
                        "content": "This is a test page.",
                        "link": None,
                    },
                    "annotations": {
                        "bold": False,
                        "italic": False,
                        "strikethrough": False,
                        "underline": False,
                        "code": False,
                        "color": "default",
                    },
                    "plain_text": "This is a test page.",
                    "href": None,
                }
            ],
        }
    },
}
COMMENT = {
    "object": "comment",
    "id": "c8f5a3e8-5a8a-4ac0-9b52-9fb62772a9bd",
    "created_time": "2021-06-13T16:48:00.000Z",
    "last_edited_time": "2021-06-13T16:48:00.000Z",
    "parent": {"type": "page_id", "page_id": "some_page_id"},
    "rich_text": [
        {
            "type": "text",
            "text": {
                "content": "This is a test comment.",
                "link": None,
            },
            "annotations": {
                "bold": False,
                "italic": False,
                "strikethrough": False,
                "underline": False,
                "code": False,
                "color": "default",
            },
            "plain_text": "This is a test comment.",
            "href": None,
        }
    ],
}

CHILD_BLOCK = {
    "object": "block",
    "id": "b8f5a3e8-5a8a-4ac0-9b52-9fb62772a9bd",
    "created_time": "2021-05-13T16:48:00.000Z",
    "last_edited_time": "2021-05-13T16:48:00.000Z",
    "has_children": False,
    "type": "paragraph",
    "paragraph": {
        "text": [
            {
                "type": "text",
                "text": {
                    "content": "This is a test paragraph block.",
                    "link": None,
                },
                "annotations": {
                    "bold": False,
                    "italic": False,
                    "strikethrough": False,
                    "underline": False,
                    "code": False,
                    "color": "default",
                },
                "plain_text": "This is a test paragraph block.",
                "href": None,
            }
        ]
    },
}
USER = {
    "object": "user",
    "id": "18e0bf6c-ae79-4e4e-81f5-4a8a1ebd615a",
    "name": "rj",
    "avatar_url": "null",
    "type": "person",
    "person": {"email": "rj@dummydomain.com"},
}
FILE_BLOCK = {
    "object": "block",
    "id": "974e6b14-9c7d-42b4-9506-c2928b6427a6",
    "parent": {
        "type": "page_id",
        "page_id": "0dfd97a0-aa48-4dfb-a9cc-f8af8a74dd98",
    },
    "created_time": "2024-01-29T11:33:00.000Z",
    "last_edited_time": "2024-01-29T11:33:00.000Z",
    "created_by": {
        "object": "user",
        "id": "18e0bf6c-ae79-4e4e-81f5-4a8a1ebd615a",
    },
    "last_edited_by": {
        "object": "user",
        "id": "18e0bf6c-ae79-4e4e-81f5-4a8a1ebd615a",
    },
    "has_children": "false",
    "archived": "false",
    "type": "file",
    "file": {
        "caption": [],
        "type": "file",
        "file": {
            "url": "https//notion-file-url",
            "expiry_time": "2024-01-29T12:56:00.577Z",
        },
        "name": "Packers.pdf",
    },
}

URL = "https://some_example_file_url"


@pytest.mark.asyncio
@patch("connectors.sources.notion.NotionClient", autospec=True)
async def test_ping(mock_notion_client):
    mock_notion_client.return_value.fetch_owner.return_value = None
    async with create_source(
        NotionDataSource,
        notion_secret_key="1234",
    ) as source:
        await source.ping()
        assert mock_notion_client.called


@pytest.mark.asyncio
@patch("connectors.sources.notion.NotionClient", autospec=True)
async def test_ping_negative(mock_notion_client):
    mock_notion_client.return_value.fetch_owner.side_effect = APIResponseError(
        message="Invalid API key",
        code=401,
        response=Response(status_code=401, text="Unauthorized"),
    )

    async with create_source(
        NotionDataSource,
        notion_secret_key="5678",
        databases=["Database1"],
        pages=["Page1"],
    ) as source:
        with pytest.raises(Exception) as exc_info:
            await source.ping()

        assert "Invalid API key" in str(exc_info.value)


@pytest.mark.asyncio
async def test_close_with_client():
    async with create_source(
        NotionDataSource,
        notion_secret_key="5678",
        databases=["Database1"],
        pages=["Page1"],
    ) as source:
        _ = source.notion_client._get_client

        await source.close()

        assert not hasattr(source.notion_client.__dict__, "_get_client")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entity_type, entity_titles, mock_search_results",
    [
        (
            "database",
            ["My Database"],
            {"results": [{"title": [{"plain_text": "My Database"}]}]},
        ),
        (
            "page",
            ["My Page"],
            {
                "results": [
                    {
                        "properties": {
                            "title": {"title": [{"text": {"content": "My Page"}}]}
                        }
                    }
                ]
            },
        ),
    ],
)
@patch("connectors.sources.notion.NotionClient", autospec=True)
async def test_remote_validation(
    mock_notion_client, entity_type, entity_titles, mock_search_results
):
    mock_notion_client.return_value.make_request.return_value = mock_search_results
    async with create_source(
        NotionDataSource,
        databases=["Database1"],
    ) as source:
        await source._remote_validation(entity_type, entity_titles)

    mock_notion_client.return_value.make_request.assert_called_once_with(
        "search",
        method="POST",
        body={
            "query": " ".join(entity_titles),
            "filter": {"value": entity_type, "property": "object"},
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entity_type, entity_titles, configuration_key",
    [
        ("database", ["Missing Database"], "databases"),
        ("page", ["Missing Page"], "pages"),
    ],
)
@patch("connectors.sources.notion.NotionClient")
async def test_remote_validation_entity_not_found(
    mock_notion_client, entity_type, entity_titles, configuration_key
):
    mock_search_results = {"results": []}

    async def mock_make_request(*args, **kwargs):
        return mock_search_results

    mock_notion_client.return_value.make_request.side_effect = mock_make_request

    configuration = DataSourceConfiguration({configuration_key: entity_titles})
    source = NotionDataSource(configuration=configuration)

    with pytest.raises(ConfigurableFieldValueError):
        await source._remote_validation(entity_type, entity_titles)


@pytest.mark.asyncio
async def test_make_request():
    async_client_mock = AsyncMock()
    with patch("connectors.sources.notion.AsyncClient", return_value=async_client_mock):
        configuration = DataSourceConfiguration(
            {"notion_secret_key": "test_secret_key", "retry_count": 3}
        )
        notion_client = NotionClient(configuration=configuration)
        response_mock = AsyncMock()
        async_client_mock.request.return_value = response_mock
        result = await notion_client.make_request(
            "owner", method="GET", param1="value1"
        )
        async_client_mock.request.assert_called_once_with(
            path="/users/me", method="GET", param1="value1"
        )
        assert result == response_mock


@pytest.mark.asyncio
async def test_remote_validation_exception():
    async with create_source(NotionDataSource) as source:
        with patch.object(NotionClient, "make_request", side_effect=Exception()):
            with pytest.raises(Exception):
                await source._remote_validation("pages", ["abc"])


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", MagicMock(return_value=0))
async def test_make_request_api_response_error():
    async with create_source(NotionDataSource) as source:
        source.notion_client._get_client.request = MagicMock(
            side_effect=APIResponseError(
                code=401,
                message="Invalid API key",
                response=Response(status_code=401, text="Unauthorized"),
            )
        )
        with pytest.raises(APIResponseError):
            await source.notion_client.make_request("owner", method="GET")


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", MagicMock(return_value=0))
async def test_make_request_client_error():
    async with create_source(NotionDataSource) as source:
        source.notion_client._get_client.request = MagicMock(
            side_effect=aiohttp.ClientError()
        )
        with pytest.raises(aiohttp.ClientError):
            await source.notion_client.make_request("owner", method="GET")


@pytest.mark.asyncio
async def test_get_content():
    mock_get_via_session = AsyncMock(return_value=MagicMock())
    mock_download_extract = AsyncMock(return_value=MagicMock())
    mock_file_metadata = AsyncMock(return_value=MagicMock())
    async with create_source(NotionDataSource) as source:
        with patch.object(
            NotionDataSource, "get_file_metadata", mock_file_metadata
        ), patch.object(
            NotionDataSource, "can_file_be_downloaded", return_value=True
        ), patch.object(
            NotionDataSource, "download_and_extract_file", mock_download_extract
        ):
            await source.get_content(FILE_BLOCK, URL)
            assert mock_get_via_session.called_once_with(url=URL)


@pytest.mark.asyncio
async def test_get_file_metadata():
    async with create_source(
        NotionDataSource,
        notion_secret_key="test_get_file_metadata_key",
    ) as source:
        with patch("aiohttp.ClientSession.get") as mock_get:
            mock_response = Mock()
            mock_response.status = 200

            mock_response.content_length = 2048
            mock_response.url = Mock()
            mock_response.url.path = "http://example.com/some_file.ext"
            mock_get.return_value.__aenter__.return_value = mock_response
            attachment_metadata = {}
            await source.get_file_metadata(
                attachment_metadata=attachment_metadata, file_url="some_file_url"
            )
            assert attachment_metadata["size"] == 2048
            assert attachment_metadata["extension"] == ".ext"


@pytest.mark.asyncio
async def test_get_docs():
    expected_responses_ids = [
        USER.get("id"),
        BLOCK.get("id"),
        CHILD_BLOCK.get("id"),
        FILE_BLOCK.get("id"),
        COMMENT.get("id"),
    ]
    async with create_source(NotionDataSource) as source:
        source.notion_client.fetch_users = AsyncIterator([USER])
        source.notion_client.fetch_by_query = AsyncIterator([BLOCK])
        source.notion_client.fetch_child_blocks = AsyncIterator(
            [CHILD_BLOCK, FILE_BLOCK]
        )
        source.notion_client.fetch_comments = AsyncIterator([COMMENT])
        source.pages = ["Test_DB_page"]
        source.index_comments = True
        response_ids = []
        async for result, _ in source.get_docs():
            response_ids.append(result.get("id"))
        assert response_ids == expected_responses_ids


def test_generate_query():
    configuration = DataSourceConfiguration({})
    source = NotionDataSource(configuration=configuration)
    source.pages = ["page1", "*"]
    source.databases = ["db1", "*"]
    result = list(source.generate_query())
    expected_output = [
        {"query": "page1", "filter": {"value": "page", "property": "object"}},
        {"query": "", "filter": {"value": "page", "property": "object"}},
        {"query": "db1", "filter": {"value": "database", "property": "object"}},
        {"query": "", "filter": {"value": "database", "property": "object"}},
    ]
    assert result == expected_output


@pytest.mark.asyncio
async def test_fetch_users():
    async with create_source(
        NotionDataSource,
        notion_secret_key="test_fetch_users_key",
    ) as source:
        with patch(
            "connectors.sources.notion.async_iterate_paginated_api",
            return_value=AsyncIterator([USER]),
        ):
            async for user in source.notion_client.fetch_users():
                assert user == USER


@pytest.mark.asyncio
async def test_fetch_child_blocks():
    async with create_source(
        NotionDataSource,
        notion_secret_key="test_fetch_child_blocks_key",
    ) as source:
        with patch(
            "connectors.sources.notion.async_iterate_paginated_api",
            return_value=AsyncIterator([CHILD_BLOCK]),
        ):
            async for block in source.notion_client.fetch_child_blocks("some_block_id"):
                assert block["object"] == "block"
                assert block["type"] == "paragraph"
                assert (
                    block["paragraph"]["text"][0]["plain_text"]
                    == "This is a test paragraph block."
                )


@pytest.mark.asyncio
async def test_fetch_comments():
    async with create_source(
        NotionDataSource,
        notion_secret_key="test_fetch_comments_key",
    ) as source:
        with patch(
            "connectors.sources.notion.async_iterate_paginated_api",
            return_value=AsyncIterator([COMMENT]),
        ):
            async for comment in source.notion_client.fetch_comments("some_page_id"):
                assert comment["object"] == "comment"
                assert (
                    comment["rich_text"][0]["plain_text"] == "This is a test comment."
                )


@pytest.mark.asyncio
async def test_fetch_by_query():
    async with create_source(
        NotionDataSource,
        notion_secret_key="test_fetch_by_query_key",
    ) as source:
        with patch(
            "connectors.sources.notion.async_iterate_paginated_api",
            return_value=AsyncIterator([BLOCK]),
        ):
            async for page in source.notion_client.fetch_by_query(
                {"query_string": "abc"}
            ):
                assert page["object"] == "page"
                assert (
                    page["properties"]["Name"]["title"][0]["plain_text"]
                    == "This is a test page."
                )


@pytest.mark.asyncio
async def test_query_database():
    async with create_source(
        NotionDataSource,
        notion_secret_key="test_query_database_key",
    ) as source:
        with patch(
            "connectors.sources.notion.async_iterate_paginated_api",
            return_value=AsyncIterator([DATABASE]),
        ):
            async for database in source.notion_client.query_database(
                "some_database_id"
            ):
                assert database["object"] == "database"
                assert database["title"][0]["plain_text"] == "This is a test database."


@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
@pytest.mark.asyncio
async def test_get_via_session_client_response_error():
    async with create_source(
        NotionDataSource,
        notion_secret_key="test_get_via_session_client_response_error_key",
    ) as source:
        with patch("aiohttp.ClientSession.get") as mock_get:
            mock_get.return_value.__aenter__.side_effect = aiohttp.ClientResponseError(
                request_info=Mock(),
                history=Mock(),
                status=404,
                message="Not Found",
                headers=Mock(),
            )
            with pytest.raises(NotFound):
                await anext(
                    source.notion_client.get_via_session("some_nonexistent_file_url")
                )
