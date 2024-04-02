from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

import aiohttp
import pytest
from httpx import Response
from notion_client import APIResponseError

from connectors.filtering.validation import SyncRuleValidationResult
from connectors.protocol import Filter
from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.notion import (
    NotFound,
    NotionAdvancedRulesValidator,
    NotionClient,
    NotionDataSource,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source

ADVANCED_SNIPPET = "advanced_snippet"
DATABASE = {
    "object": "database",
    "id": "database_id",
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
CHILD_BLOCK_WITH_CHILDREN = {
    "object": "block",
    "id": "b8f5a3e8-5a8a-4ac0-9b52-9fb62772a9bd",
    "created_time": "2021-05-13T16:48:00.000Z",
    "last_edited_time": "2021-05-13T16:48:00.000Z",
    "has_children": True,
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
        ("database", ["My Database"], [{"title": [{"plain_text": "My Database"}]}]),
        (
            "page",
            ["My Page"],
            [{"properties": {"title": {"title": [{"text": {"content": "My Page"}}]}}}],
        ),
    ],
)
@patch("connectors.sources.notion.NotionClient", autospec=True)
async def test_get_entities(
    mock_notion_client, entity_type, entity_titles, mock_search_results
):
    mock_notion_client.return_value.fetch_by_query = AsyncIterator(mock_search_results)
    async with create_source(
        NotionDataSource,
        databases=["Database1"],
    ) as source:
        await source.get_entities(entity_type, entity_titles)

    mock_notion_client.return_value.fetch_by_query.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entity_type, entity_titles, configuration_key",
    [
        ("database", ["Missing Database"], "databases"),
        ("page", ["Missing Page"], "pages"),
    ],
)
@patch("connectors.sources.notion.NotionClient")
async def test_get_entities_entity_not_found(
    mock_notion_client, entity_type, entity_titles, configuration_key
):
    mock_search_results = {"results": []}

    async def mock_make_request(*args, **kwargs):
        return mock_search_results

    mock_notion_client.return_value.make_request.side_effect = mock_make_request

    configuration = DataSourceConfiguration({configuration_key: entity_titles})
    source = NotionDataSource(configuration=configuration)

    with pytest.raises(ConfigurableFieldValueError):
        await source.get_entities(entity_type, entity_titles)


@pytest.mark.asyncio
async def test_get_entities_exception():
    async with create_source(NotionDataSource) as source:
        with patch.object(NotionClient, "fetch_by_query", side_effect=Exception()):
            with pytest.raises(Exception):
                await source.get_entities("pages", ["abc"])


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
async def test_get_content_when_url_is_empty():
    async with create_source(NotionDataSource) as source:
        content = await source.get_content(FILE_BLOCK, None)
    assert content is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "file_name",
    [
        ("some.file.with.periods"),
        ("some file with spaces"),
        ("some/file/with/slashes"),
    ],
)
async def test_get_file_metadata(file_name):
    async with create_source(
        NotionDataSource,
        notion_secret_key="test_get_file_metadata_key",
    ) as source:
        with patch("aiohttp.ClientSession.get") as mock_get:
            mock_response = Mock()
            mock_response.status = 200

            mock_response.content_length = 2048
            mock_response.url = Mock()
            mock_response.url.path = "http://example.com/{file_name}.ext"
            mock_get.return_value.__aenter__.return_value = mock_response
            attachment_metadata = {}
            await source.get_file_metadata(
                attachment_metadata=attachment_metadata, file_url="some_file_url"
            )
            assert attachment_metadata["size"] == 2048
            assert attachment_metadata["extension"] == ".ext"


@pytest.mark.asyncio
async def test_retrieve_and_process_blocks():
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
        with patch.object(
            NotionClient,
            "async_iterate_paginated_api",
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
        with patch.object(
            NotionClient,
            "async_iterate_paginated_api",
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
        with patch.object(
            NotionClient,
            "async_iterate_paginated_api",
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
        with patch.object(
            NotionClient,
            "async_iterate_paginated_api",
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
        with patch.object(
            NotionClient,
            "async_iterate_paginated_api",
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


@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
@pytest.mark.asyncio
async def test_get_via_session_with_429_status():
    retried_response = AsyncMock()

    async with create_source(
        NotionDataSource,
        notion_secret_key="test_get_via_session_client_response_error_key",
    ) as source:
        with patch("aiohttp.ClientSession.get") as mock_get:
            mock_get.return_value.__aenter__.side_effect = [
                aiohttp.ClientResponseError(
                    request_info=Mock(),
                    history=Mock(),
                    status=429,
                    message="rate-limited",
                    headers={"Retry-After": 0.1},
                ),
                retried_response,
            ]
            response = await anext(
                source.notion_client.get_via_session("some_nonexistent_file_url")
            )
        assert response == retried_response


@pytest.mark.asyncio
async def test_fetch_children_recursively():
    async with create_source(
        NotionDataSource,
        notion_secret_key="test_fetch_children_recursively_key",
    ) as source:
        with patch.object(
            NotionClient,
            "async_iterate_paginated_api",
            side_effect=[
                AsyncIterator([CHILD_BLOCK_WITH_CHILDREN]),
                AsyncIterator([CHILD_BLOCK]),
            ],
        ):
            async for block in source.notion_client.fetch_child_blocks("some_block_id"):
                assert block["object"] == "block"
                assert block["type"] == "paragraph"
                assert (
                    block["paragraph"]["text"][0]["plain_text"]
                    == "This is a test paragraph block."
                )


@pytest.mark.parametrize(
    "filtering",
    [
        Filter(
            {
                ADVANCED_SNIPPET: {
                    "value": {
                        "database_query_filters": [
                            {
                                "filter": {
                                    "property": "Task completed",
                                    "title": {"contain": "John"},
                                },
                                "database_id": "database_id",
                            }
                        ],
                        "searches": [
                            {
                                "filter": {"value": "database"},
                                "query": "efghsd",
                            }
                        ],
                    }
                }
            }
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules(filtering):
    async with create_source(
        NotionDataSource,
        notion_secret_key="secert_key",
    ) as source:
        documents = []
        with patch.object(
            NotionClient, "query_database", return_value=AsyncIterator([DATABASE])
        ), patch.object(
            NotionDataSource,
            "retrieve_and_process_blocks",
            return_value=AsyncIterator([(BLOCK, None)]),
        ):
            expected_responses_ids = [DATABASE.get("id"), BLOCK.get("id")]
            async for docs, _ in source.get_docs(filtering):
                documents.append(docs.get("id"))
            assert documents == expected_responses_ids


@pytest.mark.parametrize(
    "advanced_rules, expected_validation_result",
    [
        # Valid: search query for database
        (
            {
                "searches": [
                    {
                        "filter": {"value": "database"},
                        "query": "Demo",
                    }
                ]
            },
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        # Valid: search query for page
        (
            {
                "searches": [
                    {
                        "filter": {"value": "page"},
                        "query": "Demo",
                    }
                ]
            },
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        # Invalid: provided filter value is incorrect: pages in place of page
        (
            {
                "searches": [
                    {
                        "filter": {"value": "pages"},
                        "query": "Demo",
                    }
                ]
            },
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        # Invalid: no query key
        (
            {"searches": [{"filter": {"value": "database"}}]},
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        # Valid: database query filter
        (
            {
                "database_query_filters": [
                    {
                        "filter": {"property": "Name", "title": {"contain": "John"}},
                        "database_id": "database_id",
                    }
                ]
            },
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        # Invalid: database_id
        (
            {
                "database_query_filters": [
                    {
                        "database_id": "invalid_database_id",
                    }
                ]
            },
            SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        # Invalid: empty property value
        (
            {
                "database_query_filters": [
                    {
                        "filter": {"property": "", "title": {"contain": "John"}},
                        "database_id": "database_id",
                    }
                ]
            },
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        # Valid: database query filter using or
        (
            {
                "database_query_filters": [
                    {
                        "filter": {
                            "or": [
                                {
                                    "property": "Description",
                                    "rich_text": {"contains": "2023"},
                                }
                            ]
                        },
                        "database_id": "database_id",
                    }
                ]
            },
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        # Valid: two search query
        (
            {
                "searches": [
                    {
                        "filter": {"value": "page"},
                        "query": "Test Page",
                    },
                    {
                        "filter": {"value": "database"},
                        "query": "Test Database",
                    },
                ]
            },
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_advanced_rules_validation(advanced_rules, expected_validation_result):
    async with create_source(
        NotionDataSource, notion_secret_key="secret_key"
    ) as source:
        with patch.object(
            NotionClient,
            "async_iterate_paginated_api",
            return_value=AsyncIterator([DATABASE]),
        ), patch.object(
            NotionDataSource, "get_entities", return_value=[DATABASE, BLOCK]
        ):
            validation_result = await NotionAdvancedRulesValidator(source).validate(
                advanced_rules
            )
            assert validation_result == expected_validation_result


@pytest.mark.asyncio
async def test_async_iterate_paginated_api():
    async def mock_function(**kwargs):
        return {
            "results": [{"name": "John"}, {"name": "Alice"}],
            "next_cursor": None,
            "has_more": False,
        }

    with patch("notion_client.AsyncClient") as mock_client:
        mock_client_instance = mock_client.return_value
        mock_client_instance.__aenter__.return_value = mock_client_instance
        mock_client_instance.some_function = AsyncMock(side_effect=mock_function)
        async with create_source(
            NotionDataSource, notion_secret_key="secert_key"
        ) as source:
            async for result in source.notion_client.async_iterate_paginated_api(
                mock_client_instance.some_function
            ):
                assert "name" in result
            mock_client_instance.some_function.assert_called_once()


@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
@pytest.mark.asyncio
async def test_fetch_results_rate_limit_exceeded():
    async def mock_function_with_429(**kwargs):
        mock_function_with_429.call_count += 1

        # Initial call and first retry would raise rate-limited error
        if mock_function_with_429.call_count <= 2:
            raise APIResponseError(
                code="rate_limited",
                message="Rate limit exceeded.",
                response=Response(status_code=429, text="Rate limit exceeded."),
            )
        else:
            return {"success": True}

    async with create_source(
        NotionDataSource, notion_secret_key="secret_key"
    ) as source:
        with patch("connectors.sources.notion.DEFAULT_RETRY_SECONDS", 0.3):
            mock_function_with_429.call_count = 0
            result = await source.notion_client.fetch_results(mock_function_with_429)

            # assert it responds successfully on 2nd retry and does not raise error
            assert result == {"success": True}

            # assert it did not go for 3rd retry
            assert mock_function_with_429.call_count == 3  # initial call + 2 retries


@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
@pytest.mark.asyncio
async def test_fetch_results_other_errors_not_retried():
    async def mock_function_with_other_error(**kwargs):
        raise APIResponseError(
            code="object_not_found",
            message="Object Not Found",
            response=Response(status_code=404, text="Object Not Found"),
        )

    async with create_source(
        NotionDataSource, notion_secret_key="secret_key"
    ) as source:
        with patch.object(
            source.notion_client,
            "fetch_results",
            wraps=source.notion_client.fetch_results,
        ) as mock_fetch_results:
            with pytest.raises(APIResponseError) as exc_info:
                await source.notion_client.fetch_results(mock_function_with_other_error)

            assert exc_info.value.code == "object_not_found"
            assert exc_info.value.status == 404

            # assert that fetch_results is called exactly once and not retried for status code 404
            mock_fetch_results.assert_called_once()


@pytest.mark.asyncio
async def test_original_async_iterate_paginated_api_not_called():
    with patch.object(NotionClient, "async_iterate_paginated_api"):
        with patch(
            "notion_client.helpers.async_iterate_paginated_api"
        ) as mock_async_iterate_paginated_api:
            async with create_source(
                NotionDataSource, notion_secret_key="secret_key"
            ) as source:
                async for _ in source.notion_client.query_database("database_id"):
                    assert not mock_async_iterate_paginated_api.called


@pytest.mark.asyncio
async def test_fetch_child_blocks_with_not_found_object(caplog):
    block_id = "block_id"
    caplog.set_level("WARNING")
    with patch(
        "connectors.sources.notion.NotionClient.async_iterate_paginated_api",
        side_effect=APIResponseError(
            code="object_not_found",
            message="Object Not Found",
            response=Response(status_code=404, text="Object Not Found"),
        ),
    ):
        async with create_source(
            NotionDataSource, notion_secret_key="secret_key"
        ) as source:
            async for _ in source.notion_client.fetch_child_blocks(block_id):
                assert "Object not found" in caplog.text
