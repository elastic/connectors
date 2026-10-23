#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the GraphQL source class methods"""

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest
from aiohttp.client_exceptions import ClientResponseError
from connectors_sdk.source import ConfigurableFieldValueError
from freezegun import freeze_time
from graphql import parse

from connectors.sources.graphql import GraphQLDataSource
from connectors.sources.graphql.client import UnauthorizedException
from tests.commons import AsyncIterator
from tests.sources.support import create_source


class JSONAsyncMock(AsyncMock):
    def __init__(self, json, status, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._json = json
        self.status = status

    async def json(self):
        return self._json


def get_json_mock(mock_response, status):
    async_mock = AsyncMock()
    async_mock.__aenter__ = AsyncMock(
        return_value=JSONAsyncMock(json=mock_response, status=status)
    )
    return async_mock


@asynccontextmanager
async def create_graphql_source(
    headers=None,
    graphql_variables=None,
    graphql_query="{users {name {firstName } } }",
    graphql_object_to_id_map='{"users": "id"}',
):
    mock_session = AsyncMock()
    mock_session.close = AsyncMock()
    with patch(
        "connectors.sources.graphql.client.aiohttp.ClientSession",
        return_value=mock_session,
    ):
        async with create_source(
            GraphQLDataSource,
            http_endpoint="http://127.0.0.1:1234",
            authentication_method="none",
            graphql_query=graphql_query,
            graphql_object_to_id_map=graphql_object_to_id_map,
            headers=headers,
            graphql_variables=graphql_variables,
        ) as source:
            yield source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "object_list, data, expected_result",
    [
        (
            {"basicInfo": "id"},
            {"basicInfo": {"name": "xyz", "id": "abc#123"}},
            [{"name": "xyz", "id": "abc#123", "_id": "abc#123"}],
        ),
        (
            {"basicInfo": "id"},
            {
                "basicInfo": [
                    {"name": "xyz", "id": "abc#123"},
                    {"name": "pqr", "id": "pqr#456"},
                ]
            },
            [
                {"name": "xyz", "id": "abc#123", "_id": "abc#123"},
                {"name": "pqr", "id": "pqr#456", "_id": "pqr#456"},
            ],
        ),
        (
            {"empData.basicInfo": "id"},
            {"empData": {"basicInfo": {"name": "xyz", "id": "abc#123"}}},
            [{"id": "abc#123", "name": "xyz", "_id": "abc#123"}],
        ),
    ],
)
async def test_extract_graphql_data_items(object_list, data, expected_result):
    actual_response = []
    async with create_graphql_source() as source:
        source.graphql_client.graphql_object_to_id_map = object_list
        for doc in source.graphql_client.extract_graphql_data_items(data):
            actual_response.append(doc)
        assert actual_response == expected_result


@pytest.mark.asyncio
async def test_get():
    async with create_graphql_source() as source:
        source.graphql_client.session.get = Mock(
            return_value=get_json_mock(
                mock_response={"data": {"users": "xyz"}}, status=200
            )
        )
        data = await source.graphql_client.get("query")
        assert data == {"users": "xyz"}


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_with_errors():
    async with create_graphql_source() as source:
        source.graphql_client.session.get = Mock(
            return_value=get_json_mock(
                mock_response={
                    "errors": [{"type": "QUERY", "message": "Invalid query"}]
                },
                status=200,
            )
        )
        with pytest.raises(Exception):
            await source.graphql_client.get("query")


@pytest.mark.asyncio
async def test_post():
    async with create_graphql_source() as source:
        source.graphql_client.session.post = Mock(
            return_value=get_json_mock(
                mock_response={"data": {"users": "xyz"}}, status=200
            )
        )
        data = await source.graphql_client.post("query")
        assert data == {"users": "xyz"}


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_post_with_errors():
    async with create_graphql_source() as source:
        source.graphql_client.session.post = Mock(
            return_value=get_json_mock(
                mock_response={
                    "errors": [{"type": "QUERY", "message": "Invalid query"}]
                },
                status=200,
            )
        )
        with pytest.raises(Exception):
            await source.graphql_client.post("query")


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_make_request_with_unauthorized():
    async with create_graphql_source() as source:
        source.graphql_client.session.post = Mock(
            side_effect=ClientResponseError(
                status=401,
                request_info=aiohttp.RequestInfo(
                    real_url="", method=None, headers=None, url=""
                ),
                history=None,
            )
        )
        with pytest.raises(UnauthorizedException):
            await source.graphql_client.make_request("QUERY")


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_make_request_with_429_exception():
    async with create_graphql_source() as source:
        source.graphql_client.session.post = Mock(
            side_effect=ClientResponseError(
                status=429,
                request_info=aiohttp.RequestInfo(
                    real_url="", method=None, headers=None, url=""
                ),
                history=None,
            )
        )
        with pytest.raises(Exception):
            await source.graphql_client.make_request("query")


@pytest.mark.asyncio
async def test_validate_config_with_invalid_url():
    async with create_graphql_source() as source:
        source.graphql_client.url = "dummy_url"
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_mutation():
    async with create_graphql_source(
        graphql_query="""mutation {
                    addCategory(id: 6, name: "Green Fruits", products: [8, 2, 3]) {
                        name
                        products {
                        name
                        }
                    }
                    }"""
    ) as source:
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_non_json_headers():
    async with create_graphql_source(headers="Invalid Headers") as source:
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_non_json_variables():
    async with create_graphql_source(graphql_variables="Invalid Variables") as source:
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_ping():
    async with create_graphql_source() as source:
        source.graphql_client.post = AsyncMock()
        await source.ping()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_ping_negative():
    async with create_graphql_source() as source:
        source.graphql_client.post = AsyncMock(side_effect=Exception())
        with pytest.raises(Exception):
            await source.ping()


@pytest.mark.asyncio
async def test_fetch_data():
    expected_response = [{"id": "1", "name": {"firstName": "xyz"}, "_id": "1"}]
    actual_response = []
    async with create_graphql_source() as source:
        source.graphql_client.pagination_model = "no_pagination"
        source.graphql_client.graphql_object_to_id_map = {"users": "id"}
        source.graphql_client.post = AsyncMock(
            return_value={
                "users": [{"id": "1", "name": {"firstName": "xyz"}, "_id": "1"}]
            }
        )
        async for doc in source.fetch_data("{users {id name}}"):
            actual_response.append(doc)
    assert actual_response == expected_response


@pytest.mark.asyncio
async def test_fetch_data_with_pagination():
    expected_response = [
        {
            "id": "1",
            "name": {"firstName": "xyz"},
            "pageInfo": {"hasNextPage": True, "endCursor": "xyz#123"},
            "_id": "1",
        },
        {
            "id": "2",
            "name": {"firstName": "abc"},
            "pageInfo": {"hasNextPage": False, "endCursor": "pqr#123"},
            "_id": "2",
        },
    ]
    actual_response = []
    async with create_graphql_source() as source:
        source.graphql_client.pagination_model = "cursor_pagination"
        source.graphql_client.graphql_object_to_id_map = {"users": "id"}
        source.graphql_client.pagination_key = "users"
        source.graphql_client.post = AsyncMock(
            side_effect=[
                {
                    "users": {
                        "id": "1",
                        "name": {"firstName": "xyz"},
                        "pageInfo": {"hasNextPage": True, "endCursor": "xyz#123"},
                        "_id": "1",
                    }
                },
                {
                    "users": {
                        "id": "2",
                        "name": {"firstName": "abc"},
                        "pageInfo": {"hasNextPage": False, "endCursor": "pqr#123"},
                        "_id": "2",
                    }
                },
            ]
        )
        async for doc in source.fetch_data(
            "query($afterCursor: String!) {users(first:5, after:$afterCursor) {name pageInfo}}"
        ):
            actual_response.append(doc)
    assert actual_response == expected_response


@pytest.mark.asyncio
async def test_fetch_data_with_pagination_when_a_nested_alias_reuses_the_key():
    # `issues: title` reuses the response key of the paginated field one level
    # down; it must not hide the arguments of `repository.issues`.
    actual_response = []
    async with create_graphql_source() as source:
        source.graphql_client.pagination_model = "cursor_pagination"
        source.graphql_client.graphql_object_to_id_map = {
            "repository.issues.nodes": "id"
        }
        source.graphql_client.pagination_key = "repository.issues"
        source.graphql_client.post = AsyncMock(
            side_effect=[
                {
                    "repository": {
                        "issues": {
                            "nodes": [{"id": "1", "issues": "first"}],
                            "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
                        }
                    }
                },
                {
                    "repository": {
                        "issues": {
                            "nodes": [{"id": "2", "issues": "second"}],
                            "pageInfo": {"hasNextPage": False, "endCursor": "c2"},
                        }
                    }
                },
            ]
        )
        async for doc in source.fetch_data(
            "query($cursor: String) {repository {issues(first: 5, after: $cursor) "
            "{nodes {id issues: title} pageInfo {hasNextPage endCursor}}}}"
        ):
            actual_response.append(doc["_id"])
        assert source.graphql_client.variables == {"cursor": "c1"}
    assert actual_response == ["1", "2"]


@pytest.mark.asyncio
async def test_fetch_data_with_pagination_requires_page_info_under_the_key():
    async with create_graphql_source() as source:
        source.graphql_client.pagination_model = "cursor_pagination"
        source.graphql_client.pagination_key = "repository.issues"
        source.graphql_client.post = AsyncMock()
        with pytest.raises(ConfigurableFieldValueError):
            async for _ in source.fetch_data(
                "query($cursor: String) {repository {issues(first: 5, after: $cursor) "
                "{nodes {id}}} pageInfo {hasNextPage endCursor}}"
            ):
                pass
        source.graphql_client.post.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_data_with_pagination_on_aliased_field():
    actual_response = []
    async with create_graphql_source() as source:
        source.graphql_client.pagination_model = "cursor_pagination"
        source.graphql_client.graphql_object_to_id_map = {"people": "id"}
        source.graphql_client.pagination_key = "people"
        source.graphql_client.post = AsyncMock(
            side_effect=[
                {
                    "people": {
                        "id": "1",
                        "pageInfo": {"hasNextPage": True, "endCursor": "xyz#123"},
                    }
                },
                {
                    "people": {
                        "id": "2",
                        "pageInfo": {"hasNextPage": False, "endCursor": "pqr#123"},
                    }
                },
            ]
        )
        async for doc in source.fetch_data(
            "query($afterCursor: String!) {people: users(first:5, after:$afterCursor) {id pageInfo}}"
        ):
            actual_response.append(doc["_id"])
        assert source.graphql_client.variables == {"afterCursor": "xyz#123"}
    assert actual_response == ["1", "2"]


@pytest.mark.asyncio
async def test_get_sends_variables():
    async with create_graphql_source() as source:
        source.graphql_client.variables = {"id": "123"}
        source.graphql_client.session.get = Mock(
            return_value=get_json_mock(
                mock_response={"data": {"users": "xyz"}}, status=200
            )
        )
        await source.graphql_client.get("query")

        params = source.graphql_client.session.get.call_args.kwargs["params"]
        assert params == {"query": "query", "variables": json.dumps({"id": "123"})}


@pytest.mark.asyncio
async def test_fetch_data_with_pagination_over_get():
    query = "query($afterCursor: String) {users(first:5, after:$afterCursor) {name pageInfo}}"
    async with create_graphql_source() as source:
        source.graphql_client.http_method = "get"
        source.graphql_client.pagination_model = "cursor_pagination"
        source.graphql_client.graphql_object_to_id_map = {"users": "id"}
        source.graphql_client.pagination_key = "users"
        source.graphql_client.session.get = Mock(
            side_effect=[
                get_json_mock(
                    mock_response={
                        "data": {
                            "users": {
                                "id": "1",
                                "pageInfo": {
                                    "hasNextPage": True,
                                    "endCursor": "xyz#123",
                                },
                            }
                        }
                    },
                    status=200,
                ),
                get_json_mock(
                    mock_response={
                        "data": {
                            "users": {
                                "id": "2",
                                "pageInfo": {
                                    "hasNextPage": False,
                                    "endCursor": "pqr#123",
                                },
                            }
                        }
                    },
                    status=200,
                ),
            ]
        )

        docs = [doc async for doc in source.fetch_data(query)]

        assert [doc["_id"] for doc in docs] == ["1", "2"]
        first_call, second_call = source.graphql_client.session.get.call_args_list
        assert first_call.kwargs["params"] == {"query": query}
        assert second_call.kwargs["params"] == {
            "query": query,
            "variables": json.dumps({"afterCursor": "xyz#123"}),
        }


@pytest.mark.asyncio
async def test_fetch_data_without_pageinfo():
    async with create_graphql_source() as source:
        source.graphql_client.pagination_model = "cursor_pagination"
        source.graphql_client.graphql_object_to_id_map = {"users": id}
        source.graphql_client.post = AsyncMock(
            side_effect=[{"users": {"id": "123", "name": {"firstName": "xyz"}}}]
        )

        with pytest.raises(ConfigurableFieldValueError):
            async for _doc in source.fetch_data("{users {id name pageInfo}}"):
                pass


@pytest.mark.asyncio
async def test_teardown_does_not_use_real_client_session():
    """Regression: source teardown must not instantiate a real aiohttp.ClientSession."""
    with patch("aiohttp.ClientSession") as real_client_session:
        async with create_graphql_source():
            pass

        real_client_session.assert_not_called()


@pytest.mark.asyncio
@freeze_time("2024-01-24T04:07:19")
async def test_get_docs():
    expected_response = [
        {
            "name": "xyz",
            "id": "id1",
            "_id": "id1",
            "_timestamp": "2024-01-24T04:07:19+00:00",
        },
        {
            "name": "pqr",
            "id": "id2",
            "_id": "id2",
            "_timestamp": "2024-01-24T04:07:19+00:00",
        },
        {
            "name": "abc",
            "id": "id3",
            "_id": "id3",
            "_timestamp": "2024-01-24T04:07:19+00:00",
        },
    ]
    actual_response = []
    async with create_graphql_source() as source:
        source.graphql_client.graphql_field_id = "id"
        source.fetch_data = AsyncIterator(
            [
                {"name": "xyz", "id": "id1", "_id": "id1"},
                {"name": "pqr", "id": "id2", "_id": "id2"},
                {"name": "abc", "id": "id3", "_id": "id3"},
            ]
        )
        async for doc, _ in source.get_docs():
            actual_response.append(doc)
    assert actual_response == expected_response


@pytest.mark.asyncio
@freeze_time("2024-01-24T04:07:19")
async def test_get_docs_with_dict_id():
    async with create_graphql_source() as source:
        source.fetch_data = AsyncIterator(
            [
                {"name": "xyz", "_id": {"sys_id": 1}},
            ]
        )
        with pytest.raises(ConfigurableFieldValueError):
            async for _, _ in source.get_docs():
                pass


@pytest.mark.asyncio
async def test_extract_graphql_data_items_with_invalid_key():
    async with create_graphql_source() as source:
        source.graphql_client.graphql_object_to_id_map = {"user": "id"}
        data = {"users": {"namexyzid": "123"}}
        with pytest.raises(ConfigurableFieldValueError):
            for _ in source.graphql_client.extract_graphql_data_items(data):
                pass


@pytest.mark.asyncio
async def test_extract_pagination_info_with_invalid_key():
    async with create_graphql_source() as source:
        source.pagination_key = ["users_data.user"]
        data = {"users_data": {"users": {"namexyzid": "123"}}}
        with pytest.raises(ConfigurableFieldValueError):
            for _ in source.graphql_client.extract_pagination_info(data):
                pass


@pytest.mark.asyncio
async def test_is_query_with_mutation_query():
    async with create_graphql_source() as source:
        ast = parse("mutation Login($email: String!){login(email: $email) { token }}")
        response = source.is_query(ast)
        assert response is False


@pytest.mark.asyncio
async def test_is_query_with_invalid_query():
    async with create_graphql_source() as source:
        source.graphql_client.graphql_query = "invalid_query {user {}}"
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_invalid_objects():
    async with create_graphql_source() as source:
        source.graphql_client.graphql_query = (
            "query {organization {repository { issues {name}}}}"
        )
        source.graphql_client.graphql_object_to_id_map = {
            "organization.repository.pullRequest": "id"
        }
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_invalid_pagination_key():
    async with create_graphql_source(
        graphql_object_to_id_map='{"organization.repository.issues": "id"}'
    ) as source:
        source.graphql_client.graphql_query = (
            "query {organization {repository { issues {id name}}}}"
        )
        source.graphql_client.pagination_model = "cursor_pagination"
        source.graphql_client.pagination_key = "organization.repository.pullRequest"
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


def fragment_chain(depth, leaf):
    # Each fragment spreads the next one twice, so the query grows linearly with
    # the depth while a naive expansion visits 2 ** depth copies of the leaf.
    chain = "".join(
        f"fragment F{i} on Query {{...F{i + 1} ...F{i + 1}}} " for i in range(depth)
    )
    return f"query {{...F0}} {chain}fragment F{depth} on Query {{{leaf}}}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "leaf, valid",
    [
        ('search(query: "is:issue", type: ISSUE, first: 10) {nodes {id}}', True),
        ('search(query: "is:issue", type: ISSUE, first: 10) {nodes {title}}', False),
    ],
)
async def test_validate_config_expands_each_fragment_once(leaf, valid):
    async with create_graphql_source(
        graphql_query=fragment_chain(40, leaf),
        graphql_object_to_id_map='{"search.nodes": "id"}',
    ) as source:
        if valid:
            await source.validate_config()
        else:
            with pytest.raises(ConfigurableFieldValueError):
                await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_missing_config_field():
    async with create_graphql_source(
        graphql_object_to_id_map='{"organization.repository.issues": "id"}'
    ) as source:
        source.graphql_client.graphql_query = (
            "query {organization {repository { issues {name}}}}"
        )
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_invalid_json():
    async with create_graphql_source(
        graphql_object_to_id_map='{"organization.repository.issues": "id"'
    ) as source:
        source.graphql_client.graphql_query = (
            "query {organization {repository { issues {name}}}}"
        )
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "graphql_query, graphql_object_to_id_map",
    [
        (
            # inline fragment under the mapped object
            'query {search(query: "is:issue", type: ISSUE, first: 10) {nodes {... on Issue {id title}}}}',
            '{"search.nodes": "id"}',
        ),
        (
            # named fragment spread under the mapped object
            'query {search(query: "is:issue", type: ISSUE, first: 10) {nodes {...IssueFields}}} fragment IssueFields on Issue {id title}',
            '{"search.nodes": "id"}',
        ),
        (
            # fragment definition before the query
            'fragment IssueFields on Issue {id title} query {search(query: "is:issue", type: ISSUE, first: 10) {nodes {...IssueFields}}}',
            '{"search.nodes": "id"}',
        ),
        (
            # aliased field on the path; the response is keyed by the alias
            'query {repo: repository(owner: "elastic", name: "connectors") {issues(first: 10) {nodes {id}}}}',
            '{"repo.issues.nodes": "id"}',
        ),
        (
            # the id comes from a second selection of the same field
            'query {search(query: "is:issue", type: ISSUE, first: 10) {nodes {title}} ...SearchIds} fragment SearchIds on Query {search(query: "is:issue", type: ISSUE, first: 10) {nodes {id}}}',
            '{"search.nodes": "id"}',
        ),
    ],
)
async def test_validate_config_with_fragments_and_aliases(
    graphql_query, graphql_object_to_id_map
):
    async with create_graphql_source(
        graphql_query=graphql_query,
        graphql_object_to_id_map=graphql_object_to_id_map,
    ) as source:
        await source.validate_config()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "graphql_query, graphql_object_to_id_map",
    [
        (
            # the inline fragment does not select the id
            'query {search(query: "is:issue", type: ISSUE, first: 10) {nodes {... on Issue {title}}}}',
            '{"search.nodes": "id"}',
        ),
        (
            # the path uses the field name, but the response is keyed by the alias
            'query {repo: repository(owner: "elastic", name: "connectors") {issues(first: 10) {nodes {id}}}}',
            '{"repository.issues.nodes": "id"}',
        ),
        (
            # a fragment that spreads itself is not followed forever
            'query {search(query: "is:issue", type: ISSUE, first: 10) {nodes {...Loop}}} fragment Loop on Issue {title ...Loop}',
            '{"search.nodes": "id"}',
        ),
    ],
)
async def test_validate_config_with_fragments_and_aliases_missing_id(
    graphql_query, graphql_object_to_id_map
):
    async with create_graphql_source(
        graphql_query=graphql_query,
        graphql_object_to_id_map=graphql_object_to_id_map,
    ) as source:
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()
