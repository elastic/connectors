#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the GraphQL source class methods"""
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import pytest
from aiohttp.client_exceptions import ClientResponseError
from freezegun import freeze_time
from graphql import parse

from connectors.source import ConfigurableFieldValueError
from connectors.sources.graphql import GraphQLDataSource, UnauthorizedException
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
    graphql_object_list='{"users": "id"}',
):
    async with create_source(
        GraphQLDataSource,
        http_endpoint="http://127.0.0.1:1234",
        authentication_method="none",
        graphql_query=graphql_query,
        graphql_object_list=graphql_object_list,
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
        source.graphql_client.graphql_object_list = object_list
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
        source.graphql_client.graphql_object_list = {"users": "id"}
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
        source.graphql_client.graphql_object_list = {"users": "id"}
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
async def test_fetch_data_without_pageinfo():
    async with create_graphql_source() as source:
        source.graphql_client.pagination_model = "cursor_pagination"
        source.graphql_client.graphql_object_list = {"users": id}
        source.graphql_client.post = AsyncMock(
            side_effect=[{"users": {"id": "123", "name": {"firstName": "xyz"}}}]
        )

        with pytest.raises(ConfigurableFieldValueError):
            async for _doc in source.fetch_data("{users {id name pageInfo}}"):
                pass


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
        source.graphql_client.graphql_object_list = {"user": "id"}
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
        source.graphql_client.graphql_object_list = {
            "organization.repository.pullRequest": "id"
        }
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_invalid_pagination_key():
    async with create_graphql_source(
        graphql_object_list='{"organization.repository.issues": "id"}'
    ) as source:
        source.graphql_client.graphql_query = (
            "query {organization {repository { issues {id name}}}}"
        )
        source.graphql_client.pagination_model = "cursor_pagination"
        source.graphql_client.pagination_key = "organization.repository.pullRequest"
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_missing_config_field():
    async with create_graphql_source(
        graphql_object_list='{"organization.repository.issues": "id"}'
    ) as source:
        source.graphql_client.graphql_query = (
            "query {organization {repository { issues {name}}}}"
        )
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_invalid_json():
    async with create_graphql_source(
        graphql_object_list='{"organization.repository.issues": "id"'
    ) as source:
        source.graphql_client.graphql_query = (
            "query {organization {repository { issues {name}}}}"
        )
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()
