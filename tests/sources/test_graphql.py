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
    headers=None, graphql_variables=None, graphql_query="{users {name {firstName } } }"
):
    async with create_source(
        GraphQLDataSource,
        http_endpoint="http://127.0.0.1:1234",
        authentication_method="none",
        graphql_query=graphql_query,
        object_list=["users", "name"],
        headers=headers,
        graphql_variables=graphql_variables,
    ) as source:
        yield source


@pytest.mark.asyncio
async def test_get():
    async with create_graphql_source() as source:
        source.graphql_client._get_session.get = Mock(
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
        source.graphql_client._get_session.get = Mock(
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
        source.graphql_client._get_session.post = Mock(
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
        source.graphql_client._get_session.post = Mock(
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
        source.graphql_client._get_session.post = Mock(
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
        source.graphql_client._get_session.post = Mock(
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
    expected_respones = [{"name": {"firstName": "xyz"}}, {"firstName": "xyz"}]
    actual_response = []
    async with create_graphql_source() as source:
        source.graphql_client.post = AsyncMock(
            return_value={"data": {"users": [{"name": {"firstName": "xyz"}}]}}
        )
        async for doc in source.fetch_data("{users {name}}}"):
            actual_response.append(doc)
    assert actual_response == expected_respones


@pytest.mark.asyncio
@freeze_time("2024-01-24T04:07:19")
async def test_get_docs():
    expected_respones = [
        {
            "name": "xyz",
            "_id": "c4ca4238a0b923820dcc509a6f75849b",
            "_timestamp": "2024-01-24T04:07:19+00:00",
        },
        {
            "name": "pqr",
            "_id": "c81e728d9d4c2f636f067f89cc14862c",
            "_timestamp": "2024-01-24T04:07:19+00:00",
        },
    ]
    actual_response = []
    async with create_graphql_source() as source:
        source.fetch_data = AsyncIterator([{"name": "xyz"}, {"name": "pqr"}])
        async for doc, _ in source.get_docs():
            actual_response.append(doc)

    assert actual_response == expected_respones
