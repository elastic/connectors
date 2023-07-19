#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the OneDrive source class methods"""
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp.client_exceptions import ClientResponseError

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.onedrive import (
    AccessToken,
    OneDriveClient,
    OneDriveDataSource,
    TokenRetrievalError,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source


def token_retrieval_errors(message, error_code):
    error = ClientResponseError(None, None)
    error.status = error_code
    error.message = message
    return error


class JSONAsyncMock(AsyncMock):
    def __init__(self, json, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._json = json

    async def json(self):
        return self._json


def test_get_configuration():
    config = DataSourceConfiguration(
        config=OneDriveDataSource.get_default_configuration()
    )

    assert config["client_id"] == "client#123"


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
    source = create_source(OneDriveDataSource, **extras)

    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_close_with_client_session():
    source = create_source(OneDriveDataSource)
    source.onedrive_client.access_token = "dummy"

    await source.close()

    assert not hasattr(source.onedrive_client.__dict__, "_get_session")


@pytest.mark.asyncio
async def test_set_access_token():
    source = create_source(OneDriveDataSource)
    mock_token = {"access_token": "msgraphtoken", "expires_in": "1234555"}
    async_response = AsyncMock()
    async_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(mock_token, 200))

    with patch("aiohttp.request", return_value=async_response):
        await source.onedrive_client.token._set_access_token()

        assert source.onedrive_client.token.access_token == "msgraphtoken"


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    source = create_source(OneDriveDataSource)
    DUMMY_RESPONSE = {}
    source.onedrive_client.api_call = AsyncIterator([[DUMMY_RESPONSE]])

    await source.ping()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_for_failed_connection_exception(mock_get):
    source = create_source(OneDriveDataSource)

    with patch.object(
        OneDriveClient, "api_call", side_effect=Exception("Something went wrong")
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
