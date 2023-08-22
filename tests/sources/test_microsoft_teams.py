from unittest.mock import MagicMock, Mock, patch

import pytest
import pytest_asyncio
from aiohttp.client_exceptions import ClientOSError, ClientResponseError

from connectors.logger import logger
from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.microsoft_teams import (
    GraphAPIToken,
    InternalServerError,
    MicrosoftTeamsClient,
    MicrosoftTeamsDataSource,
    NotFound,
    PermissionsMissing,
)
from tests.sources.support import create_source


class StubAPIToken:
    async def get_with_username_password(self):
        return "something"


@pytest_asyncio.fixture
async def microsoft_client():
    yield MicrosoftTeamsClient(None, None, None, None, None)


class ClientErrorException:
    real_url = ""


class ClientSession:
    """Mock Client Session Class"""

    async def close(self):
        """Close method of Mock Client Session Class"""
        pass


async def create_fake_coroutine(item):
    """create a method for returning fake coroutine value for
    Args:
        item: Value for converting into coroutine
    """
    return item


def test_get_configuration():
    config = DataSourceConfiguration(
        config=MicrosoftTeamsDataSource.get_default_configuration()
    )

    assert config["client_id"] == ""


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
async def test_validate_configuration_with_invalid_fields_raises_error(
    extras,
):
    async with create_source(MicrosoftTeamsDataSource, **extras) as source:
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        async with create_source(MicrosoftTeamsDataSource) as source:
            DUMMY_RESPONSE = {}
            source.client.fetch = Mock(
                return_value=create_fake_coroutine(item=DUMMY_RESPONSE)
            )
            await source.ping()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_for_failed_connection_exception(mock_get):
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        async with create_source(MicrosoftTeamsDataSource) as source:
            with patch.object(
                MicrosoftTeamsClient,
                "fetch",
                side_effect=Exception("Something went wrong"),
            ):
                with pytest.raises(Exception):
                    await source.ping()


@pytest.mark.asyncio
async def test_set_internal_logger():
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source._set_internal_logger()
        assert source.client._logger == logger


@pytest.mark.asyncio
async def test_call_api_with_403(
    microsoft_client,
    mock_responses,
    patch_sleep,
    patch_cancellable_sleeps,
):
    url = "http://localhost:1234/download-some-sample-file"

    unauthorized_error = ClientResponseError(None, None)
    unauthorized_error.status = 403
    unauthorized_error.message = "Something went wrong"

    mock_responses.get(url, exception=unauthorized_error)
    mock_responses.get(url, exception=unauthorized_error)
    mock_responses.get(url, exception=unauthorized_error)
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        with pytest.raises(PermissionsMissing) as e:
            async for _ in microsoft_client._get(url):
                pass

        assert e is not None


@pytest.mark.asyncio
async def test_call_api_with_404(
    microsoft_client,
    mock_responses,
    patch_sleep,
    patch_cancellable_sleeps,
):
    url = "http://localhost:1234/download-some-sample-file"

    not_found_error = ClientResponseError(None, None)
    not_found_error.status = 404
    not_found_error.message = "Something went wrong"

    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)

    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        with pytest.raises(NotFound) as e:
            async for _ in microsoft_client._get(url):
                pass

        assert e is not None


@pytest.mark.asyncio
async def test_call_api_with_os_error(
    microsoft_client,
    mock_responses,
    patch_sleep,
    patch_cancellable_sleeps,
):
    url = "http://localhost:1234/download-some-sample-file"

    not_found_error = ClientOSError()
    not_found_error.message = "Something went wrong"

    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        with pytest.raises(ClientOSError) as e:
            async for _ in microsoft_client._get(url):
                pass

        assert e is not None


@pytest.mark.asyncio
async def test_call_api_with_500(
    microsoft_client,
    mock_responses,
    patch_sleep,
    patch_cancellable_sleeps,
):
    url = "http://localhost:1234/download-some-sample-file"

    not_found_error = ClientResponseError(
        history="history", request_info=ClientErrorException
    )
    not_found_error.status = 500
    not_found_error.message = "Something went wrong"

    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)

    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        with pytest.raises(InternalServerError) as e:
            async for _ in microsoft_client._get(url):
                pass

    assert e is not None


@pytest.mark.asyncio
async def test_call_api_with_unhandled_status(
    microsoft_client,
    mock_responses,
    patch_sleep,
    patch_cancellable_sleeps,
):
    url = "http://localhost:1234/download-some-sample-file"

    error_message = "Something went wrong"

    not_found_error = ClientResponseError(MagicMock(), MagicMock())
    not_found_error.status = 420
    not_found_error.message = error_message

    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)

    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        with pytest.raises(ClientResponseError) as e:
            async for _ in microsoft_client._get(url):
                pass

        assert e.match(error_message)


@pytest.mark.asyncio
async def test_get_for_username_password():
    bearer = "hello"
    source = GraphAPIToken(
        "tenant_id", "client_id", "client_secret", "username", "password"
    )
    source._fetch_token = Mock(return_value=create_fake_coroutine(("hello", 15)))
    actual_token = await source.get_with_username_password()

    assert actual_token == bearer


@pytest.mark.asyncio
async def test_fetch(microsoft_client, mock_responses):
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        url = "http://localhost:1234/url"

        response = {"displayName": "Dummy", "id": "123"}

        mock_responses.get(url, payload=response)

        fetch_response = await microsoft_client.fetch(url)
        assert response == fetch_response
