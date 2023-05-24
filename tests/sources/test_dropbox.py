#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Dropbox source class methods"""
from unittest.mock import AsyncMock, patch

import pytest
from dropbox.exceptions import ApiError, AuthError, BadInputError

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.dropbox import DropboxDataSource
from tests.sources.support import create_source

PATH = "/"
DUMMY_VALUES = "abc#123"

class MockError:
    def is_path(self):
        return True

    def get_path(self):
        return LookupError()

class LookupError:
    def is_not_found(self):
        return True

@pytest.mark.asyncio
async def test_configuration():
    """Tests the get configurations method of the Dropbox source class."""
    config = DataSourceConfiguration(
        config=DropboxDataSource.get_default_configuration()
    )
    assert config["path"] == PATH
    assert config["app_key"] == DUMMY_VALUES
    assert config["app_secret"] == DUMMY_VALUES
    assert config["refresh_token"] == DUMMY_VALUES
    assert config["include_deleted_files"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    ["path", "app_key", "app_secret", "refresh_token", "include_deleted_files"],
)
async def test_validate_configuration_with_empty_fields_then_raise_exception(field):
    source = create_source(DropboxDataSource)
    source.dropbox_client.configuration.set_field(name=field, value="")

    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_for_valid_path():
    source = create_source(DropboxDataSource)
    source.dropbox_client.configuration.set_field(name="path", value="/shared")

    with patch.object(
        source.dropbox_client._create_connection,
        "files_get_metadata",
        return_value=AsyncMock(),
    ):
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_path_then_raise_exception():
    source = create_source(DropboxDataSource)
    source.dropbox_client.path = "/abc"

    with patch.object(
        source.dropbox_client._create_connection,
        "files_get_metadata",
        side_effect=ApiError(
            request_id=1,
            error=MockError(),
            user_message_text="Api Error Occurred",
            user_message_locale=None,
        ),
    ):
        with pytest.raises(
            ConfigurableFieldValueError, match="Configured Path: /abc is invalid"
        ):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_app_key_and_app_secret_then_raise_exception():
    source = create_source(DropboxDataSource)
    source.dropbox_client.path = "/abc"

    with patch.object(
        source.dropbox_client._create_connection,
        "files_get_metadata",
        side_effect=BadInputError(request_id=2, message="Bad Input Error"),
    ):
        with pytest.raises(
            ConfigurableFieldValueError,
            match="Configured App Key or App Secret is invalid",
        ):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_refresh_token_then_raise_exception():
    source = create_source(DropboxDataSource)
    source.dropbox_client.path = "/abc"

    with patch.object(
        source.dropbox_client._create_connection,
        "files_get_metadata",
        side_effect=AuthError(request_id=3, error="Auth Error"),
    ):
        with pytest.raises(
            ConfigurableFieldValueError, match="Configured Refresh Token is invalid"
        ):
            await source.validate_config()


@pytest.mark.asyncio
async def test_ping():
    source = create_source(DropboxDataSource)
    with patch.object(
        source.dropbox_client._create_connection,
        "users_get_current_account",
        return_value=AsyncMock(return_value="Mock..."),
    ):
        await source.ping()


@pytest.mark.asyncio
async def test_ping_for_failed_connection_exception_then_raise_exception():
    source = create_source(DropboxDataSource)
    with patch.object(
        source.dropbox_client._create_connection,
        "users_get_current_account",
        side_effect=Exception("Something went wrong"),
    ):
        with pytest.raises(Exception):
            await source.ping()


@pytest.mark.asyncio
async def test_ping_for_incorrect_app_key_and_app_secret_then_raise_exception():
    source = create_source(DropboxDataSource)
    with patch.object(
        source.dropbox_client._create_connection,
        "users_get_current_account",
        side_effect=BadInputError(request_id=2, message="Bad Input Error"),
    ):
        with pytest.raises(
            Exception, match="Configured App Key or App Secret is invalid"
        ):
            await source.ping()


@pytest.mark.asyncio
async def test_ping_for_incorrect_refresh_token_then_raise_exception():
    source = create_source(DropboxDataSource)
    with patch.object(
        source.dropbox_client._create_connection,
        "users_get_current_account",
        side_effect=AuthError(request_id=3, error="Auth Error"),
    ):
        with pytest.raises(Exception, match="Configured Refresh Token is invalid"):
            await source.ping()


@pytest.mark.asyncio
async def test_close_with_client_session():
    source = create_source(DropboxDataSource)
    source.dropbox_client._create_connection

    await source.close()
    assert hasattr(source.dropbox_client.__dict__, "_create_connection") is False


@pytest.mark.asyncio
async def test_close_without_client_session():
    source = create_source(DropboxDataSource)

    await source.close()
    assert source.dropbox_client._session is None
