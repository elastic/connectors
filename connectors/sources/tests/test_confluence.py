#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Confluence database source class methods"""
import ssl
from unittest.mock import patch

import pytest

from connectors.source import DataSourceConfiguration
from connectors.sources.confluence import ConfluenceClient, ConfluenceDataSource
from connectors.sources.tests.support import create_source
from connectors.utils import ssl_context

HOST_URL = "http://127.0.0.1:5000"


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_concurrent_downloads():
    """Test validate configuration method of BaseDataSource class with invalid concurrent downloads"""

    # Setup
    source = create_source(ConfluenceDataSource)
    source.concurrent_downloads = 1000

    # Execute
    with pytest.raises(
        Exception, match="Configured concurrent downloads can't be set more than *"
    ):
        await source.validate_config()


def test_tweak_bulk_options():
    """Test tweak_bulk_options method of BaseDataSource class"""

    # Setup
    source = create_source(ConfluenceDataSource)
    source.concurrent_downloads = 10
    options = {"concurrent_downloads": 5}

    # Execute
    source.tweak_bulk_options(options)

    assert options["concurrent_downloads"] == 10


@pytest.mark.asyncio
async def test_close_with_client_session(patch_logger):
    """Test close method for closing the existing session"""

    # Setup
    source = create_source(ConfluenceDataSource)
    source.confluence_client.get_session()

    # Execute
    await source.close()

    assert source.confluence_client.session is None


@pytest.mark.asyncio
async def test_close_without_client_session(patch_logger):
    """Test close method when the session does not exist"""
    # Setup
    source = create_source(ConfluenceDataSource)

    # Execute
    await source.close()

    assert source.confluence_client.session is None


class MockSSL:
    """This class contains methods which returns dummy ssl context"""

    def load_verify_locations(self, cadata):
        """This method verify locations"""
        pass


@pytest.mark.asyncio
async def test_configuration():
    """Tests the get_default_configurations method of the Confluence source class."""
    # Setup
    config = DataSourceConfiguration(
        config=ConfluenceDataSource.get_default_configuration()
    )

    assert config["host_url"] == HOST_URL


@pytest.mark.parametrize(
    "field, is_cloud",
    [
        ("host_url", True),
        ("service_account_id", True),
        ("api_token", True),
        ("username", False),
        ("password", False),
    ],
)
@pytest.mark.asyncio
async def test_validate_configuration_for_empty_fields(field, is_cloud):
    source = create_source(ConfluenceDataSource)
    source.confluence_client.is_cloud = is_cloud
    source.confluence_client.configuration.set_field(name=field, value="")

    # Execute
    with pytest.raises(Exception):
        await source.validate_config()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_with_ssl(mock_get):
    """Test ping method of ConfluenceDataSource class with SSL"""

    # Execute
    mock_get.return_value.__aenter__.return_value.status = 200
    source = create_source(ConfluenceDataSource)
    source.confluence_client.get_session()

    source.confluence_client.ssl_enabled = True
    source.confluence_client.certificate = (
        "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"
    )

    # Execute
    with patch.object(ssl, "create_default_context", return_value=MockSSL()):
        source.confluence_client.ssl_ctx = ssl_context(
            certificate=source.confluence_client.certificate
        )
        await source.ping()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_for_failed_connection_exception(mock_get):
    """Tests the ping functionality when connection can not be established to Confluence."""

    # Setup
    source = create_source(ConfluenceDataSource)

    # Execute
    with patch.object(
        ConfluenceClient, "_api_call", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await source.ping()


def test_validate_configuration_for_ssl_enabled(patch_logger):
    """This function tests _validate_configuration when certification is empty and ssl is enabled"""
    # Setup
    source = create_source(ConfluenceDataSource)
    source.ssl_enabled = True

    # Execute
    with pytest.raises(Exception):
        source._validate_configuration()
