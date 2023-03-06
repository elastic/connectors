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
from connectors.sources.confluence import ConfluenceDataSource
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
    options = {"concurrent_downloads": 10}

    # Execute
    source.tweak_bulk_options(options)


@pytest.mark.asyncio
async def test_close_with_client_session(patch_logger):
    """Test close method for closing the existing session"""

    # Setup
    source = create_source(ConfluenceDataSource)
    source._generate_session()

    # Execute
    await source.close()

    # Assert
    assert source.session is None


@pytest.mark.asyncio
async def test_close_without_client_session(patch_logger):
    """Test close method when the session does not exist"""
    # Setup
    source = create_source(ConfluenceDataSource)

    # Execute
    await source.close()

    # Assert
    assert source.session is None


class MockSSL:
    """This class contains methods which returns dummy ssl context"""

    def load_verify_locations(self, cadata):
        """This method verify locations"""
        pass


@pytest.mark.asyncio
async def test_configuration():
    """Tests the get_default_configurations method of the Confluence source class."""
    # Setup
    klass = ConfluenceDataSource

    # Execute
    config = DataSourceConfiguration(config=klass.get_default_configuration())

    # Assert
    assert config["host_url"] == HOST_URL


@pytest.mark.asyncio
async def test_validate_configuration_for_host_url(patch_logger):
    """This function test _validate_configuration when host_url is invalid"""
    # Setup
    source = create_source(ConfluenceDataSource)
    source.configuration.set_field(name="host_url", value="")

    # Execute
    with pytest.raises(Exception):
        await source.validate_config()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_with_ssl(mock_get, patch_logger):
    """Test ping method of ConfluenceDataSource class with SSL"""

    # Execute
    mock_get.return_value.__aenter__.return_value.status = 200
    source = create_source(ConfluenceDataSource)

    source.ssl_enabled = True
    source.certificate = (
        "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"
    )

    # Execute
    with patch.object(ssl, "create_default_context", return_value=MockSSL()):
        source.ssl_ctx = ssl_context(certificate=source.certificate)
        await source.ping()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_for_failed_connection_exception(mock_get, patch_logger):
    """Tests the ping functionality when connection can not be established to Confluence."""

    # Setup
    source = create_source(ConfluenceDataSource)

    # Execute
    with patch.object(
        ConfluenceDataSource, "_api_call", side_effect=Exception("Something went wrong")
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
