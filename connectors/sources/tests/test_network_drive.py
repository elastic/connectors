#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Network Drive source class methods.
"""
import asyncio
from unittest import mock

import pytest
import smbclient
from smbprotocol.exceptions import LogonFailure

from connectors.source import DataSourceConfiguration
from connectors.sources.network_drive import NASDataSource
from connectors.sources.tests.support import create_source


def test_get_configuration():
    """Tests the get configurations method of the Network Drive source class."""
    # Setup
    klass = NASDataSource

    # Execute
    config = DataSourceConfiguration(config=klass.get_default_configuration())

    # Assert
    assert config["server_ip"] == "1.2.3.4"


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    """Tests the ping functionality for ensuring connection to the Network Drive."""
    # Setup
    expected_response = True
    response = asyncio.Future()
    response.set_result(expected_response)

    # Execute
    with mock.patch.object(smbclient, "register_session", return_value=response):
        source = create_source(NASDataSource)

        await source.ping()


@pytest.mark.asyncio
@mock.patch("smbclient.register_session")
async def test_ping_for_failed_connection(session_mock):
    """Tests the ping functionality when connection can not be established to Network Drive.
    
    Args:
        session_mock (patch): patch of register_session method
    """
    # Setup
    response = asyncio.Future()
    response.set_result(None)
    session_mock.side_effect = ValueError
    source = create_source(NASDataSource)

    # Execute
    with pytest.raises(Exception):
        await source.ping()


@mock.patch("smbclient.register_session")
def test_create_connection_with_invalid_credentials(session_mock):
    """Tests the create_connection fails with invalid credentials

    Args:
        session_mock (patch): patch of register_session method
    """
    # Setup
    source = create_source(NASDataSource)
    session_mock.side_effect = LogonFailure

    # Execute
    with pytest.raises(LogonFailure):
        source.create_connection()
