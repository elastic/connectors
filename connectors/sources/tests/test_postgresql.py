#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the PostgreSQL database source class methods"""
import ssl
from unittest.mock import Mock, patch

import pytest

from connectors.sources.postgresql import PostgreSQLDataSource
from connectors.sources.tests.support import create_source


class AsyncIter:
    """This Class is use to return async generator"""

    def __init__(self, items):
        """Setup list of dictionary"""
        self.items = items

    async def __aiter__(self):
        """This Method is used to return async generator"""
        for item in self.items:
            yield item

    async def __anext__(self):
        """This Method is used to return one document"""
        return self.items[0]


class mock_ssl:
    """This class contains methods which returns dummy ssl context"""

    def load_verify_locations(self, cadata):
        """This method verify locations"""
        pass


@pytest.mark.asyncio
async def test_ping(patch_logger):
    """Test ping method of PostgreSQLDataSource class"""
    # Setup
    source = create_source(PostgreSQLDataSource)
    source.execute_query = Mock(return_value=AsyncIter(["table1", "table2"]))

    # Execute
    await source.ping()


@pytest.mark.asyncio
async def test_ping_negative(patch_logger):
    """Test ping method of PostgreSQLDataSource class with negative case"""
    # Setup
    source = create_source(PostgreSQLDataSource)

    with patch.object(
        PostgreSQLDataSource,
        "execute_query",
        side_effect=Exception("Something went wrong"),
    ):
        # Execute
        with pytest.raises(Exception):
            await source.ping()


def test_get_connect_argss(patch_logger):
    """This function test get_connect_args with dummy certificate"""
    # Setup
    source = create_source(PostgreSQLDataSource)
    source.ssl_ca = "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"

    # Execute
    with patch.object(ssl, "create_default_context", return_value=mock_ssl()):
        source.get_connect_args()
