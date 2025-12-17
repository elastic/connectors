#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from connectors.sources.active_directory.client import ActiveDirectoryClient
from connectors.sources.active_directory.datasource import ActiveDirectoryDataSource


@pytest.fixture
def ad_config():
    """Fixture providing test AD configuration"""
    return {
        "host": "ad.example.com",
        "port": 636,
        "username": "DOMAIN\\testuser",
        "password": "testpass",
        "base_dn": "DC=example,DC=com",
        "use_ssl": True,
        "auth_type": "NTLM",
        "sync_users": True,
        "sync_groups": True,
        "sync_computers": False,
        "sync_ous": False,
        "page_size": 100,
    }


@pytest.fixture
def mock_ad_user():
    """Fixture providing mock AD user object"""
    return {
        "distinguishedName": "CN=John Doe,OU=Users,DC=example,DC=com",
        "objectGUID": b"\x01\x02\x03\x04",
        "name": "John Doe",
        "sAMAccountName": "jdoe",
        "userPrincipalName": "jdoe@example.com",
        "displayName": "John Doe",
        "givenName": "John",
        "sn": "Doe",
        "mail": "john.doe@example.com",
        "department": "IT",
        "whenCreated": "2023-01-01T00:00:00Z",
        "whenChanged": "2024-01-01T00:00:00Z",
        "uSNChanged": 12345,
        "memberOf": ["CN=IT-Team,OU=Groups,DC=example,DC=com"],
    }


@pytest.fixture
def mock_ad_group():
    """Fixture providing mock AD group object"""
    return {
        "distinguishedName": "CN=IT-Team,OU=Groups,DC=example,DC=com",
        "objectGUID": b"\x05\x06\x07\x08",
        "name": "IT-Team",
        "sAMAccountName": "IT-Team",
        "groupType": -2147483646,
        "description": "IT Department Team",
        "member": [
            "CN=John Doe,OU=Users,DC=example,DC=com",
            "CN=Jane Smith,OU=Users,DC=example,DC=com",
        ],
        "whenCreated": "2023-01-01T00:00:00Z",
        "whenChanged": "2024-01-01T00:00:00Z",
        "uSNChanged": 12346,
    }


class TestActiveDirectoryClient:
    """Tests for ActiveDirectoryClient"""

    @pytest.mark.asyncio
    async def test_ping_success(self, ad_config):
        """Test successful AD connection ping"""
        client = ActiveDirectoryClient(
            host=ad_config["host"],
            port=ad_config["port"],
            username=ad_config["username"],
            password=ad_config["password"],
            base_dn=ad_config["base_dn"],
        )

        with patch.object(client, "_connect"), patch.object(
            client, "_disconnect"
        ), patch.object(client, "connection") as mock_conn:
            mock_conn.search = MagicMock()
            await client.ping()

    @pytest.mark.asyncio
    async def test_get_highest_usn(self, ad_config):
        """Test getting highest USN from AD"""
        client = ActiveDirectoryClient(
            host=ad_config["host"],
            port=ad_config["port"],
            username=ad_config["username"],
            password=ad_config["password"],
            base_dn=ad_config["base_dn"],
        )

        # Mock LDAP entry with USN
        mock_entry = MagicMock()
        mock_entry.highestCommittedUSN.value = 99999

        with patch.object(client, "_connect"), patch.object(
            client, "_disconnect"
        ), patch.object(client, "connection") as mock_conn:
            mock_conn.entries = [mock_entry]
            mock_conn.search = MagicMock()

            usn = await client.get_highest_usn()
            assert usn == 99999

    @pytest.mark.asyncio
    async def test_get_users_full_sync(self, ad_config, mock_ad_user):
        """Test getting all users (full sync)"""
        client = ActiveDirectoryClient(
            host=ad_config["host"],
            port=ad_config["port"],
            username=ad_config["username"],
            password=ad_config["password"],
            base_dn=ad_config["base_dn"],
        )

        # Mock LDAP search result
        mock_entry = {
            "type": "searchResEntry",
            "dn": mock_ad_user["distinguishedName"],
            "attributes": mock_ad_user,
        }

        with patch.object(client, "_search_paged", return_value=[mock_entry]):
            users = []
            async for user in client.get_users():
                users.append(user)

            assert len(users) == 1
            assert users[0]["sAMAccountName"] == "jdoe"

    @pytest.mark.asyncio
    async def test_get_users_incremental_sync(self, ad_config, mock_ad_user):
        """Test getting changed users (incremental sync with USN)"""
        client = ActiveDirectoryClient(
            host=ad_config["host"],
            port=ad_config["port"],
            username=ad_config["username"],
            password=ad_config["password"],
            base_dn=ad_config["base_dn"],
        )

        last_usn = 10000
        mock_ad_user["uSNChanged"] = 12345  # Changed after last_usn

        mock_entry = {
            "type": "searchResEntry",
            "dn": mock_ad_user["distinguishedName"],
            "attributes": mock_ad_user,
        }

        with patch.object(client, "_search_paged", return_value=[mock_entry]):
            users = []
            async for user in client.get_users(last_usn=last_usn):
                users.append(user)

            assert len(users) == 1
            assert users[0]["uSNChanged"] == 12345


class TestActiveDirectoryDataSource:
    """Tests for ActiveDirectoryDataSource"""

    @pytest.mark.asyncio
    async def test_ping(self, ad_config):
        """Test AD data source ping"""
        source = ActiveDirectoryDataSource(ad_config)

        with patch.object(source.ad_client, "ping", new_callable=AsyncMock):
            await source.ping()

    @pytest.mark.asyncio
    async def test_get_docs_full_sync(self, ad_config, mock_ad_user, mock_ad_group):
        """Test full sync of AD objects"""
        source = ActiveDirectoryDataSource(ad_config)

        # Mock AD client methods
        async def mock_get_users():
            yield mock_ad_user

        async def mock_get_groups():
            yield mock_ad_group

        async def mock_get_highest_usn():
            return 12346

        with patch.object(
            source.ad_client, "get_users", side_effect=mock_get_users
        ), patch.object(
            source.ad_client, "get_groups", side_effect=mock_get_groups
        ), patch.object(
            source.ad_client, "get_highest_usn", side_effect=mock_get_highest_usn
        ):

            docs = []
            async for doc, _ in source.get_docs():
                docs.append(doc)

            assert len(docs) == 2
            # Check user doc
            assert docs[0]["object_type"] == "user"
            assert docs[0]["sam_account_name"] == "jdoe"
            # Check group doc
            assert docs[1]["object_type"] == "group"
            assert docs[1]["sam_account_name"] == "IT-Team"

    @pytest.mark.asyncio
    async def test_get_docs_incrementally(self, ad_config, mock_ad_user):
        """Test incremental sync with USN tracking"""
        source = ActiveDirectoryDataSource(ad_config)

        # Start with last_usn from previous sync
        sync_cursor = {"last_usn": 10000}

        # User changed after last sync
        mock_ad_user["uSNChanged"] = 12345

        async def mock_get_users(last_usn=None):
            if last_usn == 10000:
                yield mock_ad_user

        async def mock_get_groups(last_usn=None):
            return
            yield  # Empty generator

        with patch.object(
            source.ad_client, "get_users", side_effect=mock_get_users
        ), patch.object(source.ad_client, "get_groups", side_effect=mock_get_groups):

            docs = []
            new_cursor = None

            async for doc, op, cursor in source.get_docs_incrementally(sync_cursor):
                if doc:
                    docs.append(doc)
                if cursor:
                    new_cursor = cursor

            assert len(docs) == 1
            assert docs[0]["usn_changed"] == 12345
            assert new_cursor["last_usn"] == 12345

    def test_get_default_configuration(self):
        """Test default configuration schema"""
        config = ActiveDirectoryDataSource.get_default_configuration()

        assert "host" in config
        assert "port" in config
        assert "username" in config
        assert "password" in config
        assert "base_dn" in config
        assert config["password"]["sensitive"] is True
        assert config["port"]["value"] == 636
        assert config["use_ssl"]["value"] is True


# Integration test (requires actual AD server)
@pytest.mark.integration
@pytest.mark.asyncio
async def test_integration_full_sync():
    """
    Integration test with real AD server

    To run: pytest -m integration

    Requires environment variables:
    - AD_HOST
    - AD_PORT
    - AD_USERNAME
    - AD_PASSWORD
    - AD_BASE_DN
    """
    import os

    config = {
        "host": os.getenv("AD_HOST", "ad.example.com"),
        "port": int(os.getenv("AD_PORT", "636")),
        "username": os.getenv("AD_USERNAME", "testuser"),
        "password": os.getenv("AD_PASSWORD", "testpass"),
        "base_dn": os.getenv("AD_BASE_DN", "DC=example,DC=com"),
        "use_ssl": True,
        "auth_type": "NTLM",
        "sync_users": True,
        "sync_groups": True,
        "sync_computers": False,
        "sync_ous": False,
        "page_size": 100,
    }

    source = ActiveDirectoryDataSource(config)

    # Test connection
    await source.ping()

    # Test full sync (limit results for testing)
    doc_count = 0
    async for doc, _ in source.get_docs():
        doc_count += 1
        if doc_count >= 10:  # Limit to 10 docs for testing
            break

    assert doc_count > 0
