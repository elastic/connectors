#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
import pytest_asyncio

from connectors.sources.google import (
    GMailClient,
    GoogleDirectoryClient,
    GoogleServiceAccountClient,
    remove_universe_domain,
)
from tests.commons import AsyncIterator

JSON_CREDENTIALS = {"key": "value"}
CUSTOMER_ID = "customer_id"


def setup_gmail_client():
    return GMailClient(JSON_CREDENTIALS, CUSTOMER_ID, "me")


def setup_google_directory_client():
    return GoogleDirectoryClient(JSON_CREDENTIALS, CUSTOMER_ID)


def setup_google_service_account_client():
    return GoogleServiceAccountClient(JSON_CREDENTIALS, "some api", "v1", [], 60)


def test_remove_universe_domain():
    universe_domain = "universe_domain"
    json_credentials = {universe_domain: "some_value", "key": "value"}
    remove_universe_domain(json_credentials)

    assert universe_domain not in json_credentials


class TestGoogleServiceAccountClient:
    @pytest_asyncio.fixture(autouse=True)
    async def patch_service_account_creds(self):
        with patch(
            "connectors.sources.google.ServiceAccountCreds", return_value=Mock()
        ) as class_mock:
            yield class_mock

    @pytest_asyncio.fixture
    async def patch_aiogoogle(self):
        with patch(
            "connectors.sources.google.Aiogoogle", return_value=MagicMock()
        ) as mock:
            aiogoogle_client = AsyncMock()
            mock.return_value.__aenter__.return_value = aiogoogle_client
            yield aiogoogle_client

    @pytest.mark.asyncio
    async def test_api_call_paged(self, patch_service_account_creds, patch_aiogoogle):
        items = ["a", "b", "c"]
        first_page_mock = AsyncIterator(items)
        first_page_mock.content = items

        google_service_account_client = setup_google_service_account_client()
        patch_aiogoogle.as_service_account = AsyncMock(return_value=first_page_mock)
        workspace_client_mock = MagicMock()

        resource = "resourcet"
        method = "method"
        resource_object = Mock()
        method_object = Mock()
        setattr(resource_object, method, method_object)
        setattr(workspace_client_mock, resource, resource_object)

        patch_aiogoogle.discover = AsyncMock(return_value=workspace_client_mock)

        actual_items = []

        async for item in google_service_account_client.api_call_paged(
            resource, method
        ):
            actual_items.append(item)

        assert actual_items == items

    @pytest.mark.asyncio
    async def test_execute_api_call(self, patch_service_account_creds, patch_aiogoogle):
        items = ["a", "b", "c"]

        async def _call_api_func(*args):
            for item in items:
                yield item

        google_service_account_client = setup_google_service_account_client()
        workspace_client_mock = MagicMock()

        resource = "resource"
        method = "method"
        resource_object = Mock()
        method_object = Mock()
        setattr(resource_object, method, method_object)
        setattr(workspace_client_mock, resource, resource_object)

        patch_aiogoogle.discover = AsyncMock(return_value=workspace_client_mock)

        actual_items = []

        async for item in google_service_account_client._execute_api_call(
            resource, method, _call_api_func, ""
        ):
            actual_items.append(item)

        assert actual_items == items

    @pytest.mark.asyncio
    async def test_api_call(self, patch_service_account_creds, patch_aiogoogle):
        item = "a"

        google_service_account_client = setup_google_service_account_client()
        patch_aiogoogle.as_service_account = AsyncMock(return_value=item)
        workspace_client_mock = MagicMock()

        resource = "resource"
        method = "method"
        resource_object = Mock()
        method_object = Mock()
        setattr(resource_object, method, method_object)
        setattr(workspace_client_mock, resource, resource_object)

        patch_aiogoogle.discover = AsyncMock(return_value=workspace_client_mock)
        actual_item = await google_service_account_client.api_call(resource, method)

        assert actual_item == item


class TestGoogleDirectoryClient:
    @pytest_asyncio.fixture
    async def patch_google_service_account_client(self):
        with patch(
            "connectors.sources.google.GoogleServiceAccountClient",
            return_value=AsyncMock(),
        ) as mock:
            client = mock.return_value
            yield client

    @pytest.mark.asyncio
    async def test_ping_successful(self, patch_google_service_account_client):
        google_directory_client = setup_google_directory_client()
        patch_google_service_account_client.api_call = AsyncMock()

        try:
            await google_directory_client.ping()
        except Exception:
            raise AssertionError("Ping should've been successful") from None

    @pytest.mark.asyncio
    async def test_ping_failed(self, patch_google_service_account_client):
        google_directory_client = setup_google_directory_client()
        patch_google_service_account_client.api_call = AsyncMock(
            side_effect=Exception()
        )

        with pytest.raises(Exception):
            await google_directory_client.ping()

    @pytest.mark.asyncio
    async def test_users(self, patch_google_service_account_client):
        google_directory_client = setup_google_directory_client()

        users = [
            {
                "users": [
                    {"primaryEmail": "some.user1@gmail.com"},
                    {"primaryEmail": "some.user2@gmail.com"},
                    {"primaryEmail": "some.user3@gmail.com"},
                ]
            }
        ]

        patch_google_service_account_client.api_call_paged = AsyncIterator(users)

        actual_users = []

        async for user in google_directory_client.users():
            actual_users.append(user)

        assert actual_users == users[0]["users"]


class TestGMailClient:
    @pytest_asyncio.fixture
    async def patch_google_service_account_client(self):
        with patch(
            "connectors.sources.google.GoogleServiceAccountClient",
            return_value=AsyncMock(),
        ) as mock:
            client = mock.return_value
            yield client

    @pytest.mark.asyncio
    async def test_ping_successful(self, patch_google_service_account_client):
        gmail_client = setup_gmail_client()
        patch_google_service_account_client.api_call = AsyncMock()

        try:
            await gmail_client.ping()
        except Exception:
            raise AssertionError("Ping should've been successful") from None

    @pytest.mark.asyncio
    async def test_ping_failed(self, patch_google_service_account_client):
        gmail_client = setup_gmail_client()
        patch_google_service_account_client.api_call = AsyncMock(
            side_effect=Exception()
        )

        with pytest.raises(Exception):
            await gmail_client.ping()

    @pytest.mark.asyncio
    async def test_messages(self, patch_google_service_account_client):
        gmail_client = setup_gmail_client()

        messages = [
            {
                "messages": [
                    # other fields are omitted in the test
                    {"raw": "some-message-1"},
                    {"raw": "some-message-2"},
                    {"raw": "some-message-3"},
                ]
            }
        ]

        patch_google_service_account_client.api_call_paged = AsyncIterator(messages)

        actual_messages = []

        async for message in gmail_client.messages("some.user@gmail.com"):
            actual_messages.append(message)

        assert actual_messages == messages[0]["messages"]

    @pytest.mark.asyncio
    async def test_message(self, patch_google_service_account_client):
        gmail_client = setup_gmail_client()

        message = {"raw": "some content", "internalDate": "some date"}
        patch_google_service_account_client.api_call = AsyncMock(return_value=message)

        actual_message = await gmail_client.message("1")

        assert actual_message == message
