#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

import aiohttp
import pytest
import pytest_asyncio
from aiohttp.client_exceptions import ClientResponseError

from connectors.logger import logger
from connectors.sources.slack import SlackClient, SlackDataSource
from tests.commons import AsyncIterator
from tests.sources.support import create_source

configuration = {
    "token": "fake_token",
    "auto_join_channels": False,
    "fetch_last_n_days": 180,
    "sync_users": False,
}


@pytest.fixture
def slack_client():
    client = SlackClient(configuration)
    client.set_logger(logger)
    return client


@pytest.fixture
def slack_data_source():
    source = create_source(SlackDataSource, **configuration)
    source.set_logger(logger)
    return source


# Tests for SlackClient


@pytest.mark.asyncio
async def test_slack_client_list_channels(slack_client, mock_responses):
    response_data = {"channels": [{"name": "channel1"}]}
    mock_responses.get(
        "https://slack.com/api/conversations.list?limit=200",
        payload=response_data,
    )

    async for channel in slack_client.list_channels(True):
        assert channel["name"] == "channel1"


@pytest.mark.asyncio
async def test_slack_client_list_messages(slack_client, mock_responses):
    message1 = {"text": "message1", "type": "message"}
    message2 = {"text": "message2", "type": "message"}
    timestamp1 = "1690674765"
    timestamp2 = "1690761165"
    response_data = {"messages": [message1, message2]}
    mock_responses.get(
        f"https://slack.com/api/conversations.history?channel=channel_id&limit=200&oldest={timestamp1}&latest={timestamp2}",
        payload=response_data,
    )

    messages = []
    async for message in slack_client.list_messages(
        "channel_id", timestamp1, timestamp2
    ):
        messages.append(message)

    assert len(messages) == 2
    assert messages[0]["text"] == "message1"
    assert messages[1]["text"] == "message2"


@pytest.mark.asyncio
async def test_slack_client_list_users(slack_client, mock_responses):
    response_data = {"members": [{"id": "user1"}]}
    mock_responses.get(
        "https://slack.com/api/users.list?limit=200",
        payload=response_data,
    )

    users = []
    async for user in slack_client.list_users():
        users.append(user)

    assert len(users) == 1
    assert users[0]["id"] == "user1"


# Tests for SlackDataSource


@pytest.mark.asyncio
async def test_slack_data_source_get_docs(slack_data_source, mock_responses):
    users_response = [{"id": "user1"}]
    channels_response = [{"id": "1", "name": "channel1"}]
    messages_response = [{"text": "message1", "type": "message"}]

    slack_client = Mock()
    slack_client.list_users = AsyncIterator(users_response)
    slack_client.list_channels = AsyncIterator(channels_response)
    slack_client.list_messages = AsyncIterator(messages_response)
    slack_data_source.slack_client = slack_client

    docs = []
    async for doc, _ in slack_data_source.get_docs():
        docs.append(doc)

    assert len(docs) == 2
    assert docs[0]["type"] == "channel"
    assert docs[1]["type"] == "message"


# Add more test cases for different scenarios...
