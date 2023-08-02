#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from unittest.mock import ANY, AsyncMock, Mock, patch

import pytest
import pytest_asyncio
from aiohttp.client_exceptions import ClientError

from connectors.logger import logger
from connectors.sources.slack import SlackClient, SlackDataSource
from tests.commons import AsyncIterator
from tests.sources.support import create_source

configuration = {
    "token": "fake_token",
    "auto_join_channels": False,
    "fetch_last_n_days": 180,
    "sync_users": True,
}


@pytest_asyncio.fixture
async def slack_client():
    client = SlackClient(configuration)
    client.set_logger(logger)
    yield client
    await client.close()


@pytest_asyncio.fixture
async def slack_data_source():
    source = create_source(SlackDataSource, **configuration)
    source.set_logger(logger)
    yield source
    await source.close()


# Tests for SlackClient


@pytest.mark.asyncio
async def test_slack_client_list_channels(slack_client, mock_responses):
    page1 = {
        "channels": [{"name": "channel1", "is_member": True}],
        "response_metadata": {"next_cursor": "abc"},
    }
    mock_responses.get(
        "https://slack.com/api/conversations.list?limit=200",
        payload=page1,
    )
    page2 = {
        "channels": [
            {"name": "channel2", "id": "2", "is_member": False},
            {"name": "channel3", "is_member": True},
        ]
    }
    mock_responses.get(
        "https://slack.com/api/conversations.list?limit=200&cursor=abc",
        payload=page2,
    )

    docs = []
    async for channel in slack_client.list_channels(True):
        docs.append(channel)
    assert len(docs) == 2
    assert docs[0]["name"] == "channel1"
    assert docs[1]["name"] == "channel3"


@pytest.mark.asyncio
async def test_slack_client_list_messages(slack_client, mock_responses):
    timestamp1 = 1690674765
    timestamp2 = 1690665000
    timestamp3 = 1690761165
    channel = {"id": 1, "name": "test"}
    message1 = {
        "text": "message1",
        "type": "message",
        "reply_count": 1,
        "ts": timestamp1,
    }
    message2 = {"text": "message2", "type": "message", "ts": timestamp2}
    message3 = {"text": "message3", "type": "message", "ts": timestamp3}
    reply = {"text": "reply", "type": "message"}

    page1 = {"messages": [message1, message2], "has_more": True}
    mock_responses.get(
        f"https://slack.com/api/conversations.history?channel=1&limit=200&oldest={timestamp1}&latest={timestamp3}",
        payload=page1,
    )
    replies = {"messages": [message1, reply]}
    mock_responses.get(
        f"https://slack.com/api/conversations.replies?channel=1&limit=200&ts={timestamp1}",
        payload=replies,
    )

    page2 = {"messages": [message3]}
    mock_responses.get(
        f"https://slack.com/api/conversations.history?channel=1&limit=200&oldest={timestamp1}&latest={timestamp2}",
        payload=page2,
    )

    messages = []
    async for message in slack_client.list_messages(channel, timestamp1, timestamp3):
        messages.append(message)

    assert len(messages) == 4
    assert messages[0]["text"] == "message1"
    assert messages[1]["text"] == "reply"
    assert messages[2]["text"] == "message2"
    assert messages[3]["text"] == "message3"


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


@pytest.mark.asyncio
@patch("connectors.utils.apply_retry_strategy")
async def test_handle_throttled_error(
    mock_apply_retry_strategy, slack_client, mock_responses
):
    channel = {"id": 1, "name": "test"}
    error_response_data = {"error": "rate_limited"}
    response_data = {"messages": [{"text": "message", "type": "message"}]}
    mock_responses.get(
        "https://slack.com/api/conversations.history?channel=1&latest=456&limit=200&oldest=123",
        status=429,
        payload=error_response_data,
    )
    mock_responses.get(
        "https://slack.com/api/conversations.history?channel=1&latest=456&limit=200&oldest=123",
        status=200,
        payload=response_data,
    )

    docs = []
    with patch.object(slack_client, "_sleeps") as mock_sleeps:
        async for doc in slack_client.list_messages(channel, 123, 456):
            docs.append(doc)
        mock_sleeps.sleep.assert_called_once_with(ANY)  # Verify that sleep was called
    assert len(docs) == 1
    assert docs[0]["text"] == "message"


@pytest.mark.asyncio
async def test_ping(slack_client, mock_responses):
    response_data = {"ok": True}
    mock_responses.get(
        "https://slack.com/api/auth.test",
        status=200,
        payload=response_data,
    )
    assert await slack_client.ping()


@pytest.mark.asyncio
@patch("connectors.utils.apply_retry_strategy")
async def test_bad_ping(mock_apply_retry_strategy, slack_client, mock_responses):
    response_data = {"error": "not_authed"}
    mock_responses.get(
        "https://slack.com/api/auth.test",
        status=401,
        payload=response_data,
    )
    with pytest.raises(ClientError):
        await slack_client.ping()


# Tests for SlackDataSource


@pytest.mark.asyncio
async def test_slack_data_source_get_docs(slack_data_source, mock_responses):
    users_response = [{"id": "user1"}]
    channels_response = [{"id": "1", "name": "channel1"}]
    messages_response = [{"text": "message1", "type": "message", "ts": 123456}]

    slack_client = AsyncMock()
    slack_client.list_users = AsyncIterator(users_response)
    slack_client.list_channels = AsyncIterator(channels_response)
    slack_client.list_messages = AsyncIterator(messages_response)
    slack_client.close = AsyncMock()
    slack_data_source.slack_client = slack_client

    docs = []
    async for doc, _ in slack_data_source.get_docs():
        docs.append(doc)

    assert len(docs) == 3
    assert docs[0]["type"] == "user"
    assert docs[1]["type"] == "channel"
    assert docs[2]["type"] == "message"


@pytest.mark.asyncio
async def test_slack_data_source_convert_usernames(slack_data_source):
    usernames = {"USERID1": "user_one"}
    message = {"text": "<@USERID1> Hello, <@USERID2>", "ts": 1}
    channel = {"id": 12345, "name": "channel"}
    slack_data_source.usernames = usernames
    remapped_message = slack_data_source.remap_message(message, channel)

    assert remapped_message["text"] == "<@user_one> Hello, <@USERID2>"
