#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from connectors.access_control import ACCESS_CONTROL
from connectors.sources.microsoft_teams import (
    MicrosoftTeamsClient,
    MicrosoftTeamsDataSource,
)
from connectors.sources.microsoft_teams.client import (
    EndSignal,
    Schema,
    TeamsObjectType,
)
from connectors.sources.microsoft_teams.formatter import MicrosoftTeamsFormatter
from connectors.sources.shared.microsoft.graph import (
    EntraAPIToken,
    GraphAPIToken,
    NotFound,
    PermissionsMissing,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source

TEAMS = [
    {
        "id": "team-1",
        "displayName": "Team One",
        "description": "First team",
        "createdDateTime": "2023-08-16T04:46:53.056Z",
    }
]

TEAM_MEMBERS = [
    {
        "id": "membership-1",
        "displayName": "Alice",
        "userId": "user-alice",
        "email": "alice@example.com",
        "roles": ["owner"],
    },
    {
        "id": "membership-2",
        "displayName": "Bob",
        "userId": "user-bob",
        "email": "bob@example.com",
        "roles": [],
    },
]

CHANNELS = [
    {
        "id": "channel-1",
        "displayName": "General",
        "description": "General channel",
        "webUrl": "https://teams.microsoft.com/l/channel/1",
        "createdDateTime": "2023-08-16T04:46:53.056Z",
    }
]

CHANNEL_MESSAGES = [
    {
        "id": "message-1",
        "messageType": "message",
        "createdDateTime": "2023-08-16T04:47:55.794Z",
        "lastModifiedDateTime": "2023-08-16T04:47:55.794Z",
        "deletedDateTime": None,
        "webUrl": "https://teams.microsoft.com/l/message/1",
        "from": {"user": {"displayName": "Alice"}},
        "body": {"contentType": "html", "content": "<div>Hello channel</div>"},
        "attachments": [],
        "replies": [
            {
                "id": "reply-1",
                "messageType": "message",
                "createdDateTime": "2023-08-16T04:48:55.794Z",
                "lastModifiedDateTime": "2023-08-16T04:48:55.794Z",
                "deletedDateTime": None,
                "webUrl": "https://teams.microsoft.com/l/message/1/reply/1",
                "from": {"user": {"displayName": "Bob"}},
                "body": {"contentType": "html", "content": "<div>Hi Alice</div>"},
                "attachments": [],
            }
        ],
    },
    {
        "id": "message-2-deleted",
        "messageType": "message",
        "createdDateTime": "2023-08-16T04:49:55.794Z",
        "lastModifiedDateTime": "2023-08-16T04:49:55.794Z",
        "deletedDateTime": "2023-08-16T04:50:55.794Z",
        "webUrl": "https://teams.microsoft.com/l/message/2",
        "from": {"user": {"displayName": "Alice"}},
        "body": {"contentType": "html", "content": "<div>deleted</div>"},
        "attachments": [],
        "replies": [],
    },
]

CHANNEL_FILES_FOLDER = {
    "id": "root-folder",
    "parentReference": {"driveId": "drive-123"},
}

CHANNEL_DRIVE_CHILDREN = [
    {
        "id": "file-1",
        "name": "report.txt",
        "webUrl": "https://example.com/report.txt",
        "size": 42,
        "lastModifiedDateTime": "2023-08-16T04:47:29Z",
        "createdDateTime": "2023-08-16T04:47:26Z",
        "file": {"mimeType": "text/plain"},
        "parentReference": {"driveId": "drive-123"},
    }
]

CHATS = [
    {
        "id": "chat-1",
        "topic": "Project chat",
        "chatType": "group",
        "webUrl": "https://teams.microsoft.com/l/chat/1",
        "createdDateTime": "2023-07-21T21:24:18.338Z",
        "lastUpdatedDateTime": "2023-07-21T21:24:18.338Z",
        "members": [
            {
                "id": "cm-1",
                "displayName": "Alice",
                "userId": "user-alice",
                "email": "alice@example.com",
            },
            {
                "id": "cm-2",
                "displayName": "Bob",
                "userId": "user-bob",
                "email": "bob@example.com",
            },
        ],
    }
]

CHAT_MESSAGES = [
    {
        "id": "chat-message-1",
        "messageType": "message",
        "createdDateTime": "2023-07-21T21:24:18.726Z",
        "lastModifiedDateTime": "2023-07-21T21:24:18.726Z",
        "deletedDateTime": None,
        "webUrl": None,
        "from": {"user": {"id": "user-alice", "displayName": "Alice"}},
        "body": {"contentType": "html", "content": "<h1>chat body</h1>"},
        "attachments": [
            {
                "id": "att-1",
                "name": "doc.txt",
                "contentType": "reference",
            }
        ],
    }
]

CHAT_ATTACHMENTS = [
    {
        "id": "chat-file-1",
        "name": "doc.txt",
        "webUrl": "https://example.com/doc.txt",
        "size": 10,
        "lastModifiedDateTime": "2023-07-21T21:24:18.726Z",
        "createdDateTime": "2023-07-21T21:24:18.726Z",
        "file": {"mimeType": "text/plain"},
        "parentReference": {"driveId": "drive-chat"},
    }
]


@asynccontextmanager
async def create_teams_source(
    auth_method="secret",
    use_document_level_security=False,
    fetch_attachment_content=True,
):
    async with create_source(
        MicrosoftTeamsDataSource,
        tenant_id="tenant-id",
        client_id="client-id",
        auth_method=auth_method,
        secret_value="secret",
        certificate="certificate",
        private_key="private-key",
        use_document_level_security=use_document_level_security,
        fetch_attachment_content=fetch_attachment_content,
    ) as source:
        yield source


class FakeGraphSession:
    """Minimal MicrosoftAPISession stand-in for client tests."""

    def __init__(self, pages=None, fetches=None, raises=None):
        self._pages = pages or {}
        self._fetches = fetches or {}
        self._raises = raises or {}
        self.set_logger = Mock()
        self.close = Mock()

    async def scroll(self, url):
        for key, exc in self._raises.items():
            if key in url:
                raise exc
        for key, pages in self._pages.items():
            if key in url:
                for page in pages:
                    yield page
                return
        return

    async def fetch(self, url):
        for key, exc in self._raises.items():
            if key in url:
                raise exc
        for key, value in self._fetches.items():
            if key in url:
                return value
        return None


def build_client():
    client = MicrosoftTeamsClient("tenant-id", "client-id", client_secret="secret")
    return client


@pytest.mark.asyncio
async def test_get_default_configuration_has_new_auth_fields():
    config = MicrosoftTeamsDataSource.get_default_configuration()
    assert set(config.keys()) == {
        "tenant_id",
        "client_id",
        "auth_method",
        "secret_value",
        "certificate",
        "private_key",
        "fetch_attachment_content",
        "use_text_extraction_service",
        "use_document_level_security",
    }
    # legacy username/password auth is gone
    assert "username" not in config
    assert "password" not in config


@pytest.mark.asyncio
async def test_client_uses_graph_token_for_secret_auth():
    async with create_teams_source(auth_method="secret") as source:
        assert isinstance(source.client.graph_api_token, GraphAPIToken)
        await source.client.close()


@pytest.mark.asyncio
async def test_client_uses_entra_token_for_certificate_auth():
    async with create_teams_source(auth_method="certificate") as source:
        assert isinstance(source.client.graph_api_token, EntraAPIToken)
        await source.client.close()


@pytest.mark.asyncio
async def test_client_rejects_missing_credentials():
    with pytest.raises(Exception):
        MicrosoftTeamsClient("tenant-id", "client-id")


@pytest.mark.asyncio
async def test_client_get_teams():
    client = build_client()
    client._graph_api_client = FakeGraphSession(pages={"/teams?$top=999": [TEAMS]})
    result = []
    async for page in client.get_teams():
        result.extend(page)
    await client.close()
    assert result == TEAMS


@pytest.mark.asyncio
async def test_client_get_team_members_swallows_permissions_missing():
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        raises={"/members": PermissionsMissing()}
    )
    result = []
    async for page in client.get_team_members("team-1"):
        result.extend(page)
    await client.close()
    assert result == []


@pytest.mark.asyncio
async def test_client_get_channel_messages_swallows_not_found():
    client = build_client()
    client._graph_api_client = FakeGraphSession(raises={"/messages": NotFound()})
    result = []
    async for page in client.get_channel_messages("team-1", "channel-1"):
        result.extend(page)
    await client.close()
    assert result == []


@pytest.mark.asyncio
async def test_client_get_channel_drive_children_recurses_into_folders():
    client = build_client()
    folder = {
        "id": "folder-1",
        "folder": {"childCount": 1},
        "parentReference": {"driveId": "d"},
    }
    nested_file = {"id": "nested", "file": {}, "parentReference": {"driveId": "d"}}
    top_file = {"id": "top", "file": {}, "parentReference": {"driveId": "d"}}

    pages = {
        "/items/root-folder/children": [[folder, top_file]],
        "/items/folder-1/children": [[nested_file]],
    }
    client._graph_api_client = FakeGraphSession(pages=pages)

    seen = []
    async for child in client.get_channel_drive_children("drive-123", "root-folder"):
        seen.append(child["id"])
    await client.close()

    assert "nested" in seen
    assert "top" in seen


@pytest.mark.asyncio
async def test_client_ping_calls_teams_endpoint():
    client = build_client()
    client._graph_api_client = MagicMock()
    client._graph_api_client.fetch = AsyncMock(return_value={"value": []})
    await client.ping()
    await client.close()
    client._graph_api_client.fetch.assert_awaited_once()
    assert "/teams" in client._graph_api_client.fetch.await_args.args[0]


@pytest.mark.asyncio
async def test_validate_config_fetches_token():
    async with create_teams_source() as source:
        source.client.graph_api_token.get = AsyncMock(return_value="token")
        await source.validate_config()
        source.client.graph_api_token.get.assert_awaited()


@pytest.mark.asyncio
async def test_ping_raises_on_error():
    async with create_teams_source() as source:
        source.client.ping = AsyncMock(side_effect=Exception("boom"))
        with pytest.raises(Exception):
            await source.ping()


# -- Formatter ---------------------------------------------------------------


def test_formatter_team_member_id_is_prefixed_with_team():
    formatter = MicrosoftTeamsFormatter(Schema())
    doc = formatter.format_team_member(TEAM_MEMBERS[0], "team-1", "Team One")
    assert doc["_id"] == "team-1-membership-1"
    assert doc["type"] == TeamsObjectType.TEAM_MEMBER.value
    assert doc["title"] == "Alice"
    assert doc["email"] == "alice@example.com"


def test_formatter_channel_message():
    formatter = MicrosoftTeamsFormatter(Schema())
    doc = formatter.format_channel_message(
        item=CHANNEL_MESSAGES[0],
        channel_name="General",
        message_content="Hello channel",
    )
    assert doc["type"] == TeamsObjectType.CHANNEL_MESSAGE.value
    assert doc["sender"] == "Alice"
    assert doc["channel"] == "General"
    assert doc["message"] == "Hello channel"
    assert doc["_id"] == "message-1"


def test_formatter_chat_message_uses_topic_as_title():
    formatter = MicrosoftTeamsFormatter(Schema())
    doc = formatter.format_chat_message(
        chat=CHATS[0],
        message=CHAT_MESSAGES[0],
        message_content="chat body",
        members="Alice,Bob",
    )
    assert doc["type"] == TeamsObjectType.CHAT_MESSAGE.value
    assert doc["title"] == "Project chat"
    assert doc["sender"] == "Alice"
    assert doc["message"] == "chat body"


def test_formatter_chat_message_falls_back_to_members():
    formatter = MicrosoftTeamsFormatter(Schema())
    chat = dict(CHATS[0])
    chat["topic"] = None
    doc = formatter.format_chat_message(
        chat=chat,
        message=CHAT_MESSAGES[0],
        message_content="chat body",
        members="Alice,Bob",
    )
    assert doc["title"] == "Alice,Bob"


# -- DLS ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "feature_flag, config_value, expected",
    [
        (True, True, True),
        (True, False, False),
        (False, True, False),
    ],
)
@pytest.mark.asyncio
async def test_dls_enabled(feature_flag, config_value, expected):
    async with create_teams_source(use_document_level_security=config_value) as source:
        source._features = Mock()
        source._features.document_level_security_enabled = Mock(
            return_value=feature_flag
        )
        assert source._dls_enabled() == expected


@pytest.mark.asyncio
async def test_dls_disabled_when_features_missing():
    async with create_teams_source(use_document_level_security=True) as source:
        source._features = None
        assert not source._dls_enabled()


@pytest.mark.asyncio
async def test_access_control_for_members():
    async with create_teams_source() as source:
        acl = source._access_control_for_members(TEAM_MEMBERS)
        assert "user_id:user-alice" in acl
        assert "email:alice@example.com" in acl
        assert "user_id:user-bob" in acl


@pytest.mark.asyncio
async def test_decorate_with_access_control_noop_when_dls_disabled():
    async with create_teams_source(use_document_level_security=False) as source:
        source._features = None
        doc = source._decorate_with_access_control({"_id": "x"}, ["user_id:1"])
        assert ACCESS_CONTROL not in doc


@pytest.mark.asyncio
async def test_decorate_with_access_control_when_dls_enabled():
    async with create_teams_source(use_document_level_security=True) as source:
        source._features = Mock()
        source._features.document_level_security_enabled = Mock(return_value=True)
        doc = source._decorate_with_access_control({"_id": "x"}, ["user_id:1"])
        assert doc[ACCESS_CONTROL] == ["user_id:1"]


@pytest.mark.asyncio
async def test_get_access_control_yields_unique_identities():
    async with create_teams_source(use_document_level_security=True) as source:
        source._features = Mock()
        source._features.document_level_security_enabled = Mock(return_value=True)
        source.client.get_teams = MagicMock(return_value=AsyncIterator([TEAMS]))
        source.client.get_team_members = MagicMock(
            return_value=AsyncIterator([TEAM_MEMBERS])
        )
        source.client.get_chats = MagicMock(return_value=AsyncIterator([CHATS]))

        ids = []
        async for doc in source.get_access_control():
            ids.append(doc["_id"])

        # alice and bob appear in both team and chat, but must be deduped
        assert sorted(ids) == ["user-alice", "user-bob"]


@pytest.mark.asyncio
async def test_get_access_control_skips_when_disabled():
    async with create_teams_source(use_document_level_security=False) as source:
        source._features = None
        docs = [doc async for doc in source.get_access_control()]
        assert docs == []


# -- get_docs ----------------------------------------------------------------


def _mock_client_for_get_docs(source, with_attachments=True):
    source.client.get_teams = MagicMock(return_value=AsyncIterator([TEAMS]))
    source.client.get_team_members = MagicMock(
        return_value=AsyncIterator([TEAM_MEMBERS])
    )
    source.client.get_team_channels = MagicMock(return_value=AsyncIterator([CHANNELS]))
    source.client.get_channel_messages = MagicMock(
        return_value=AsyncIterator([CHANNEL_MESSAGES])
    )
    source.client.get_channel_file = AsyncMock(return_value=CHANNEL_FILES_FOLDER)
    source.client.get_channel_drive_children = MagicMock(
        return_value=AsyncIterator(CHANNEL_DRIVE_CHILDREN)
    )
    source.client.get_chats = MagicMock(return_value=AsyncIterator([CHATS]))
    source.client.get_chat_messages = MagicMock(
        return_value=AsyncIterator([CHAT_MESSAGES])
    )
    source.client.get_chat_attachments = MagicMock(
        return_value=AsyncIterator([CHAT_ATTACHMENTS])
    )


@pytest.mark.asyncio
async def test_get_docs_emits_expected_types():
    async with create_teams_source() as source:
        _mock_client_for_get_docs(source)

        docs = []
        async for doc, _download in source.get_docs():
            docs.append(doc)

        types = {doc["type"] for doc in docs}
        assert types == {
            TeamsObjectType.TEAM.value,
            TeamsObjectType.TEAM_MEMBER.value,
            TeamsObjectType.CHANNEL.value,
            TeamsObjectType.CHANNEL_MESSAGE.value,
            TeamsObjectType.CHANNEL_ATTACHMENT.value,
            TeamsObjectType.CHAT.value,
            TeamsObjectType.CHAT_MESSAGE.value,
            TeamsObjectType.CHAT_ATTACHMENT.value,
        }


@pytest.mark.asyncio
async def test_get_docs_skips_deleted_channel_messages():
    async with create_teams_source() as source:
        _mock_client_for_get_docs(source)

        message_ids = []
        async for doc, _download in source.get_docs():
            if doc["type"] == TeamsObjectType.CHANNEL_MESSAGE.value:
                message_ids.append(doc["_id"])

        assert "message-1" in message_ids
        assert "reply-1" in message_ids
        assert "message-2-deleted" not in message_ids


@pytest.mark.asyncio
async def test_get_docs_without_attachments_when_disabled():
    async with create_teams_source(fetch_attachment_content=False) as source:
        _mock_client_for_get_docs(source)

        types = set()
        async for doc, _download in source.get_docs():
            types.add(doc["type"])

        assert TeamsObjectType.CHANNEL_ATTACHMENT.value not in types
        assert TeamsObjectType.CHAT_ATTACHMENT.value not in types


@pytest.mark.asyncio
async def test_get_docs_continues_when_teams_permission_missing():
    async with create_teams_source() as source:
        source.client.get_teams = MagicMock(side_effect=PermissionsMissing())
        source.client.get_chats = MagicMock(return_value=AsyncIterator([CHATS]))
        source.client.get_chat_messages = MagicMock(
            return_value=AsyncIterator([CHAT_MESSAGES])
        )
        source.client.get_chat_attachments = MagicMock(
            return_value=AsyncIterator([CHAT_ATTACHMENTS])
        )

        types = set()
        async for doc, _download in source.get_docs():
            types.add(doc["type"])

        assert TeamsObjectType.CHAT.value in types
        assert TeamsObjectType.TEAM.value not in types


# -- get_content -------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_content_returns_none_when_not_doit():
    async with create_teams_source() as source:
        attachment = {
            "_id": "file-1",
            "_timestamp": "2023-08-16T04:47:29Z",
            "name": "report.txt",
            "size_in_bytes": 42,
        }
        result = await source.get_content(
            attachment, drive_id="d", item_id="i", doit=False
        )
        assert result is None


@pytest.mark.asyncio
async def test_get_content_returns_none_for_zero_size():
    async with create_teams_source() as source:
        attachment = {
            "_id": "file-1",
            "_timestamp": "2023-08-16T04:47:29Z",
            "name": "report.txt",
            "size_in_bytes": 0,
        }
        result = await source.get_content(
            attachment, drive_id="d", item_id="i", doit=True
        )
        assert result is None


@pytest.mark.asyncio
async def test_get_content_downloads_and_extracts():
    async with create_teams_source() as source:
        attachment = {
            "_id": "file-1",
            "_timestamp": "2023-08-16T04:47:29Z",
            "name": "report.txt",
            "size_in_bytes": 42,
        }
        source.client.download_drive_item = AsyncMock()

        with patch.object(
            source,
            "download_and_extract_file",
            new=AsyncMock(),
        ):
            # Bypass the real extraction; assert only branching / helper calls
            source.can_file_be_downloaded = Mock(return_value=True)
            source.create_temp_file = MagicMock()
            handle = AsyncMock(return_value={"_id": "file-1", "_attachment": "b64"})
            source.handle_file_content_extraction = handle

            @asynccontextmanager
            async def fake_temp(_ext):
                buffer = MagicMock()
                buffer.name = "/tmp/report.txt"
                buffer.close = AsyncMock()
                yield buffer

            source.create_temp_file = fake_temp

            result = await source.get_content(
                attachment, drive_id="drive-123", item_id="file-1", doit=True
            )

        source.client.download_drive_item.assert_awaited_once()
        handle.assert_awaited_once()
        assert result["_attachment"] == "b64"


@pytest.mark.asyncio
async def test_consumer_decrements_tasks_on_end_signal():
    async with create_teams_source() as source:
        source.tasks = 1
        await source.queue.put(({"_id": "1"}, None))
        await source.queue.put(EndSignal.TEAM_TASK_FINISHED)

        collected = []
        async for item in source._consumer():
            collected.append(item)

        assert collected == [({"_id": "1"}, None)]
        assert source.tasks == 0
