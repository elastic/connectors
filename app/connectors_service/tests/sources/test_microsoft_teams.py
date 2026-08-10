#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import base64
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from connectors_sdk.source import BaseDataSource, ConfigurableFieldValueError

from connectors.access_control import ACCESS_CONTROL
from connectors.sources.microsoft_teams.client import (
    EndSignal,
    MicrosoftTeamsClient,
    Schema,
    TeamsObjectType,
    _jwt_payload_roles,
)
from connectors.sources.microsoft_teams.datasource import (
    MicrosoftTeamsDataSource,
    _message_body_text,
    _message_subject,
)
from connectors.sources.microsoft_teams.formatter import MicrosoftTeamsFormatter
from connectors.sources.shared.microsoft.graph import (
    EntraAPIToken,
    GraphAPIToken,
    NotFound,
    PermissionsMissing,
)
from connectors.utils import ConcurrentTasks
from tests.commons import AsyncIterator
from tests.sources.support import create_source


def _fake_access_token(roles):
    header = (
        base64.urlsafe_b64encode(b'{"alg":"none","typ":"JWT"}').rstrip(b"=").decode()
    )
    payload = (
        base64.urlsafe_b64encode(json.dumps({"roles": roles}).encode())
        .rstrip(b"=")
        .decode()
    )
    return f"{header}.{payload}.sig"


TOKEN_WITH_FILES = _fake_access_token(["Files.Read.All", "Team.ReadBasic.All"])
TOKEN_WITHOUT_FILES = _fake_access_token(["Team.ReadBasic.All"])

TEAMS = [
    {
        "id": "team-1",
        "displayName": "Team One",
        "description": "First team",
        "createdDateTime": "2023-08-16T04:46:53.056Z",
        "webUrl": "https://teams.microsoft.com/l/team/1",
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
        "membershipType": "standard",
    }
]

PRIVATE_CHANNEL = {
    "id": "channel-private",
    "displayName": "Private",
    "description": "Private channel",
    "webUrl": "https://teams.microsoft.com/l/channel/private",
    "createdDateTime": "2023-08-16T04:46:53.056Z",
    "membershipType": "private",
}

PRIVATE_CHANNEL_MEMBERS = [
    {
        "id": "pcm-1",
        "displayName": "Alice",
        "userId": "user-alice",
        "email": "alice@example.com",
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
        "replyToId": None,
        "subject": None,
        "from": {"user": {"id": "user-alice", "displayName": "Alice"}},
        "body": {"contentType": "html", "content": "<div>Hello channel</div>"},
        "attachments": [],
    },
    {
        "id": "message-2-deleted",
        "messageType": "message",
        "createdDateTime": "2023-08-16T04:49:55.794Z",
        "lastModifiedDateTime": "2023-08-16T04:49:55.794Z",
        "deletedDateTime": "2023-08-16T04:50:55.794Z",
        "webUrl": "https://teams.microsoft.com/l/message/2",
        "replyToId": None,
        "from": {"user": {"id": "user-alice", "displayName": "Alice"}},
        "body": {"contentType": "html", "content": "<div>deleted</div>"},
        "attachments": [],
    },
]

CHANNEL_REPLIES = [
    {
        "id": "reply-1",
        "messageType": "message",
        "createdDateTime": "2023-08-16T04:48:55.794Z",
        "lastModifiedDateTime": "2023-08-16T04:48:55.794Z",
        "deletedDateTime": None,
        "webUrl": "https://teams.microsoft.com/l/message/1/reply/1",
        "replyToId": "message-1",
        "from": {"user": {"id": "user-bob", "displayName": "Bob"}},
        "body": {"contentType": "html", "content": "<div>Hi Alice</div>"},
        "attachments": [],
    }
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
    }
]

CHAT_MEMBERS = [
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
]

GRAPH_USERS = {
    "user-alice": {
        "id": "user-alice",
        "displayName": "Alice",
        "mail": "alice@example.com",
        "userPrincipalName": "alice@example.com",
    },
    "user-bob": {
        "id": "user-bob",
        "displayName": "Bob",
        "mail": "bob@example.com",
        "userPrincipalName": "bob@example.com",
    },
    "user-carol": {
        "id": "user-carol",
        "displayName": "Carol",
        "mail": None,
        "userPrincipalName": "carol_guest#EXT#@example.com",
    },
    "user-dave": {
        "id": "user-dave",
        "displayName": "Dave",
        "mail": "dave@example.com",
        "userPrincipalName": "dave@example.com",
    },
}


async def _graph_users_by_ids(user_ids):
    return {uid: GRAPH_USERS[uid] for uid in user_ids if uid in GRAPH_USERS}


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
                "contentUrl": "https://example.com/doc.txt",
            }
        ],
    }
]

CHAT_FILE = {
    "id": "chat-file-1",
    "name": "doc.txt",
    "webUrl": "https://example.com/doc.txt",
    "size": 10,
    "lastModifiedDateTime": "2023-07-21T21:24:18.726Z",
    "createdDateTime": "2023-07-21T21:24:18.726Z",
    "file": {"mimeType": "text/plain"},
    "parentReference": {"driveId": "drive-chat"},
}


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

    def __init__(self, pages=None, fetches=None, raises=None, posts=None):
        self._pages = pages or {}
        self._fetches = fetches or {}
        self._raises = raises or {}
        self._posts = posts or {}
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

    async def post(self, url, payload):
        for key, exc in self._raises.items():
            if key in url:
                raise exc
        for key, value in self._posts.items():
            if key in url:
                if callable(value):
                    return value(payload)
                return value
        return {"responses": []}


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
async def test_client_get_team_members_propagates_permissions_missing():
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        raises={"/members": PermissionsMissing()}
    )
    with pytest.raises(PermissionsMissing):
        async for _page in client.get_team_members("team-1"):
            pass
    await client.close()


@pytest.mark.asyncio
async def test_client_get_team_members_swallows_not_found():
    client = build_client()
    client._graph_api_client = FakeGraphSession(raises={"/members": NotFound()})
    result = []
    async for page in client.get_team_members("team-1"):
        result.extend(page)
    await client.close()
    assert result == []
    assert client._skipped["teams' members"] == 1


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
async def test_client_get_channel_messages_propagates_permissions_missing():
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        raises={"/messages": PermissionsMissing()}
    )
    with pytest.raises(PermissionsMissing):
        async for _page in client.get_channel_messages("team-1", "channel-1"):
            pass
    await client.close()


@pytest.mark.asyncio
async def test_client_get_chats_dedupes_across_users():
    """Chats come from GET /users/{id}/chats; the same chat id across members is once."""
    shared_chat = {
        "id": "chat-shared",
        "topic": "Shared",
        "chatType": "group",
    }
    alice_only = {
        "id": "chat-alice",
        "topic": "Alice only",
        "chatType": "oneOnOne",
    }
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        pages={
            "/users/user-alice/chats": [[shared_chat, alice_only]],
            "/users/user-bob/chats": [[shared_chat]],
        }
    )
    chats = []
    async for page in client.get_chats(["user-alice", "user-bob"]):
        chats.extend(page)
    await client.close()
    assert [c["id"] for c in chats] == ["chat-shared", "chat-alice"]


@pytest.mark.asyncio
async def test_client_get_chats_propagates_permissions_missing():
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        pages={"/users/user-alice/chats": [[{"id": "chat-1"}]]},
        raises={"/users/user-bob/chats": PermissionsMissing()},
    )
    with pytest.raises(PermissionsMissing):
        async for _page in client.get_chats(["user-alice", "user-bob"]):
            pass
    await client.close()


@pytest.mark.asyncio
async def test_client_get_chat_members_propagates_permissions_missing():
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        raises={"/chats/chat-1/members": PermissionsMissing()}
    )
    with pytest.raises(PermissionsMissing):
        async for _page in client.get_chat_members("chat-1"):
            pass
    await client.close()


@pytest.mark.asyncio
async def test_client_get_chat_members_swallows_not_found():
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        raises={"/chats/chat-1/members": NotFound()}
    )
    result = []
    async for page in client.get_chat_members("chat-1"):
        result.extend(page)
    await client.close()
    assert result == []
    assert client._skipped["chats' members"] == 1


@pytest.mark.asyncio
async def test_client_get_channel_message_replies():
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        pages={"/messages/message-1/replies": [CHANNEL_REPLIES]}
    )
    replies = []
    async for page in client.get_channel_message_replies(
        "team-1", "channel-1", "message-1"
    ):
        replies.extend(page)
    await client.close()
    assert [r["id"] for r in replies] == ["reply-1"]


@pytest.mark.asyncio
async def test_client_assert_files_permission_requires_role():
    client = build_client()
    client.graph_api_token.get = AsyncMock(return_value=TOKEN_WITHOUT_FILES)
    with pytest.raises(PermissionsMissing, match="Files.Read.All"):
        await client.assert_files_permission()
    await client.close()


@pytest.mark.asyncio
async def test_client_assert_files_permission_passes_with_role():
    client = build_client()
    client.graph_api_token.get = AsyncMock(return_value=TOKEN_WITH_FILES)
    await client.assert_files_permission()
    await client.close()


def test_jwt_payload_roles():
    assert "Files.Read.All" in _jwt_payload_roles(TOKEN_WITH_FILES)
    assert _jwt_payload_roles(TOKEN_WITHOUT_FILES) == ["Team.ReadBasic.All"]
    assert _jwt_payload_roles("not-a-jwt") == []


@pytest.mark.asyncio
async def test_client_get_channel_file_propagates_permissions_missing():
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        raises={"/filesFolder": PermissionsMissing()}
    )
    with pytest.raises(PermissionsMissing):
        await client.get_channel_file("team-1", "channel-1")
    await client.close()


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
    async with create_teams_source(fetch_attachment_content=False) as source:
        source.client.graph_api_token.get = AsyncMock(return_value="token")
        await source.validate_config()
        source.client.graph_api_token.get.assert_awaited()


@pytest.mark.asyncio
async def test_validate_config_requires_files_role_when_attachments_enabled():
    async with create_teams_source(fetch_attachment_content=True) as source:
        source.client.graph_api_token.get = AsyncMock(return_value=TOKEN_WITHOUT_FILES)
        with pytest.raises(PermissionsMissing, match="Files.Read.All"):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_passes_files_role_when_attachments_enabled():
    async with create_teams_source(fetch_attachment_content=True) as source:
        source.client.graph_api_token.get = AsyncMock(return_value=TOKEN_WITH_FILES)
        await source.validate_config()


@pytest.mark.asyncio
async def test_ping_raises_on_error():
    async with create_teams_source() as source:
        source.client.ping = AsyncMock(side_effect=Exception("boom"))
        with pytest.raises(Exception):
            await source.ping()


@pytest.mark.asyncio
async def test_close_ends_extraction_session_when_configured():
    async with create_teams_source() as source:
        source.client.close = AsyncMock()
        source.extraction_service = MagicMock()
        source.extraction_service._end_session = AsyncMock()

        await source.close()

        source.client.close.assert_awaited_once()
        source.extraction_service._end_session.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_skips_extraction_session_when_absent():
    async with create_teams_source() as source:
        source.client.close = AsyncMock()
        source.extraction_service = None

        await source.close()

        source.client.close.assert_awaited_once()


# -- Formatter ---------------------------------------------------------------


def test_formatter_user():
    formatter = MicrosoftTeamsFormatter(Schema())
    doc = formatter.format_user(
        "user-alice", "Alice", "alice@example.com", upn="alice@contoso.onmicrosoft.com"
    )
    assert doc["_id"] == "user-alice"
    assert doc["type"] == TeamsObjectType.USER.value
    assert doc["name"] == "Alice"
    assert doc["email"] == "alice@example.com"
    assert doc["upn"] == "alice@contoso.onmicrosoft.com"


def test_formatter_user_empty_upn_defaults_to_empty_string():
    formatter = MicrosoftTeamsFormatter(Schema())
    doc = formatter.format_user("user-alice", "Alice", None)
    assert doc["email"] == ""
    assert doc["upn"] == ""


def test_formatter_channel_message():
    formatter = MicrosoftTeamsFormatter(Schema())
    doc = formatter.format_channel_message(
        item=CHANNEL_MESSAGES[0],
        channel_id="channel-1",
        channel_title="General",
        message_content="Hello channel",
        subject="Channel subject",
    )
    assert doc["type"] == TeamsObjectType.CHANNEL_MESSAGE.value
    assert doc["sender_name"] == "Alice"
    assert doc["sender_id"] == "user-alice"
    assert doc["channel_id"] == "channel-1"
    assert doc["channel_title"] == "General"
    assert doc["subject"] == "Channel subject"
    assert doc["message"] == "Hello channel"
    assert doc["reply_to_id"] == ""
    assert doc["_id"] == "message-1"
    assert doc["attachments"] == []


def test_formatter_chat_message_uses_topic_as_chat_title():
    formatter = MicrosoftTeamsFormatter(Schema())
    doc = formatter.format_chat_message(
        chat=CHATS[0],
        message=CHAT_MESSAGES[0],
        message_content="chat body",
        members="Alice,Bob",
        attachments=[{"id": "chat-file-1", "title": "doc.txt"}],
    )
    assert doc["type"] == TeamsObjectType.CHAT_MESSAGE.value
    assert doc["chat_id"] == "chat-1"
    assert doc["chat_title"] == "Project chat"
    assert doc["sender_name"] == "Alice"
    assert doc["sender_id"] == "user-alice"
    assert doc["message"] == "chat body"
    assert doc["url"] == CHATS[0]["webUrl"]
    assert doc["attachments"] == [{"id": "chat-file-1", "title": "doc.txt"}]
    assert "reply_to_id" not in doc
    assert "chatType" not in doc
    assert "title" not in doc


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
    assert doc["chat_title"] == "Alice,Bob"


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
        # Content ACLs are user_id: only; email/UPN belong on identity docs.
        acl = source._access_control_for_members(TEAM_MEMBERS)
        assert sorted(acl) == ["user_id:user-alice", "user_id:user-bob"]
        assert not any(token.startswith("email:") for token in acl)
        assert not any(token.startswith("user:") for token in acl)


@pytest.mark.asyncio
async def test_access_control_for_members_ignores_membership_id_without_user_id():
    """Graph conversationMember ``id`` is not an Entra oid — do not stamp it."""
    async with create_teams_source() as source:
        members = [
            {
                "id": "membership-row-not-entra",
                "displayName": "Bot or incomplete member",
                # no userId
            },
            {
                "id": "pcm-1",
                "displayName": "Alice",
                "userId": "user-alice",
            },
        ]
        acl = source._access_control_for_members(members)
        assert acl == ["user_id:user-alice"]
        assert "user_id:membership-row-not-entra" not in acl


@pytest.mark.asyncio
async def test_profile_from_graph_user_keeps_mail_and_upn_separate():
    from connectors.sources.microsoft_teams.datasource import (
        _mail_from_graph_user,
        _profile_from_graph_user,
        _upn_from_graph_user,
    )

    assert (
        _mail_from_graph_user(
            {"mail": "mail@example.com", "userPrincipalName": "upn@example.com"}
        )
        == "mail@example.com"
    )
    assert (
        _mail_from_graph_user({"mail": "", "userPrincipalName": "upn@example.com"})
        is None
    )
    assert (
        _upn_from_graph_user(
            {"mail": "mail@example.com", "userPrincipalName": "upn@example.com"}
        )
        == "upn@example.com"
    )
    profile = _profile_from_graph_user(
        {
            "displayName": "Alice",
            "mail": "",
            "userPrincipalName": "upn@example.com",
        }
    )
    assert profile["email"] == ""
    assert profile["user"] == "upn@example.com"
    assert profile["name"] == "Alice"


@pytest.mark.asyncio
async def test_user_access_control_doc_keeps_email_and_upn_dialects():
    async with create_teams_source(use_document_level_security=True) as source:
        source._features = Mock()
        source._features.document_level_security_enabled = Mock(return_value=True)
        source._user_profiles = {
            "user-carol": {
                "name": "Carol",
                "email": "",
                "user": "carol_guest#EXT#@example.com",
            }
        }
        doc = source._user_access_control_doc("user-carol")
        assert doc["identity"]["user_id"] == "user_id:user-carol"
        assert doc["identity"]["email"] is None
        assert doc["identity"]["user"] == "user:carol_guest#EXT#@example.com"
        assert (
            "user_id:user-carol" in doc["query"]["template"]["params"]["access_control"]
        )
        assert (
            "user:carol_guest#EXT#@example.com"
            in doc["query"]["template"]["params"]["access_control"]
        )


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
        source.client.get_team_channels = MagicMock(
            return_value=AsyncIterator([CHANNELS])
        )
        source.client.get_chats = MagicMock(return_value=AsyncIterator([CHATS]))
        source.client.get_chat_members = MagicMock(
            return_value=AsyncIterator([CHAT_MEMBERS])
        )
        source.client.get_users_by_ids = AsyncMock(side_effect=_graph_users_by_ids)

        docs = []
        async for doc in source.get_access_control():
            docs.append(doc)

        ids = [doc["_id"] for doc in docs]
        # alice and bob appear in both team and chat, but must be deduped
        assert sorted(ids) == ["user-alice", "user-bob"]
        alice = next(doc for doc in docs if doc["_id"] == "user-alice")
        assert alice["identity"]["email"] == "email:alice@example.com"
        assert alice["identity"]["user"] == "user:alice@example.com"
        source.client.get_chats.assert_called()
        source.client.get_chat_members.assert_called()
        source.client.get_users_by_ids.assert_called()


@pytest.mark.asyncio
async def test_get_access_control_includes_private_channel_members():
    async with create_teams_source(use_document_level_security=True) as source:
        source._features = Mock()
        source._features.document_level_security_enabled = Mock(return_value=True)
        source.client.get_teams = MagicMock(return_value=AsyncIterator([TEAMS]))
        source.client.get_team_members = MagicMock(
            return_value=AsyncIterator([[TEAM_MEMBERS[0]]])  # alice only on team
        )
        source.client.get_team_channels = MagicMock(
            return_value=AsyncIterator([[PRIVATE_CHANNEL]])
        )
        # shared/private channel adds bob, who is not on the team roster above
        source.client.get_channel_members = MagicMock(
            return_value=AsyncIterator(
                [
                    [
                        {
                            "id": "pcm-bob",
                            "displayName": "Bob",
                            "userId": "user-bob",
                            "email": "bob@example.com",
                        }
                    ]
                ]
            )
        )
        source.client.get_chats = MagicMock(return_value=AsyncIterator([]))
        source.client.get_users_by_ids = AsyncMock(side_effect=_graph_users_by_ids)

        ids = [doc["_id"] async for doc in source.get_access_control()]
        assert sorted(ids) == ["user-alice", "user-bob"]
        source.client.get_channel_members.assert_called()
        source.client.get_users_by_ids.assert_called()


@pytest.mark.asyncio
async def test_get_access_control_raises_on_permissions_missing():
    async with create_teams_source(use_document_level_security=True) as source:
        source._features = Mock()
        source._features.document_level_security_enabled = Mock(return_value=True)
        source.client.get_teams = MagicMock(side_effect=PermissionsMissing())

        with pytest.raises(PermissionsMissing):
            async for _doc in source.get_access_control():
                pass


@pytest.mark.asyncio
async def test_get_access_control_skips_when_disabled():
    async with create_teams_source(use_document_level_security=False) as source:
        source._features = None
        docs = [doc async for doc in source.get_access_control()]
        assert docs == []


# -- get_docs ----------------------------------------------------------------


def _mock_client_for_get_docs(source, with_attachments=True):
    source.client.get_teams = MagicMock(return_value=AsyncIterator([TEAMS]))
    source.client.get_team = AsyncMock(return_value=TEAMS[0])
    source.client.get_team_members = MagicMock(
        return_value=AsyncIterator([TEAM_MEMBERS])
    )
    source.client.get_team_channels = MagicMock(
        side_effect=lambda *a, **k: AsyncIterator([CHANNELS])
    )
    source.client.get_channel_members = MagicMock(
        side_effect=lambda *a, **k: AsyncIterator([PRIVATE_CHANNEL_MEMBERS])
    )
    source.client.get_channel_messages = MagicMock(
        return_value=AsyncIterator([CHANNEL_MESSAGES])
    )

    async def _replies(_team_id, _channel_id, message_id):
        if message_id == "message-1":
            yield CHANNEL_REPLIES
        return

    source.client.get_channel_message_replies = MagicMock(side_effect=_replies)
    source.client.get_channel_file = AsyncMock(return_value=CHANNEL_FILES_FOLDER)
    source.client.get_channel_drive_children = MagicMock(
        return_value=AsyncIterator(CHANNEL_DRIVE_CHILDREN)
    )
    source.client.get_chats = MagicMock(side_effect=lambda *a, **k: AsyncIterator([CHATS]))
    source.client.get_chat_members = MagicMock(
        side_effect=lambda *a, **k: AsyncIterator([CHAT_MEMBERS])
    )
    source.client.get_chat_messages = MagicMock(
        return_value=AsyncIterator([CHAT_MESSAGES])
    )
    source.client.get_drive_item_by_content_url = AsyncMock(return_value=CHAT_FILE)
    source.client.get_users_by_ids = AsyncMock(side_effect=_graph_users_by_ids)


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
            TeamsObjectType.USER.value,
            TeamsObjectType.CHANNEL.value,
            TeamsObjectType.CHANNEL_MESSAGE.value,
            TeamsObjectType.FILE.value,
            TeamsObjectType.CHAT.value,
            TeamsObjectType.CHAT_MESSAGE.value,
        }

        users = [doc for doc in docs if doc["type"] == TeamsObjectType.USER.value]
        assert {u["_id"] for u in users} == {"user-alice", "user-bob"}
        team = next(doc for doc in docs if doc["type"] == TeamsObjectType.TEAM.value)
        assert team["member_ids"] == ["user-alice", "user-bob"]
        channel = next(
            doc for doc in docs if doc["type"] == TeamsObjectType.CHANNEL.value
        )
        assert channel["member_ids"] == ["user-alice", "user-bob"]
        chat = next(doc for doc in docs if doc["type"] == TeamsObjectType.CHAT.value)
        assert chat["member_ids"] == ["user-alice", "user-bob"]
        files = [doc for doc in docs if doc["type"] == TeamsObjectType.FILE.value]
        assert {f["_id"] for f in files} == {"file-1", "chat-file-1"}
        assert all(f.get("title") for f in files)
        channel_file = next(f for f in files if f["_id"] == "file-1")
        assert channel_file["channel_id"] == "channel-1"
        assert channel_file["channel_title"] == "General"
        assert "chat_id" not in channel_file
        chat_file = next(f for f in files if f["_id"] == "chat-file-1")
        assert chat_file["chat_id"] == "chat-1"
        assert chat_file["chat_title"] == "Project chat"
        assert "channel_id" not in chat_file
        chat_msg = next(
            doc for doc in docs if doc["type"] == TeamsObjectType.CHAT_MESSAGE.value
        )
        assert chat_msg["attachments"] == [{"id": "chat-file-1", "title": "doc.txt"}]


@pytest.mark.asyncio
async def test_get_docs_emits_user_for_chat_only_participant():
    """Chat-only tenant users get User docs when they appear in a synced chat."""
    chat_members_with_guest = [
        CHAT_MEMBERS[0],
        {
            "id": "cm-dave",
            "displayName": "Dave",
            "userId": "user-dave",
            "email": "dave@example.com",
        },
    ]
    async with create_teams_source(fetch_attachment_content=False) as source:
        source.client.get_teams = MagicMock(return_value=AsyncIterator([TEAMS]))
        source.client.get_team = AsyncMock(return_value=TEAMS[0])
        source.client.get_team_members = MagicMock(
            return_value=AsyncIterator([[TEAM_MEMBERS[0]]])
        )
        source.client.get_team_channels = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([CHANNELS])
        )
        source.client.get_channel_messages = MagicMock(
            return_value=AsyncIterator([[]])
        )
        source.client.get_channel_message_replies = MagicMock(
            return_value=AsyncIterator([])
        )
        source.client.get_chats = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([CHATS])
        )
        source.client.get_chat_members = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([chat_members_with_guest])
        )
        source.client.get_chat_messages = MagicMock(return_value=AsyncIterator([[]]))
        source.client.get_users_by_ids = AsyncMock(side_effect=_graph_users_by_ids)

        docs = [doc async for doc, _download in source.get_docs()]
        users = {doc["_id"] for doc in docs if doc["type"] == TeamsObjectType.USER.value}
        assert users == {"user-alice", "user-dave"}
        chat = next(doc for doc in docs if doc["type"] == TeamsObjectType.CHAT.value)
        assert chat["member_ids"] == ["user-alice", "user-dave"]


@pytest.mark.asyncio
async def test_get_docs_emits_user_for_private_channel_only_member():
    """Private-channel members who are not on the team roster still get User docs."""
    async with create_teams_source(fetch_attachment_content=False) as source:
        source.client.get_teams = MagicMock(return_value=AsyncIterator([TEAMS]))
        source.client.get_team = AsyncMock(return_value=TEAMS[0])
        source.client.get_team_members = MagicMock(
            return_value=AsyncIterator([[TEAM_MEMBERS[0]]])
        )
        source.client.get_team_channels = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([[PRIVATE_CHANNEL]])
        )
        source.client.get_channel_members = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator(
                [
                    [
                        {
                            "id": "pcm-bob",
                            "displayName": "Bob",
                            "userId": "user-bob",
                            "email": "bob@example.com",
                        }
                    ]
                ]
            )
        )
        source.client.get_channel_messages = MagicMock(
            return_value=AsyncIterator([[]])
        )
        source.client.get_channel_message_replies = MagicMock(
            return_value=AsyncIterator([])
        )
        source.client.get_chats = MagicMock(return_value=AsyncIterator([]))
        source.client.get_users_by_ids = AsyncMock(side_effect=_graph_users_by_ids)

        docs = [doc async for doc, _download in source.get_docs()]
        users = {doc["_id"] for doc in docs if doc["type"] == TeamsObjectType.USER.value}
        assert users == {"user-alice", "user-bob"}


@pytest.mark.asyncio
async def test_get_docs_user_email_from_graph_not_membership():
    """User docs and ACLs use /users profiles even when membership email is empty."""
    members_without_email = [
        {
            "id": "membership-1",
            "displayName": "Alice",
            "userId": "user-alice",
            "email": None,
        },
        {
            "id": "membership-2",
            "displayName": "Bob",
            "userId": "user-bob",
            "email": "",
        },
    ]
    async with create_teams_source(use_document_level_security=True) as source:
        source._features = Mock()
        source._features.document_level_security_enabled = Mock(return_value=True)
        _mock_client_for_get_docs(source)
        source.client.get_team_members = MagicMock(
            return_value=AsyncIterator([members_without_email])
        )

        docs = [doc async for doc, _download in source.get_docs()]

        users = {
            doc["_id"]: doc for doc in docs if doc["type"] == TeamsObjectType.USER.value
        }
        assert users["user-alice"]["email"] == "alice@example.com"
        assert users["user-alice"]["upn"] == "alice@example.com"
        assert users["user-bob"]["email"] == "bob@example.com"
        assert users["user-bob"]["upn"] == "bob@example.com"
        assert ACCESS_CONTROL not in users["user-alice"]
        assert ACCESS_CONTROL not in users["user-bob"]

        chat = next(doc for doc in docs if doc["type"] == TeamsObjectType.CHAT.value)
        assert sorted(chat[ACCESS_CONTROL]) == [
            "user_id:user-alice",
            "user_id:user-bob",
        ]
        source.client.get_users_by_ids.assert_called()
        # Chat discovery still uses team-member ids
        source.client.get_chats.assert_called()
        called_ids = set(source.client.get_chats.call_args[0][0])
        assert called_ids == {"user-alice", "user-bob"}


@pytest.mark.asyncio
async def test_get_docs_user_profile_404_emits_user_without_email():
    members_without_email = [
        {
            "id": "membership-1",
            "displayName": "Alice",
            "userId": "user-alice",
            "email": None,
        },
        {
            "id": "membership-2",
            "displayName": "Bob",
            "userId": "user-bob",
            "email": None,
        },
    ]

    async def _partial_users(user_ids):
        # Alice resolves; Bob is missing from Graph
        return {
            uid: GRAPH_USERS[uid]
            for uid in user_ids
            if uid == "user-alice" and uid in GRAPH_USERS
        }

    async with create_teams_source(use_document_level_security=True) as source:
        source._features = Mock()
        source._features.document_level_security_enabled = Mock(return_value=True)
        _mock_client_for_get_docs(source)
        source.client.get_team_members = MagicMock(
            return_value=AsyncIterator([members_without_email])
        )
        source.client.get_users_by_ids = AsyncMock(side_effect=_partial_users)
        source.client.get_chats = MagicMock(return_value=AsyncIterator([]))

        docs = [doc async for doc, _download in source.get_docs()]
        users = {
            doc["_id"]: doc for doc in docs if doc["type"] == TeamsObjectType.USER.value
        }
        assert users["user-alice"]["email"] == "alice@example.com"
        assert users["user-alice"]["upn"] == "alice@example.com"
        assert users["user-bob"]["email"] == ""
        assert users["user-bob"]["upn"] == ""
        assert users["user-bob"]["name"] == "Bob"  # membership fallback
        assert ACCESS_CONTROL not in users["user-bob"]
        team = next(doc for doc in docs if doc["type"] == TeamsObjectType.TEAM.value)
        assert sorted(team[ACCESS_CONTROL]) == [
            "user_id:user-alice",
            "user_id:user-bob",
        ]
        assert "email:bob@example.com" not in team[ACCESS_CONTROL]
        # Chat discovery still receives both team member ids
        called_ids = set(source.client.get_chats.call_args[0][0])
        assert called_ids == {"user-alice", "user-bob"}


@pytest.mark.asyncio
async def test_get_docs_raises_when_user_profile_permission_missing():
    async with create_teams_source(fetch_attachment_content=False) as source:
        source.client.get_teams = MagicMock(return_value=AsyncIterator([TEAMS]))
        source.client.get_team_members = MagicMock(
            return_value=AsyncIterator([TEAM_MEMBERS])
        )
        source.client.get_users_by_ids = AsyncMock(side_effect=PermissionsMissing())
        source.client.get_team_channels = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([CHANNELS])
        )
        source.client.get_chats = MagicMock(return_value=AsyncIterator([]))

        with pytest.raises(PermissionsMissing, match="Enumeration failed"):
            async for _doc, _download in source.get_docs():
                pass


@pytest.mark.asyncio
async def test_client_get_users_by_ids_batch_success():
    client = build_client()

    def _batch_response(payload):
        responses = []
        for request in payload["requests"]:
            user_id = request["id"]
            responses.append(
                {"id": user_id, "status": 200, "body": GRAPH_USERS[user_id]}
            )
        return {"responses": responses}

    client._graph_api_client = FakeGraphSession(posts={"/$batch": _batch_response})
    users = await client.get_users_by_ids(["user-alice", "user-bob"])
    await client.close()
    assert set(users) == {"user-alice", "user-bob"}
    assert users["user-alice"]["mail"] == "alice@example.com"


@pytest.mark.asyncio
async def test_client_get_users_by_ids_falls_back_on_batch_not_found():
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        raises={"/$batch": NotFound()},
        fetches={
            "/users/user-alice": GRAPH_USERS["user-alice"],
            "/users/user-bob": GRAPH_USERS["user-bob"],
        },
    )
    users = await client.get_users_by_ids(["user-alice", "user-bob"])
    await client.close()
    assert set(users) == {"user-alice", "user-bob"}


@pytest.mark.asyncio
async def test_client_get_user_swallows_not_found():
    client = build_client()
    client._graph_api_client = FakeGraphSession(raises={"/users/missing": NotFound()})
    assert await client.get_user("missing") is None
    await client.close()
    assert client._skipped["users"] == 1


@pytest.mark.asyncio
async def test_client_get_users_by_ids_propagates_permissions_missing():
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        raises={"/$batch": PermissionsMissing()}
    )
    with pytest.raises(PermissionsMissing):
        await client.get_users_by_ids(["user-alice"])
    await client.close()


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

        assert TeamsObjectType.FILE.value not in types


@pytest.mark.asyncio
async def test_get_docs_raises_when_teams_permission_missing():
    # One-sided enumeration failure must not succeed: that would wipe the other
    # corpus on the next full sync.
    async with create_teams_source() as source:
        source.client.get_teams = MagicMock(side_effect=PermissionsMissing())
        source.client.get_users_by_ids = AsyncMock(side_effect=_graph_users_by_ids)
        source.client.get_chats = MagicMock(return_value=AsyncIterator([CHATS]))
        source.client.get_chat_members = MagicMock(
            return_value=AsyncIterator([CHAT_MEMBERS])
        )
        source.client.get_chat_messages = MagicMock(
            return_value=AsyncIterator([CHAT_MESSAGES])
        )
        source.client.get_drive_item_by_content_url = AsyncMock(return_value=CHAT_FILE)

        with pytest.raises(PermissionsMissing, match="Enumeration failed"):
            async for _doc, _download in source.get_docs():
                pass


@pytest.mark.asyncio
async def test_get_docs_raises_when_chats_permission_missing():
    async with create_teams_source(fetch_attachment_content=False) as source:
        source.client.get_teams = MagicMock(return_value=AsyncIterator([TEAMS]))
        source.client.get_team_members = MagicMock(
            return_value=AsyncIterator([TEAM_MEMBERS])
        )
        source.client.get_team_channels = MagicMock(
            return_value=AsyncIterator([CHANNELS])
        )
        source.client.get_channel_messages = MagicMock(return_value=AsyncIterator([[]]))
        source.client.get_channel_message_replies = MagicMock(
            return_value=AsyncIterator([])
        )
        source.client.get_users_by_ids = AsyncMock(side_effect=_graph_users_by_ids)
        source.client.get_chats = MagicMock(side_effect=PermissionsMissing())

        with pytest.raises(PermissionsMissing, match="Enumeration failed"):
            async for _doc, _download in source.get_docs():
                pass


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
            "title": "report.txt",
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
            "title": "report.txt",
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
async def test_get_content_works_after_sink_pops_id():
    """Framework pops ``_id`` before deferred download; content must still extract."""
    async with create_teams_source() as source:
        attachment = {
            "_id": "file-1",
            "id": "file-1",
            "_timestamp": "2023-08-16T04:47:29Z",
            "title": "report.txt",
            "size_in_bytes": 42,
        }
        # Mirror SyncJobRunner / Extractor: pop ``_id``, keep ``id``.
        doc_id = attachment.pop("_id")
        attachment["id"] = doc_id

        source.client.download_drive_item = AsyncMock()
        source.can_file_be_downloaded = Mock(return_value=True)
        handle = AsyncMock(
            return_value={
                "_id": doc_id,
                "_timestamp": attachment["_timestamp"],
                "body": "hello",
            }
        )
        source.handle_file_content_extraction = handle

        @asynccontextmanager
        async def fake_temp(_ext):
            buffer = MagicMock()
            buffer.name = "/tmp/report.txt"
            buffer.close = AsyncMock()
            yield buffer

        source.create_temp_file = fake_temp

        result = await source.get_content(
            attachment, drive_id="drive-123", item_id=doc_id, doit=True
        )

        assert result is not None
        assert result["_id"] == doc_id
        assert result["body"] == "hello"
        source.client.download_drive_item.assert_awaited_once()


@pytest.mark.asyncio
async def test_emit_file_queues_once_and_merges_parents_on_rediscovery():
    """Same driveItem from message + folder must not double-queue the File."""
    drive_item = {
        "id": "file-1",
        "name": "report.txt",
        "webUrl": "https://example.com/report.txt",
        "size": 42,
        "lastModifiedDateTime": "2023-08-16T04:47:29Z",
        "createdDateTime": "2023-08-16T04:47:26Z",
        "file": {"mimeType": "text/plain"},
        "parentReference": {"driveId": "drive-123"},
    }
    async with create_teams_source() as source:
        source.queue = MagicMock()
        source.queue.put = AsyncMock()

        ref1 = await source._emit_file(
            drive_item,
            ["user_id:user-alice"],
            channel_id="channel-1",
            channel_title="General",
        )
        ref2 = await source._emit_file(
            drive_item,
            ["user_id:user-bob"],
            channel_id="channel-1",
            channel_title="General",
            chat_id="chat-1",
            chat_title="Side chat",
        )

        assert ref1 == {"id": "file-1", "title": "report.txt"}
        assert ref2 == {"id": "file-1", "title": "report.txt"}
        assert source.queue.put.await_count == 1

        queued_doc, download = source.queue.put.await_args.args[0]
        assert download is not None
        assert queued_doc is source._file_docs["file-1"]
        assert queued_doc["channel_id"] == "channel-1"
        assert queued_doc["channel_title"] == "General"
        assert queued_doc["chat_id"] == "chat-1"
        assert queued_doc["chat_title"] == "Side chat"


@pytest.mark.asyncio
async def test_emit_file_merges_acl_on_same_doc_when_dls_enabled():
    drive_item = {
        "id": "file-1",
        "name": "report.txt",
        "size": 42,
        "lastModifiedDateTime": "2023-08-16T04:47:29Z",
        "createdDateTime": "2023-08-16T04:47:26Z",
        "file": {"mimeType": "text/plain"},
        "parentReference": {"driveId": "drive-123"},
    }
    async with create_teams_source(use_document_level_security=True) as source:
        source._features = Mock()
        source._features.document_level_security_enabled = Mock(return_value=True)
        source.queue = MagicMock()
        source.queue.put = AsyncMock()

        await source._emit_file(drive_item, ["user_id:user-alice"], channel_id="c1")
        await source._emit_file(drive_item, ["user_id:user-bob"], channel_id="c1")

        assert source.queue.put.await_count == 1
        doc = source._file_docs["file-1"]
        assert sorted(doc[ACCESS_CONTROL]) == [
            "user_id:user-alice",
            "user_id:user-bob",
        ]


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


# -- Message processing edge cases -------------------------------------------


def _empty_body_message(with_attachment):
    return {
        "id": "msg-empty",
        "messageType": "message",
        "createdDateTime": "2023-08-16T04:47:55.794Z",
        "lastModifiedDateTime": "2023-08-16T04:47:55.794Z",
        "deletedDateTime": None,
        "webUrl": None,
        "from": {"user": {"id": "user-alice", "displayName": "Alice"}},
        "body": {"contentType": "html", "content": ""},
        "attachments": [
            {
                "id": "a1",
                "name": "f.txt",
                "contentType": "reference",
                "contentUrl": "https://example.com/f.txt",
            }
        ]
        if with_attachment
        else [],
    }


@pytest.mark.asyncio
async def test_process_channel_message_indexes_attachment_only_message():
    async with create_teams_source() as source:
        source.client.get_drive_item_by_content_url = AsyncMock(
            return_value={
                "id": "drive-f",
                "name": "f.txt",
                "webUrl": "https://example.com/f.txt",
                "size": 1,
                "lastModifiedDateTime": "2023-08-16T04:47:55.794Z",
                "createdDateTime": "2023-08-16T04:47:55.794Z",
                "parentReference": {"driveId": "drive-1"},
            }
        )
        await source._process_channel_message(
            _empty_body_message(with_attachment=True), "channel-1", "General", []
        )
        docs = []
        while not source.queue.empty():
            _, item = source.queue.get_nowait()
            docs.append(item[0])
        message = next(
            d for d in docs if d["type"] == TeamsObjectType.CHANNEL_MESSAGE.value
        )
        assert message["_id"] == "msg-empty"
        assert message["attachments"] == [{"id": "drive-f", "title": "f.txt"}]
        file_doc = next(d for d in docs if d["type"] == TeamsObjectType.FILE.value)
        assert file_doc["channel_id"] == "channel-1"
        assert file_doc["channel_title"] == "General"
        assert "chat_id" not in file_doc


@pytest.mark.asyncio
async def test_process_channel_message_drops_truly_empty_message():
    async with create_teams_source() as source:
        await source._process_channel_message(
            _empty_body_message(with_attachment=False), "channel-1", "General", []
        )
        assert source.queue.empty()


@pytest.mark.asyncio
async def test_process_chat_message_indexes_attachment_only_message():
    async with create_teams_source() as source:
        source.client.get_drive_item_by_content_url = AsyncMock(
            return_value={
                "id": "drive-f",
                "name": "f.txt",
                "webUrl": "https://example.com/f.txt",
                "size": 1,
                "lastModifiedDateTime": "2023-08-16T04:47:55.794Z",
                "createdDateTime": "2023-08-16T04:47:55.794Z",
                "parentReference": {"driveId": "drive-1"},
            }
        )
        await source._process_chat_message(
            CHATS[0], _empty_body_message(with_attachment=True), "Alice,Bob", []
        )
        docs = []
        while not source.queue.empty():
            _, item = source.queue.get_nowait()
            docs.append(item[0])
        message = next(
            d for d in docs if d["type"] == TeamsObjectType.CHAT_MESSAGE.value
        )
        assert message["_id"] == "msg-empty"
        assert message["attachments"] == [{"id": "drive-f", "title": "f.txt"}]
        file_doc = next(d for d in docs if d["type"] == TeamsObjectType.FILE.value)
        assert file_doc["chat_id"] == "chat-1"
        assert file_doc["chat_title"] == "Project chat"
        assert "channel_id" not in file_doc


@pytest.mark.asyncio
async def test_process_message_handles_null_body_without_raising():
    async with create_teams_source() as source:
        null_body = _empty_body_message(with_attachment=False)
        null_body["body"] = None
        # Should not raise AttributeError; message is dropped (no text, no attachments)
        await source._process_channel_message(null_body, "channel-1", "General", [])
        await source._process_chat_message(CHATS[0], null_body, "Alice,Bob", [])
        assert source.queue.empty()


def test_message_body_and_subject_are_separate():
    payload = {
        "body": {"content": "<p>I just wanna see how all the lovely people doing</p>"},
        "subject": "Hey everyone, how's it going?",
    }
    assert (
        _message_body_text(payload)
        == "I just wanna see how all the lovely people doing"
    )
    assert _message_subject(payload) == "Hey everyone, how's it going?"


def test_message_body_empty_for_attachment_stub():
    payload = {
        "body": {
            "content": '<attachment id="93485830-1df1-4870-a0e3-129884eff6c6"></attachment>'
        },
        "subject": "Yo, here are some logs",
    }
    assert _message_body_text(payload) == ""
    assert _message_subject(payload) == "Yo, here are some logs"


@pytest.mark.asyncio
async def test_process_channel_message_keeps_subject_and_body_separate():
    async with create_teams_source() as source:
        message = {
            "id": "msg-both",
            "messageType": "message",
            "createdDateTime": "2023-08-16T04:47:55.794Z",
            "lastModifiedDateTime": "2023-08-16T04:47:55.794Z",
            "deletedDateTime": None,
            "webUrl": "https://teams.example/msg-both",
            "replyToId": None,
            "subject": "Hey everyone, how's it going?",
            "from": {"user": {"id": "user-alice", "displayName": "Alice"}},
            "body": {
                "contentType": "html",
                "content": "<p>I just wanna see how all the lovely people doing</p>",
            },
            "attachments": [],
        }
        await source._process_channel_message(message, "channel-1", "General", [])
        _, (doc, _download) = source.queue.get_nowait()
        assert doc["subject"] == "Hey everyone, how's it going?"
        assert doc["message"] == "I just wanna see how all the lovely people doing"
        assert doc["sender_name"] == "Alice"
        assert doc["sender_id"] == "user-alice"
        assert doc["channel_id"] == "channel-1"
        assert doc["channel_title"] == "General"
        assert doc["reply_to_id"] == ""


@pytest.mark.asyncio
async def test_process_channel_message_file_share_subject_only():
    async with create_teams_source() as source:
        source.client.get_drive_item_by_content_url = AsyncMock(
            return_value={
                "id": "drive-f",
                "name": "f.txt",
                "webUrl": "https://example.com/f.txt",
                "size": 1,
                "lastModifiedDateTime": "2023-08-16T04:47:55.794Z",
                "createdDateTime": "2023-08-16T04:47:55.794Z",
                "parentReference": {"driveId": "drive-1"},
            }
        )
        message = _empty_body_message(with_attachment=True)
        message["body"] = {
            "contentType": "html",
            "content": '<attachment id="a1"></attachment>',
        }
        message["subject"] = "Yo, here are some logs"
        message["replyToId"] = "root-1"
        await source._process_channel_message(message, "channel-1", "General", [])
        docs = []
        while not source.queue.empty():
            _, item = source.queue.get_nowait()
            docs.append(item[0])
        doc = next(
            d for d in docs if d["type"] == TeamsObjectType.CHANNEL_MESSAGE.value
        )
        assert doc["subject"] == "Yo, here are some logs"
        assert doc["message"] == ""
        assert doc["attachments"] == [{"id": "drive-f", "title": "f.txt"}]
        assert doc["sender_id"] == "user-alice"
        assert doc["reply_to_id"] == "root-1"


@pytest.mark.asyncio
async def test_process_channel_message_indexes_subject_only_without_attachments():
    async with create_teams_source() as source:
        message = _empty_body_message(with_attachment=False)
        message["subject"] = "Announcement only"
        await source._process_channel_message(message, "channel-1", "General", [])
        _, (doc, _download) = source.queue.get_nowait()
        assert doc["subject"] == "Announcement only"
        assert doc["message"] == ""


# -- Robustness: access control + total-failure safeguard --------------------


@pytest.mark.asyncio
async def test_get_docs_raises_when_both_enumerations_fail():
    async with create_teams_source() as source:
        source.client.get_teams = MagicMock(side_effect=PermissionsMissing())
        source.client.get_chats = MagicMock(side_effect=PermissionsMissing())

        with pytest.raises(PermissionsMissing, match="Enumeration failed"):
            async for _doc, _download in source.get_docs():
                pass


@pytest.mark.asyncio
async def test_get_docs_empty_tenant_does_not_raise():
    async with create_teams_source() as source:
        source.client.get_teams = MagicMock(return_value=AsyncIterator([]))
        source.client.get_chats = MagicMock(return_value=AsyncIterator([]))

        docs = [doc async for doc, _download in source.get_docs()]
        assert docs == []


@pytest.mark.asyncio
async def test_get_docs_raises_on_unexpected_enumeration_error():
    # A non-PermissionsMissing error during enumeration must fail the sync, not
    # hang it (the orchestrator runs as a pool task whose exception is swallowed
    # by ConcurrentTasks; get_docs re-raises the recorded error instead).
    async with create_teams_source() as source:
        source.client.get_teams = MagicMock(side_effect=RuntimeError("boom"))
        source.client.get_chats = MagicMock(return_value=AsyncIterator([]))

        async def drain():
            async for _doc, _download in source.get_docs():
                pass

        with pytest.raises(RuntimeError, match="boom"):
            await asyncio.wait_for(drain(), timeout=5)


@pytest.mark.asyncio
async def test_get_docs_raises_on_producer_error():
    # Producer exceptions must fail the sync. ConcurrentTasks removes finished
    # tasks before join returns, so get_docs re-raises the recorded error.
    async with create_teams_source(fetch_attachment_content=False) as source:
        source.client.get_teams = MagicMock(return_value=AsyncIterator([TEAMS]))
        source.client.get_team_members = MagicMock(
            side_effect=RuntimeError("team producer boom")
        )
        source.client.get_chats = MagicMock(return_value=AsyncIterator([]))

        async def drain():
            async for _doc, _download in source.get_docs():
                pass

        with pytest.raises(RuntimeError, match="team producer boom"):
            await asyncio.wait_for(drain(), timeout=5)


@pytest.mark.asyncio
async def test_get_docs_raises_when_channel_message_permission_missing():
    async with create_teams_source(fetch_attachment_content=False) as source:
        source.client.get_teams = MagicMock(return_value=AsyncIterator([TEAMS]))
        source.client.get_team_members = MagicMock(
            return_value=AsyncIterator([TEAM_MEMBERS])
        )
        source.client.get_users_by_ids = AsyncMock(side_effect=_graph_users_by_ids)
        source.client.get_team_channels = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([CHANNELS])
        )
        source.client.get_channel_messages = MagicMock(
            side_effect=PermissionsMissing("ChannelMessage.Read.All missing")
        )
        source.client.get_chats = MagicMock(return_value=AsyncIterator([]))

        with pytest.raises(PermissionsMissing, match="ChannelMessage.Read.All"):
            async for _doc, _download in source.get_docs():
                pass


@pytest.mark.asyncio
async def test_private_channel_uses_channel_member_acl_when_dls_enabled():
    async with create_teams_source(
        use_document_level_security=True, fetch_attachment_content=False
    ) as source:
        source._features = Mock()
        source._features.document_level_security_enabled = Mock(return_value=True)

        source.client.get_teams = MagicMock(return_value=AsyncIterator([TEAMS]))
        source.client.get_team_members = MagicMock(
            return_value=AsyncIterator([TEAM_MEMBERS])
        )
        source.client.get_users_by_ids = AsyncMock(side_effect=_graph_users_by_ids)
        source.client.get_team_channels = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([[PRIVATE_CHANNEL]])
        )
        source.client.get_channel_members = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([PRIVATE_CHANNEL_MEMBERS])
        )
        source.client.get_channel_messages = MagicMock(
            return_value=AsyncIterator(
                [
                    [
                        {
                            "id": "priv-msg-1",
                            "messageType": "message",
                            "createdDateTime": "2023-08-16T04:47:55.794Z",
                            "lastModifiedDateTime": "2023-08-16T04:47:55.794Z",
                            "deletedDateTime": None,
                            "webUrl": None,
                            "from": {"user": {"displayName": "Alice"}},
                            "body": {
                                "contentType": "html",
                                "content": "<div>secret</div>",
                            },
                            "attachments": [],
                        }
                    ]
                ]
            )
        )
        source.client.get_channel_message_replies = MagicMock(
            return_value=AsyncIterator([])
        )
        source.client.get_chats = MagicMock(return_value=AsyncIterator([]))

        channel_docs = []
        message_docs = []
        async for doc, _download in source.get_docs():
            if doc["type"] == TeamsObjectType.CHANNEL.value:
                channel_docs.append(doc)
            if doc["type"] == TeamsObjectType.CHANNEL_MESSAGE.value:
                message_docs.append(doc)

        assert len(channel_docs) == 1
        assert len(message_docs) == 1
        assert channel_docs[0]["member_ids"] == ["user-alice"]
        # Only Alice (channel member), not Bob (team member outside the channel)
        assert channel_docs[0][ACCESS_CONTROL] == ["user_id:user-alice"]
        assert message_docs[0][ACCESS_CONTROL] == channel_docs[0][ACCESS_CONTROL]
        source.client.get_channel_members.assert_called()


@pytest.mark.asyncio
async def test_private_channel_skipped_when_dls_on_and_members_unresolved():
    async with create_teams_source(
        use_document_level_security=True, fetch_attachment_content=False
    ) as source:
        source._features = Mock()
        source._features.document_level_security_enabled = Mock(return_value=True)

        source.client.get_teams = MagicMock(return_value=AsyncIterator([TEAMS]))
        source.client.get_team_members = MagicMock(
            return_value=AsyncIterator([TEAM_MEMBERS])
        )
        source.client.get_users_by_ids = AsyncMock(side_effect=_graph_users_by_ids)
        source.client.get_team_channels = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([[PRIVATE_CHANNEL]])
        )
        # Empty generator simulates unresolved channel members (e.g. NotFound)
        source.client.get_channel_members = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([])
        )
        source.client.get_channel_messages = MagicMock(
            return_value=AsyncIterator([CHANNEL_MESSAGES])
        )
        source.client.get_channel_message_replies = MagicMock(
            return_value=AsyncIterator([])
        )
        source.client.get_chats = MagicMock(return_value=AsyncIterator([]))

        docs = [doc async for doc, _download in source.get_docs()]
        types = {doc["type"] for doc in docs}

        assert TeamsObjectType.TEAM.value in types
        assert TeamsObjectType.CHANNEL.value not in types
        assert TeamsObjectType.CHANNEL_MESSAGE.value not in types
        source.client.get_channel_messages.assert_not_called()


@pytest.mark.asyncio
async def test_client_get_channel_members_propagates_permissions_missing():
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        raises={"/channels/channel-1/members": PermissionsMissing()}
    )
    with pytest.raises(PermissionsMissing):
        async for _page in client.get_channel_members("team-1", "channel-1"):
            pass
    await client.close()


@pytest.mark.asyncio
async def test_client_get_channel_members_swallows_not_found():
    client = build_client()
    client._graph_api_client = FakeGraphSession(
        raises={"/channels/channel-1/members": NotFound()}
    )
    result = []
    async for page in client.get_channel_members("team-1", "channel-1"):
        result.extend(page)
    await client.close()
    assert result == []
    assert client._skipped["channels' members"] == 1


@pytest.mark.asyncio
async def test_get_docs_completes_under_low_concurrency():
    # Regression: channels used to be scheduled into the same bounded pool from
    # within team_producer. With a small pool and multiple teams that each have a
    # channel, that nested scheduling deadlocked. Channels are now processed
    # inline, so the sync must complete regardless of pool size.
    two_teams = [
        {
            "id": "team-1",
            "displayName": "Team One",
            "webUrl": "https://teams.example/1",
            "createdDateTime": "2023-08-16T04:46:53.056Z",
        },
        {
            "id": "team-2",
            "displayName": "Team Two",
            "webUrl": "https://teams.example/2",
            "createdDateTime": "2023-08-16T04:46:53.056Z",
        },
    ]
    async with create_teams_source(fetch_attachment_content=False) as source:
        source.fetchers = ConcurrentTasks(max_concurrency=2)
        source.client.get_teams = MagicMock(return_value=AsyncIterator([two_teams]))
        source.client.get_team = AsyncMock(
            side_effect=lambda team_id: next(
                (t for t in two_teams if t["id"] == team_id), None
            )
        )
        source.client.get_team_members = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([TEAM_MEMBERS])
        )
        source.client.get_users_by_ids = AsyncMock(side_effect=_graph_users_by_ids)
        source.client.get_team_channels = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([CHANNELS])
        )
        source.client.get_channel_messages = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([[]])
        )
        source.client.get_channel_message_replies = MagicMock(
            side_effect=lambda *a, **k: AsyncIterator([])
        )
        source.client.get_chats = MagicMock(return_value=AsyncIterator([]))

        async def drain():
            return [doc async for doc, _download in source.get_docs()]

        docs = await asyncio.wait_for(drain(), timeout=5)

        types = {doc["type"] for doc in docs}
        assert TeamsObjectType.TEAM.value in types
        assert TeamsObjectType.CHANNEL.value in types
        # both teams' channels were produced inline
        channel_ids = [
            doc["_id"] for doc in docs if doc["type"] == TeamsObjectType.CHANNEL.value
        ]
        assert len(channel_ids) == 2


@pytest.mark.asyncio
async def test_get_docs_calls_log_skip_summary():
    async with create_teams_source() as source:
        _mock_client_for_get_docs(source)
        source.client.log_skip_summary = Mock()

        async for _doc, _download in source.get_docs():
            pass

        source.client.log_skip_summary.assert_called_once()


# -- Skip visibility ---------------------------------------------------------


@pytest.mark.asyncio
async def test_client_counts_skipped_resources():
    client = build_client()
    client._graph_api_client = FakeGraphSession(raises={"/members": NotFound()})
    async for _ in client.get_team_members("team-1"):
        pass
    await client.close()
    assert client._skipped["teams' members"] == 1


@pytest.mark.asyncio
async def test_log_skip_summary_warns_when_skips_recorded():
    client = build_client()
    client._logger = Mock()
    client._skipped["channels' messages"] = 3
    client.log_skip_summary()
    await client.close()
    client._logger.warning.assert_called_once()
    warning = client._logger.warning.call_args[0][0]
    assert "not found" in warning
    # counters are reset after logging
    assert not client._skipped


@pytest.mark.asyncio
async def test_log_skip_summary_silent_when_nothing_skipped():
    client = build_client()
    client._logger = Mock()
    client.log_skip_summary()
    await client.close()
    client._logger.warning.assert_not_called()


# -- Config validation -------------------------------------------------------


@pytest.mark.asyncio
async def test_incremental_sync_disabled():
    assert MicrosoftTeamsDataSource.incremental_sync_enabled is False


@asynccontextmanager
async def _source_with_config(**overrides):
    config = {
        "tenant_id": "tenant-id",
        "client_id": "client-id",
        "auth_method": "secret",
        "secret_value": "secret",
        "certificate": "certificate",
        "private_key": "private-key",
        "use_document_level_security": False,
        "fetch_attachment_content": True,
    }
    config.update(overrides)
    async with create_source(MicrosoftTeamsDataSource, **config) as source:
        yield source


@pytest.mark.asyncio
async def test_validate_config_requires_secret_value():
    async with _source_with_config(auth_method="secret", secret_value="") as source:
        # Prevent the client cached_property from building with invalid creds
        source.client = MagicMock(close=AsyncMock())
        with patch.object(BaseDataSource, "validate_config", new=AsyncMock()):
            with pytest.raises(ConfigurableFieldValueError):
                await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_requires_certificate_and_key():
    async with _source_with_config(
        auth_method="certificate", certificate="cert", private_key=""
    ) as source:
        source.client = MagicMock(close=AsyncMock())
        with patch.object(BaseDataSource, "validate_config", new=AsyncMock()):
            with pytest.raises(ConfigurableFieldValueError):
                await source.validate_config()
