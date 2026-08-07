#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft Teams client.

The connector uses application-only authentication (client secret or certificate)
and tenant-wide Microsoft Graph application permissions (no Teams app install /
RSC / WhereInstalled):

- Teams: `Team.ReadBasic.All`, `TeamMember.Read.All`
- Channels: `Channel.ReadBasic.All`, `ChannelMember.Read.All`,
  `ChannelMessage.Read.All`
- Chats: `Chat.ReadBasic.All` (metadata/members), `Chat.Read.All` (messages).
  Chat discovery walks a provided set of team-member user ids and calls
  `GET /users/{id}/chats` (there is no tenant-wide app-only `GET /chats`).
  Chat membership for DLS uses dedicated `GET /chats/{id}/members`.
  Channel replies use dedicated `GET .../messages/{id}/replies`.
- Users: `User.ReadBasic.All` to resolve profiles (`mail` / UPN / displayName)
  for User docs and consistent DLS ``email:`` tokens. Chat discovery itself
  does not need User.Read*; there is no tenant-wide user list sync.
- Attachments: `Files.Read.All` (required when "Fetch attachment content" is on;
  validated via the app token ``roles`` claim)

`ChannelMessage.Read.All` and `Chat.Read.All` are protected Teams APIs: admin
consent alone may not be enough until Microsoft grants protected API access for
the app in the tenant.

Missing resources (`NotFound`) are soft-skipped where that is normal (e.g. no
files folder). Permission failures (`PermissionsMissing`) are not swallowed —
they fail the sync so a misconfigured app is visible.
"""

import base64
import json
import os
from collections import Counter
from collections.abc import AsyncIterator, Iterable
from enum import Enum

import aiohttp
from connectors_sdk.logger import logger

from connectors.sources.shared.microsoft.graph import (
    EntraAPIToken,
    GraphAPIToken,
    MicrosoftAPISession,
    NotFound,
    PermissionsMissing,
)

GRAPH_ACQUIRE_TOKEN_URL = "https://graph.microsoft.com/.default"  # noqa S105
DEFAULT_PARALLEL_CONNECTION_COUNT = 10
FILES_READ_ALL_ROLE = "Files.Read.All"
USER_PROFILE_SELECT = "id,mail,userPrincipalName,displayName"
USER_PROFILE_BATCH_SIZE = 20

if "OVERRIDE_URL" in os.environ:
    logger.warning("x" * 50)
    logger.warning(
        f"MICROSOFT TEAMS CONNECTOR CALLS ARE REDIRECTED TO {os.environ['OVERRIDE_URL']}"
    )
    logger.warning("IT'S SUPPOSED TO BE USED ONLY FOR TESTING")
    logger.warning("x" * 50)
    BASE_URL = os.environ["OVERRIDE_URL"]
else:
    BASE_URL = "https://graph.microsoft.com/v1.0"


def encode_sharing_url(url):
    """Encode a sharing URL for ``GET /shares/{shareIdOrEncodedUrl}``.

    https://learn.microsoft.com/en-us/graph/api/shares-get
    """
    encoded = base64.urlsafe_b64encode(url.encode("utf-8")).decode("ascii").rstrip("=")
    return f"u!{encoded}"


class TeamsObjectType(Enum):
    """Document `type` values emitted by the connector."""

    TEAM = "Team"
    USER = "User"
    CHANNEL = "Channel"
    CHANNEL_MESSAGE = "Channel Message"
    CHAT = "Chat"
    CHAT_MESSAGE = "Chat Message"
    FILE = "File"


class EndSignal(Enum):
    ENUMERATION_FINISHED = "ENUMERATION_FINISHED"
    TEAM_TASK_FINISHED = "TEAM_TASK_FINISHED"
    CHAT_TASK_FINISHED = "CHAT_TASK_FINISHED"


class Schema:
    """Maps Elasticsearch document fields to Microsoft Graph fields."""

    def team(self):
        return {
            "_id": "id",
            "title": "displayName",
            "description": "description",
            "url": "webUrl",
            "creation_time": "createdDateTime",
        }

    def user(self):
        return {
            "_id": "id",
            "name": "displayName",
            "email": "mail",
        }

    def channel(self):
        return {
            "_id": "id",
            "url": "webUrl",
            "title": "displayName",
            "description": "description",
            "creation_time": "createdDateTime",
        }

    def channel_message(self):
        return {
            "_id": "id",
            "url": "webUrl",
            "_timestamp": "lastModifiedDateTime",
            "creation_time": "createdDateTime",
        }

    def file(self):
        return {
            "_id": "id",
            "title": "name",
            "url": "webUrl",
            "size_in_bytes": "size",
            "_timestamp": "lastModifiedDateTime",
            "creation_time": "createdDateTime",
        }

    def chat(self):
        return {
            "_id": "id",
            "title": "topic",
            "url": "webUrl",
            "chatType": "chatType",
            "_timestamp": "lastUpdatedDateTime",
            "creation_time": "createdDateTime",
        }

    def chat_message(self):
        return {
            "_id": "id",
            "_timestamp": "lastModifiedDateTime",
            "creation_time": "createdDateTime",
            "url": "webUrl",
        }


def _jwt_payload_roles(access_token):
    """Return the ``roles`` claim from an app-only access token (unsigned decode)."""
    try:
        payload_segment = access_token.split(".")[1]
        padding = "=" * (-len(payload_segment) % 4)
        payload = json.loads(
            base64.urlsafe_b64decode(payload_segment + padding).decode("utf-8")
        )
    except (IndexError, ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return []
    roles = payload.get("roles") or []
    return roles if isinstance(roles, list) else []


class MicrosoftTeamsClient:
    """Client Class for API calls to Microsoft Teams via Microsoft Graph."""

    def __init__(
        self,
        tenant_id,
        client_id,
        client_secret=None,
        certificate=None,
        private_key=None,
    ):
        tcp_connector = aiohttp.TCPConnector(limit=DEFAULT_PARALLEL_CONNECTION_COUNT)
        self._http_session = aiohttp.ClientSession(
            connector=tcp_connector,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=None),
            raise_for_status=True,
        )

        if client_secret and not certificate and not private_key:
            self.graph_api_token = GraphAPIToken(
                self._http_session, tenant_id, None, client_id, client_secret
            )
        elif certificate and private_key:
            self.graph_api_token = EntraAPIToken(
                self._http_session,
                tenant_id,
                None,
                client_id,
                certificate,
                private_key,
                GRAPH_ACQUIRE_TOKEN_URL,
            )
        else:
            msg = "Unexpected authentication: either a client_secret or certificate+private_key should be provided"
            raise Exception(msg)

        self._logger = logger
        self._skipped = Counter()
        self._graph_api_client = MicrosoftAPISession(
            self._http_session, self.graph_api_token, "@odata.nextLink", self._logger
        )

    def set_logger(self, logger_):
        self._logger = logger_
        self._graph_api_client.set_logger(self._logger)

    def log_skip_summary(self):
        """Emit an aggregate warning for resources soft-skipped during the sync.

        Per-resource ``NotFound`` cases are logged at debug; this surfaces totals
        for optional/missing resources (e.g. no files folder) without treating
        permission failures as silent skips.
        """
        if not self._skipped:
            return

        details = ", ".join(
            f"{count} {resource}" for resource, count in sorted(self._skipped.items())
        )
        self._logger.warning(
            f"Skipped some resources that were not found: {details}. "
            f"Content for those resources was not indexed."
        )
        self._skipped.clear()

    async def assert_files_permission(self):
        """Fail if the app token lacks ``Files.Read.All`` (needed for attachments)."""
        access_token = await self.graph_api_token.get()
        roles = _jwt_payload_roles(access_token)
        if FILES_READ_ALL_ROLE not in roles:
            msg = (
                "Fetch attachment content is enabled, but the application token "
                f"does not include the '{FILES_READ_ALL_ROLE}' role. Grant "
                f"'{FILES_READ_ALL_ROLE}' as an application permission with admin "
                "consent, or disable 'Fetch attachment content'."
            )
            raise PermissionsMissing(msg)

    async def ping(self):
        return await self._graph_api_client.fetch(f"{BASE_URL}/teams?$top=1")

    async def get_teams(self):
        async for teams in self._graph_api_client.scroll(f"{BASE_URL}/teams?$top=999"):
            yield teams

    async def get_team(self, team_id):
        """Fetch a single team (list often omits ``webUrl`` / ``createdDateTime``)."""
        try:
            return await self._graph_api_client.fetch(f"{BASE_URL}/teams/{team_id}")
        except NotFound:
            return None

    async def get_team_members(self, team_id):
        try:
            async for members in self._graph_api_client.scroll(
                f"{BASE_URL}/teams/{team_id}/members"
            ):
                yield members
        except NotFound:
            self._skipped["teams' members"] += 1
            self._logger.debug(
                f"Skipping members for team '{team_id}': team was not found."
            )
            return

    async def get_team_channels(self, team_id):
        try:
            async for channels in self._graph_api_client.scroll(
                f"{BASE_URL}/teams/{team_id}/channels"
            ):
                yield channels
        except NotFound:
            self._skipped["teams' channels"] += 1
            self._logger.debug(f"Skipping channels for team '{team_id}': not found.")
            return

    async def get_channel_messages(self, team_id, channel_id):
        try:
            async for messages in self._graph_api_client.scroll(
                f"{BASE_URL}/teams/{team_id}/channels/{channel_id}/messages?$top=50"
            ):
                yield messages
        except NotFound:
            self._skipped["channels' messages"] += 1
            self._logger.debug(
                f"Skipping messages for channel '{channel_id}' in team '{team_id}': "
                f"channel was not found."
            )
            return

    async def get_channel_message_replies(self, team_id, channel_id, message_id):
        """Yield all replies for a channel root message (dedicated replies API)."""
        try:
            async for replies in self._graph_api_client.scroll(
                f"{BASE_URL}/teams/{team_id}/channels/{channel_id}/messages/"
                f"{message_id}/replies?$top=50"
            ):
                yield replies
        except NotFound:
            self._skipped["channels' message replies"] += 1
            self._logger.debug(
                f"Skipping replies for message '{message_id}' in channel "
                f"'{channel_id}' (team '{team_id}'): message was not found."
            )
            return

    async def get_channel_members(self, team_id, channel_id):
        """Yield membership pages for a channel.

        Required for private/shared channels when DLS is enabled: those channels
        have a membership set that differs from the parent team's members.
        Requires the ``ChannelMember.Read.All`` application permission.
        """
        try:
            async for members in self._graph_api_client.scroll(
                f"{BASE_URL}/teams/{team_id}/channels/{channel_id}/members"
            ):
                yield members
        except NotFound:
            self._skipped["channels' members"] += 1
            self._logger.debug(
                f"Skipping members for channel '{channel_id}' in team '{team_id}': "
                f"channel was not found."
            )
            return

    async def get_channel_file(self, team_id, channel_id):
        try:
            return await self._graph_api_client.fetch(
                f"{BASE_URL}/teams/{team_id}/channels/{channel_id}/filesFolder"
            )
        except NotFound:
            self._logger.debug(
                f"Skipping files folder for channel '{channel_id}' in team '{team_id}'."
            )
            return None

    async def get_channel_drive_children(
        self, drive_id, item_id
    ) -> AsyncIterator[dict]:
        try:
            async for children in self._graph_api_client.scroll(
                f"{BASE_URL}/drives/{drive_id}/items/{item_id}/children?$top=5000"
            ):
                for child in children:
                    if child.get("folder"):
                        async for descendant in self.get_channel_drive_children(
                            drive_id, child["id"]
                        ):
                            yield descendant
                    yield child
        except NotFound:
            self._logger.debug(
                f"Skipping drive children for item '{item_id}' in drive '{drive_id}'."
            )
            return

    async def get_user(self, user_id):
        """Fetch a single user profile, or ``None`` if the user was not found."""
        if not user_id:
            return None
        try:
            return await self._graph_api_client.fetch(
                f"{BASE_URL}/users/{user_id}?$select={USER_PROFILE_SELECT}"
            )
        except NotFound:
            self._skipped["users"] += 1
            self._logger.debug(f"Skipping user '{user_id}': user was not found.")
            return None

    async def get_users_by_ids(self, user_ids: Iterable[str]) -> dict:
        """Resolve Graph user profiles for the given Entra ids.

        Uses ``$batch`` of ``GET /users/{id}`` (≤20 per request). Shared Graph
        batch error handling cannot soft-skip a single item ``404``, so a batch
        that hits ``NotFound`` falls back to per-id ``get_user`` for that chunk.
        Missing ``User.ReadBasic.All`` raises ``PermissionsMissing``.
        """
        result = {}
        unique_ids = sorted({user_id for user_id in (user_ids or []) if user_id})
        for offset in range(0, len(unique_ids), USER_PROFILE_BATCH_SIZE):
            chunk = unique_ids[offset : offset + USER_PROFILE_BATCH_SIZE]
            await self._fetch_users_chunk(chunk, result)
        return result

    async def _fetch_users_chunk(self, user_ids, result):
        requests = [
            {
                "id": user_id,
                "method": "GET",
                "url": f"/users/{user_id}?$select={USER_PROFILE_SELECT}",
            }
            for user_id in user_ids
        ]
        try:
            batch_response = await self._graph_api_client.post(
                f"{BASE_URL}/$batch", {"requests": requests}
            )
        except NotFound:
            # Shared $batch handling raises when any item is 404; resolve one-by-one.
            for user_id in user_ids:
                user = await self.get_user(user_id)
                if user is not None:
                    result[user_id] = user
            return

        for response in batch_response.get("responses", []) or []:
            user_id = response.get("id")
            status = response.get("status", 200)
            if status == 404:
                self._skipped["users"] += 1
                self._logger.debug(f"Skipping user '{user_id}': user was not found.")
                continue
            if status in (401, 403):
                msg = (
                    f"Unable to resolve user profiles. Verify the "
                    f"'User.ReadBasic.All' application permission is granted."
                )
                raise PermissionsMissing(msg)
            if status != 200 or not user_id:
                self._logger.warning(
                    f"Skipping user profile batch item '{user_id}' with status {status}."
                )
                continue
            body = response.get("body") or {}
            if isinstance(body, dict):
                result[user_id] = body

    async def get_user_chats(self, user_id):
        """Yield chat pages for a user (app-only list-chats path; no member expand)."""
        async for chats in self._graph_api_client.scroll(
            f"{BASE_URL}/users/{user_id}/chats?$top=50"
        ):
            yield chats

    async def get_chats(self, user_ids: Iterable[str]):
        """Yield unique chats for the given user ids.

        App-only Graph has no tenant-wide ``GET /chats``. The caller supplies
        deduplicated team-member ``userId`` values; this method lists each user's
        chats and deduplicates by chat id. Membership is fetched separately via
        ``get_chat_members``.
        """
        seen_chat_ids = set()

        for user_id in user_ids:
            if not user_id:
                continue
            try:
                async for chats in self.get_user_chats(user_id):
                    unique = []
                    for chat in chats:
                        chat_id = chat.get("id")
                        if not chat_id or chat_id in seen_chat_ids:
                            continue
                        seen_chat_ids.add(chat_id)
                        unique.append(chat)
                    if unique:
                        yield unique
            except NotFound:
                self._logger.debug(
                    f"Skipping chats for user '{user_id}': user was not found."
                )

    async def get_chat_members(self, chat_id):
        """Yield full membership pages for a chat (dedicated members API)."""
        try:
            async for members in self._graph_api_client.scroll(
                f"{BASE_URL}/chats/{chat_id}/members"
            ):
                yield members
        except NotFound:
            self._skipped["chats' members"] += 1
            self._logger.debug(
                f"Skipping members for chat '{chat_id}': chat was not found."
            )
            return

    async def get_chat_messages(self, chat_id):
        try:
            async for messages in self._graph_api_client.scroll(
                f"{BASE_URL}/chats/{chat_id}/messages?$top=50"
            ):
                yield messages
        except NotFound:
            self._skipped["chats' messages"] += 1
            self._logger.debug(
                f"Skipping messages for chat '{chat_id}': chat was not found."
            )
            return

    async def get_drive_item_by_content_url(self, content_url):
        """Resolve a message attachment ``contentUrl`` to a driveItem via shares API.

        Requires the ``Files.Read.All`` application permission.
        """
        if not content_url:
            return None
        share_id = encode_sharing_url(content_url)
        try:
            return await self._graph_api_client.fetch(
                f"{BASE_URL}/shares/{share_id}/driveItem"
            )
        except NotFound:
            return None

    async def download_drive_item(self, drive_id, item_id, async_buffer):
        await self._graph_api_client.pipe(
            f"{BASE_URL}/drives/{drive_id}/items/{item_id}/content", async_buffer
        )

    async def close(self):
        self._graph_api_client.close()
        await self._http_session.close()
