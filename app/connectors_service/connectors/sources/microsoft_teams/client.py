#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft Teams client.

The connector uses application-only authentication (client secret or certificate)
and the least-privilege permission model:

- Enumerating teams uses the tenant-wide `Team.ReadBasic.All` application
  permission.
- Enumerating channels uses `Channel.ReadBasic.All` (or the resource-specific
  `ChannelSettings.Read.Group`).
- Reading channel messages/replies uses the resource-specific consent (RSC)
  permission `ChannelMessage.Read.Group`, granted when the connector's Teams app
  is installed in a team.
- Reading team members uses the RSC permission `TeamMember.Read.Group`.
- Enumerating and reading chats uses `Chat.ReadBasic.WhereInstalled` and
  `Chat.Read.WhereInstalled`, which scope results to chats where the connector's
  Teams app is installed.
- Downloading attachment content requires `Files.Read.All`.

Because RSC/WhereInstalled permissions are only granted where the app is
installed, per-resource calls that hit a resource the app is not installed in
respond with 403/404. Those are swallowed so a partially-installed tenant still
syncs whatever it has access to.
"""

import os
from collections import Counter
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
from connectors.utils import url_encode

GRAPH_ACQUIRE_TOKEN_URL = "https://graph.microsoft.com/.default"  # noqa S105
DEFAULT_PARALLEL_CONNECTION_COUNT = 10

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

CHAT_FILES_FOLDER_NAME = "microsoft teams chat files"


class TeamsObjectType(Enum):
    """Document `type` values emitted by the connector."""

    TEAM = "Teams"
    TEAM_MEMBER = "Team Member"
    CHANNEL = "Team Channel"
    CHANNEL_MESSAGE = "Channel Message"
    CHANNEL_ATTACHMENT = "Channel Attachment"
    CHAT = "Chat"
    CHAT_MESSAGE = "Chat Message"
    CHAT_ATTACHMENT = "Chat Attachment"


class EndSignal(Enum):
    ENUMERATION_FINISHED = "ENUMERATION_FINISHED"
    TEAM_TASK_FINISHED = "TEAM_TASK_FINISHED"
    CHANNEL_TASK_FINISHED = "CHANNEL_TASK_FINISHED"
    CHAT_TASK_FINISHED = "CHAT_TASK_FINISHED"


class Schema:
    """Maps Elasticsearch document fields to Microsoft Graph fields."""

    def team(self):
        return {
            "_id": "id",
            "title": "displayName",
            "description": "description",
            "creation_time": "createdDateTime",
        }

    def team_member(self):
        return {
            "_id": "id",
            "title": "displayName",
            "email": "email",
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

    def channel_attachment(self):
        return {
            "_id": "id",
            "name": "name",
            "weburl": "webUrl",
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
            "webUrl": "webUrl",
            "title": "title",
            "chatType": "chatType",
            "sender": "sender",
            "message": "message",
        }

    def chat_attachment(self):
        return {
            "_id": "id",
            "name": "name",
            "weburl": "webUrl",
            "size_in_bytes": "size",
            "_timestamp": "lastModifiedDateTime",
            "creation_time": "createdDateTime",
        }


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
        """Emit an aggregate warning for resources skipped during the sync.

        Per-resource 403/404s are logged at debug to avoid noise; this surfaces the
        totals so an under-installed tenant (missing RSC / Teams app not installed)
        is visible instead of producing a quiet, near-empty "successful" sync.
        """
        if not self._skipped:
            return

        details = ", ".join(
            f"{count} {resource}" for resource, count in sorted(self._skipped.items())
        )
        self._logger.warning(
            f"Skipped some resources because the connector's Teams app is not "
            f"installed there or the required permissions are missing: {details}. "
            f"Content in those teams/chats was not indexed."
        )
        self._skipped.clear()

    async def ping(self):
        return await self._graph_api_client.fetch(f"{BASE_URL}/teams?$top=1")

    async def get_teams(self):
        async for teams in self._graph_api_client.scroll(f"{BASE_URL}/teams?$top=999"):
            yield teams

    async def get_team_members(self, team_id):
        try:
            async for members in self._graph_api_client.scroll(
                f"{BASE_URL}/teams/{team_id}/members"
            ):
                yield members
        except (NotFound, PermissionsMissing):
            self._skipped["teams' members"] += 1
            self._logger.debug(
                f"Skipping members for team '{team_id}': the connector's Teams app is not installed there or 'TeamMember.Read.Group' is missing."
            )
            return

    async def get_team_channels(self, team_id):
        try:
            async for channels in self._graph_api_client.scroll(
                f"{BASE_URL}/teams/{team_id}/channels"
            ):
                yield channels
        except (NotFound, PermissionsMissing):
            self._skipped["teams' channels"] += 1
            self._logger.debug(f"Skipping channels for team '{team_id}'.")
            return

    async def get_channel_messages(self, team_id, channel_id):
        try:
            async for messages in self._graph_api_client.scroll(
                f"{BASE_URL}/teams/{team_id}/channels/{channel_id}/messages?$expand=replies&$top=50"
            ):
                yield messages
        except (NotFound, PermissionsMissing):
            self._skipped["channels' messages"] += 1
            self._logger.debug(
                f"Skipping messages for channel '{channel_id}' in team '{team_id}': the connector's Teams app is not installed there or 'ChannelMessage.Read.Group' is missing."
            )
            return

    async def get_channel_file(self, team_id, channel_id):
        try:
            return await self._graph_api_client.fetch(
                f"{BASE_URL}/teams/{team_id}/channels/{channel_id}/filesFolder"
            )
        except (NotFound, PermissionsMissing):
            self._logger.debug(
                f"Skipping files folder for channel '{channel_id}' in team '{team_id}'."
            )
            return None

    async def get_channel_drive_children(self, drive_id, item_id):
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
        except (NotFound, PermissionsMissing):
            self._logger.debug(
                f"Skipping drive children for item '{item_id}' in drive '{drive_id}'."
            )
            return

    async def get_chats(self):
        async for chats in self._graph_api_client.scroll(
            f"{BASE_URL}/chats?$expand=members&$top=50"
        ):
            yield chats

    async def get_chat_messages(self, chat_id):
        try:
            async for messages in self._graph_api_client.scroll(
                f"{BASE_URL}/chats/{chat_id}/messages?$top=50"
            ):
                yield messages
        except (NotFound, PermissionsMissing):
            self._skipped["chats' messages"] += 1
            self._logger.debug(
                f"Skipping messages for chat '{chat_id}': 'Chat.Read.WhereInstalled' is missing or the connector's Teams app is not installed there."
            )
            return

    async def _get_user_drive(self, user_id):
        try:
            return await self._graph_api_client.fetch(
                f"{BASE_URL}/users/{user_id}/drive"
            )
        except (NotFound, PermissionsMissing):
            return None

    async def _get_chat_files_folder(self, drive_id):
        try:
            async for children in self._graph_api_client.scroll(
                f"{BASE_URL}/drives/{drive_id}/items/root/children"
            ):
                for child in children:
                    if child.get("name", "").lower() == CHAT_FILES_FOLDER_NAME:
                        return child
        except (NotFound, PermissionsMissing):
            return None
        return None

    async def get_chat_attachments(self, sender_id, attachment_name):
        """Resolve a chat attachment (a reference to a file in the sender's OneDrive).

        Requires the `Files.Read.All` application permission.
        """
        drive = await self._get_user_drive(sender_id)
        if not drive:
            return
        folder = await self._get_chat_files_folder(drive.get("id"))
        if not folder:
            return
        escaped_name = url_encode(attachment_name).replace("'", "''")
        try:
            async for attachments in self._graph_api_client.scroll(
                f"{BASE_URL}/drives/{drive.get('id')}/items/{folder.get('id')}/children?$filter=name eq '{escaped_name}'"
            ):
                yield attachments
        except (NotFound, PermissionsMissing):
            return

    async def download_drive_item(self, drive_id, item_id, async_buffer):
        await self._graph_api_client.pipe(
            f"{BASE_URL}/drives/{drive_id}/items/{item_id}/content", async_buffer
        )

    async def close(self):
        self._graph_api_client.close()
        await self._http_session.close()
