#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft Teams source module responsible to fetch documents from Microsoft Teams."""

import os
from functools import cached_property, partial

from connectors_sdk.source import BaseDataSource, ConfigurableFieldValueError
from connectors_sdk.utils import iso_zulu

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.sources.microsoft_teams.client import (
    EndSignal,
    MicrosoftTeamsClient,
    Schema,
    TeamsObjectType,
)
from connectors.sources.microsoft_teams.formatter import MicrosoftTeamsFormatter
from connectors.sources.shared.microsoft.graph import PermissionsMissing
from connectors.utils import (
    ConcurrentTasks,
    MemQueue,
    html_to_text,
)

QUEUE_MEM_SIZE = 5 * 1024 * 1024  # Size in Megabytes
MAX_CONCURRENCY = 80
# Graph membershipType values whose membership differs from the parent team.
# https://learn.microsoft.com/en-us/graph/api/resources/channel
NON_STANDARD_CHANNEL_TYPES = frozenset({"private", "shared"})


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_user(user_principal_name):
    """Prefix Entra UPN (SPO-aligned ``user:`` dialect, not a mailbox)."""
    return prefix_identity("user", user_principal_name)


def _channel_membership_type(channel):
    return (channel.get("membershipType") or "standard").lower()


def _message_body_text(message):
    """Plain-text Graph ``body`` content (subject is stored separately)."""
    body = html_to_text(html=(message.get("body") or {}).get("content")) or ""
    return body.strip()


def _message_subject(message):
    """Graph ``subject`` (plaintext headline / file-share caption)."""
    return (message.get("subject") or "").strip()


def _member_ids(members):
    """Stable list of Entra user ids from Graph conversation members."""
    return sorted(
        {member.get("userId") for member in members or [] if member.get("userId")}
    )


def _mail_from_graph_user(user):
    """Real mailbox from Graph ``mail`` only (never UPN)."""
    if not user:
        return None
    mail = (user.get("mail") or "").strip()
    return mail or None


def _upn_from_graph_user(user):
    """Entra login name from Graph ``userPrincipalName``."""
    if not user:
        return None
    upn = (user.get("userPrincipalName") or "").strip()
    return upn or None


def _profile_from_graph_user(user):
    """Normalize a Graph user resource into ``{name, email, user}``.

    ``email`` is mailbox (``mail``) only; ``user`` is UPN for the ``user:`` ACL dialect.
    """
    return {
        "name": (user.get("displayName") or "").strip() if user else "",
        "email": _mail_from_graph_user(user) or "",
        "user": _upn_from_graph_user(user) or "",
    }


def _member_display_names(members):
    """Fallback display names from membership payloads keyed by userId."""
    names = {}
    for member in members or []:
        user_id = member.get("userId")
        if not user_id:
            continue
        name = (member.get("displayName") or "").strip()
        if name and user_id not in names:
            names[user_id] = name
    return names


class MicrosoftTeamsDataSource(BaseDataSource):
    """Microsoft Teams"""

    name = "Microsoft Teams"
    service_type = "microsoft_teams"
    incremental_sync_enabled = False
    dls_enabled = True

    def __init__(self, configuration):
        """Set up the connection to Microsoft Teams.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.tasks = 0
        self._teams_enumeration_failed = False
        self._chats_enumeration_failed = False
        self._enumeration_error: Exception | None = None
        self._producer_error: Exception | None = None
        self.queue = MemQueue(maxmemsize=QUEUE_MEM_SIZE, refresh_timeout=120)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)
        self.schema = Schema()
        self.formatter = MicrosoftTeamsFormatter(self.schema)
        # Per-sync File dedupe: union ACLs across discovery paths; download once.
        self._file_acls = {}
        self._file_download_scheduled = set()
        self._file_parents = {}
        # Entra userId → {name, email, user} from Graph /users (not conversationMember).
        # email = mail only; user = userPrincipalName for the ``user:`` ACL dialect.
        self._user_profiles = {}
        # Membership displayName fallback when /users/{id} 404s.
        self._member_names = {}

    def _set_internal_logger(self):
        self.client.set_logger(self._logger)

    @cached_property
    def client(self):
        tenant_id = self.configuration["tenant_id"]
        client_id = self.configuration["client_id"]
        auth_method = self.configuration["auth_method"]

        if auth_method == "certificate":
            return MicrosoftTeamsClient(
                tenant_id,
                client_id,
                certificate=self.configuration["certificate"],
                private_key=self.configuration["private_key"],
            )

        return MicrosoftTeamsClient(
            tenant_id,
            client_id,
            client_secret=self.configuration["secret_value"],
        )

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Microsoft Teams.

        Returns:
            dictionary: Default configuration.
        """
        return {
            "tenant_id": {
                "label": "Tenant ID",
                "order": 1,
                "type": "str",
            },
            "client_id": {
                "label": "Client ID",
                "order": 2,
                "type": "str",
            },
            "auth_method": {
                "label": "Authentication Method",
                "order": 3,
                "type": "str",
                "display": "dropdown",
                "options": [
                    {"label": "Client Secret", "value": "secret"},
                    {"label": "Certificate", "value": "certificate"},
                ],
                "value": "secret",
            },
            "secret_value": {
                "label": "Secret value",
                "order": 4,
                "sensitive": True,
                "type": "str",
                "depends_on": [{"field": "auth_method", "value": "secret"}],
            },
            "certificate": {
                "label": "Content of certificate file",
                "display": "textarea",
                "sensitive": True,
                "order": 5,
                "type": "str",
                "depends_on": [{"field": "auth_method", "value": "certificate"}],
            },
            "private_key": {
                "label": "Content of private key file",
                "display": "textarea",
                "sensitive": True,
                "order": 6,
                "type": "str",
                "depends_on": [{"field": "auth_method", "value": "certificate"}],
            },
            "fetch_attachment_content": {
                "display": "toggle",
                "label": "Fetch attachment content",
                "order": 7,
                "tooltip": "Index channel Files-folder items and message file attachments (as File documents), and extract their content. Requires the 'Files.Read.All' application permission.",
                "type": "bool",
                "value": True,
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 8,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 9,
                "tooltip": "Document level security ensures identities and permissions set in Microsoft Teams are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
        }

    async def validate_config(self):
        await super().validate_config()

        auth_method = self.configuration["auth_method"]
        if auth_method == "certificate":
            if (
                not self.configuration["certificate"]
                or not self.configuration["private_key"]
            ):
                msg = "Both 'Content of certificate file' and 'Content of private key file' are required when the authentication method is 'Certificate'."
                raise ConfigurableFieldValueError(msg)
        elif not self.configuration["secret_value"]:
            msg = "'Secret value' is required when the authentication method is 'Client Secret'."
            raise ConfigurableFieldValueError(msg)

        # Check that we can obtain a Graph API token with the provided credentials
        await self.client.graph_api_token.get()

        if self._attachments_enabled():
            await self.client.assert_files_permission()

    async def ping(self):
        """Verify the connection with Microsoft Teams"""
        try:
            await self.client.ping()
            self._logger.info("Successfully connected to Microsoft Teams")
        except Exception:
            self._logger.exception("Error while connecting to Microsoft Teams")
            raise

    async def close(self):
        """Closes unclosed client session"""
        await self.client.close()

    # -- Document level security -------------------------------------------

    def _dls_enabled(self):
        if self._features is None:
            return False

        if not self._features.document_level_security_enabled():
            return False

        return self.configuration["use_document_level_security"]

    def access_control_query(self, access_control):
        return es_access_control_query(access_control)

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = sorted(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )
        return document

    def _remember_member_names(self, members):
        for user_id, name in _member_display_names(members).items():
            if user_id not in self._member_names:
                self._member_names[user_id] = name

    async def _ensure_user_profiles(self, user_ids):
        """Resolve missing Graph user profiles into ``_user_profiles``."""
        missing = [
            user_id
            for user_id in {uid for uid in (user_ids or []) if uid}
            if user_id not in self._user_profiles
        ]
        if not missing:
            return
        users = await self.client.get_users_by_ids(missing)
        for user_id in missing:
            graph_user = users.get(user_id)
            if graph_user is not None:
                self._user_profiles[user_id] = _profile_from_graph_user(graph_user)
            else:
                # Mark as resolved-empty so we do not re-fetch; name may come from members.
                self._user_profiles[user_id] = {
                    "name": self._member_names.get(user_id) or "",
                    "email": "",
                    "user": "",
                }

    def _access_control_for_members(self, members):
        """Content ACL tokens: ``user_id:`` only (email/UPN live on identity docs)."""
        access_control = set()
        for member in members or []:
            user_id = member.get("userId") or member.get("id")
            if user_id:
                access_control.add(_prefix_user_id(user_id))
        return list(access_control)

    def _user_access_control_doc(self, user_id):
        if not user_id:
            return None

        profile = self._user_profiles.get(user_id) or {}
        email = profile.get("email") or None
        upn = profile.get("user") or None
        prefixed_user_id = _prefix_user_id(user_id)
        prefixed_email = _prefix_email(email) if email else None
        prefixed_user = _prefix_user(upn) if upn else None

        access_control = [prefixed_user_id]
        if prefixed_email:
            access_control.append(prefixed_email)
        if prefixed_user:
            access_control.append(prefixed_user)

        return {
            "_id": user_id,
            "identity": {
                "user_id": prefixed_user_id,
                "email": prefixed_email,
                "user": prefixed_user,
            },
            "created_at": iso_zulu(),
        } | self.access_control_query(access_control)

    async def get_access_control(self):
        """Yields an access control document for every user participating in a synced team or chat."""
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping access control sync.")
            return

        self._user_profiles = {}
        self._member_names = {}
        identity_ids = set()
        team_member_ids = set()

        async for teams in self.client.get_teams():
            for team in teams:
                async for members in self.client.get_team_members(team["id"]):
                    self._remember_member_names(members)
                    for member in members:
                        user_id = member.get("userId")
                        if user_id:
                            identity_ids.add(user_id)
                            team_member_ids.add(user_id)

                async for channels in self.client.get_team_channels(team["id"]):
                    for channel in channels:
                        if (
                            _channel_membership_type(channel)
                            not in NON_STANDARD_CHANNEL_TYPES
                        ):
                            continue
                        async for channel_members in self.client.get_channel_members(
                            team["id"], channel["id"]
                        ):
                            self._remember_member_names(channel_members)
                            for member in channel_members:
                                user_id = member.get("userId")
                                if user_id:
                                    identity_ids.add(user_id)

        async for chats in self.client.get_chats(team_member_ids):
            for chat in chats:
                chat_id = chat.get("id")
                if not chat_id:
                    continue
                async for chat_members in self.client.get_chat_members(chat_id):
                    self._remember_member_names(chat_members)
                    for member in chat_members:
                        user_id = member.get("userId")
                        if user_id:
                            identity_ids.add(user_id)

        await self._ensure_user_profiles(identity_ids)

        for user_id in sorted(identity_ids):
            doc = self._user_access_control_doc(user_id)
            if doc:
                yield doc

    # -- Content extraction ------------------------------------------------

    async def get_content(
        self, attachment, drive_id, item_id, timestamp=None, doit=False
    ):
        """Extracts the content for allowed file types.

        Args:
            attachment (dict): Attachment document (already mapped by the formatter).
            drive_id (str): The drive id the item belongs to.
            item_id (str): The drive item id.
            timestamp (str, optional): Unused, kept for interface compatibility.
            doit (bool, optional): Whether to actually fetch the content.

        Returns:
            dict: Content document with `_id`, `_timestamp` and the attachment content.
        """
        file_size = int(attachment["size_in_bytes"] or 0)
        if not (doit and file_size):
            return

        filename = attachment["title"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        # Sink pops ``_id`` from this same dict before deferred download runs;
        # prefer ``item_id`` / surviving ``id`` over ``_id``.
        document = {
            "_id": item_id or attachment.get("id") or attachment.get("_id"),
            "_timestamp": attachment.get("_timestamp") or timestamp,
        }

        async with self.create_temp_file(file_extension) as async_buffer:
            temp_filename = async_buffer.name
            await self.client.download_drive_item(drive_id, item_id, async_buffer)
            await async_buffer.close()
            document = await self.handle_file_content_extraction(
                document, filename, temp_filename
            )
        return document

    def get_file_extension(self, filename):
        return os.path.splitext(filename)[-1]

    # -- Document producers ------------------------------------------------

    async def _consumer(self):
        """Async generator to process entries of the queue.

        Yields:
            dictionary: Documents from Microsoft Teams.
        """
        while self.tasks > 0:
            _, item = await self.queue.get()

            if isinstance(item, EndSignal):
                self.tasks -= 1
            else:
                yield item

    def _attachments_enabled(self):
        return self.configuration["fetch_attachment_content"]

    async def _emit_file(
        self,
        drive_item,
        access_control,
        *,
        channel_id=None,
        channel_title=None,
        chat_id=None,
        chat_title=None,
    ):
        """Upsert a File document; schedule content download at most once per sync.

        Parent fields are sparse: set channel_* and/or chat_* only when known.
        Re-emitting the same driveItem merges parents and unions ACLs.
        """
        item_id = drive_item.get("id")
        drive_id = (drive_item.get("parentReference") or {}).get("driveId")
        if not item_id or not drive_id:
            return None

        acl = self._file_acls.setdefault(item_id, set())
        acl.update(access_control or [])

        parents = self._file_parents.setdefault(item_id, {})
        if channel_id:
            parents["channel_id"] = channel_id
            if channel_title:
                parents["channel_title"] = channel_title
        if chat_id:
            parents["chat_id"] = chat_id
            if chat_title:
                parents["chat_title"] = chat_title

        document = self.formatter.format_file(drive_item, parents=parents)
        document = self._decorate_with_access_control(document, sorted(acl))

        download = None
        if item_id not in self._file_download_scheduled:
            self._file_download_scheduled.add(item_id)
            download = partial(
                self.get_content,
                attachment=document,
                drive_id=drive_id,
                item_id=item_id,
            )

        await self.queue.put((document, download))
        return {
            "id": item_id,
            "title": drive_item.get("name") or "",
        }

    async def _attachments_for_message(
        self,
        message,
        access_control,
        *,
        channel_id=None,
        channel_title=None,
        chat_id=None,
        chat_title=None,
    ):
        """Resolve reference attachments to File docs; return message ``attachments``."""
        if not self._attachments_enabled():
            return []

        refs = []
        for attachment in message.get("attachments") or []:
            if attachment.get("contentType") != "reference":
                continue
            content_url = attachment.get("contentUrl")
            if not content_url:
                continue
            drive_item = await self.client.get_drive_item_by_content_url(content_url)
            if not drive_item:
                self._logger.debug(
                    f"Skipping message attachment '{attachment.get('name')}': "
                    f"unable to resolve contentUrl to a driveItem."
                )
                continue
            ref = await self._emit_file(
                drive_item,
                access_control,
                channel_id=channel_id,
                channel_title=channel_title,
                chat_id=chat_id,
                chat_title=chat_title,
            )
            if ref:
                if not ref["title"] and attachment.get("name"):
                    ref["title"] = attachment["name"]
                refs.append(ref)
        return refs

    async def _process_channel_message(
        self, message, channel_id, channel_title, access_control
    ):
        if message.get("deletedDateTime"):
            return
        if "unknownFutureValue" in (message.get("messageType") or ""):
            return
        subject = _message_subject(message)
        message_content = _message_body_text(message)
        if not subject and not message_content and not message.get("attachments"):
            return
        attachments = await self._attachments_for_message(
            message,
            access_control,
            channel_id=channel_id,
            channel_title=channel_title,
        )
        document = self.formatter.format_channel_message(
            item=message,
            channel_id=channel_id,
            channel_title=channel_title,
            message_content=message_content,
            subject=subject,
            attachments=attachments,
        )
        await self.queue.put(
            (self._decorate_with_access_control(document, access_control), None)
        )

    async def _process_channel_files(
        self, channel_id, channel_title, access_control, team_id
    ):
        files_folder = await self.client.get_channel_file(team_id, channel_id)
        if not files_folder:
            return
        drive_id = files_folder.get("parentReference", {}).get("driveId")
        item_id = files_folder.get("id")
        if not drive_id or not item_id:
            return
        async for child in self.client.get_channel_drive_children(drive_id, item_id):
            if child.get("file"):
                await self._emit_file(
                    child,
                    access_control,
                    channel_id=channel_id,
                    channel_title=channel_title,
                )

    async def _resolve_channel_access_control(
        self, channel, team_id, team_members, team_access_control
    ):
        """Return ``(access_control, members)`` for a channel, or ``None`` to skip.

        Standard channels inherit the parent team's membership. Private and shared
        channels have a different membership set. When DLS is enabled we must
        resolve channel members; if that fails, return ``None`` so the caller
        skips indexing the channel rather than leaking content under the team ACL.
        When DLS is off we still fetch channel members for ``member_ids`` when
        possible, and fall back to an empty list if they cannot be resolved.
        """
        if _channel_membership_type(channel) not in NON_STANDARD_CHANNEL_TYPES:
            return team_access_control, team_members

        channel_id = channel.get("id")
        channel_title = channel.get("displayName")
        channel_members = []
        async for page in self.client.get_channel_members(team_id, channel_id):
            channel_members.extend(page)

        if not channel_members:
            if self._dls_enabled():
                self._logger.warning(
                    f"Skipping private/shared channel '{channel_title}' ({channel_id}) "
                    f"in team '{team_id}': unable to resolve channel members for DLS. "
                    f"Verify the 'ChannelMember.Read.All' application permission is "
                    f"granted, or disable document level security."
                )
                return None
            self._logger.warning(
                f"Private/shared channel '{channel_title}' ({channel_id}) in team "
                f"'{team_id}' has no resolvable members; indexing with empty member_ids."
            )
            return team_access_control, []

        return self._access_control_for_members(channel_members), channel_members

    async def _produce_channel(
        self, channel, team_id, team_title, team_members, access_control
    ):
        channel_id = channel.get("id")
        channel_title = channel.get("displayName")

        resolved = await self._resolve_channel_access_control(
            channel, team_id, team_members, access_control
        )
        if resolved is None:
            return
        channel_access_control, channel_members = resolved

        channel_document = self.formatter.format_doc(
            item=channel,
            document_type=self.schema.channel,
            document={
                "type": TeamsObjectType.CHANNEL.value,
                "_timestamp": iso_zulu(),
                "team_id": team_id,
                "team_title": team_title,
                "member_ids": _member_ids(channel_members),
            },
        )
        await self.queue.put(
            (
                self._decorate_with_access_control(
                    channel_document, channel_access_control
                ),
                None,
            )
        )

        async for messages in self.client.get_channel_messages(team_id, channel_id):
            for message in messages:
                await self._process_channel_message(
                    message, channel_id, channel_title, channel_access_control
                )
                message_id = message.get("id")
                if not message_id:
                    continue
                async for replies in self.client.get_channel_message_replies(
                    team_id, channel_id, message_id
                ):
                    for reply in replies:
                        await self._process_channel_message(
                            reply, channel_id, channel_title, channel_access_control
                        )

        if self._attachments_enabled():
            await self._process_channel_files(
                channel_id, channel_title, channel_access_control, team_id
            )

    async def team_producer(self, team, members):
        team_id = team.get("id")
        # List /teams often returns null webUrl / createdDateTime; enrich when needed.
        if team_id and (not team.get("webUrl") or not team.get("createdDateTime")):
            detailed = await self.client.get_team(team_id)
            if detailed:
                team = {**team, **{k: v for k, v in detailed.items() if v is not None}}

        team_title = team.get("displayName")

        try:
            access_control = self._access_control_for_members(members)

            team_document = self.formatter.format_doc(
                item=team,
                document_type=self.schema.team,
                document={
                    "type": TeamsObjectType.TEAM.value,
                    "_timestamp": iso_zulu(),
                    "member_ids": _member_ids(members),
                },
            )
            await self.queue.put(
                (
                    self._decorate_with_access_control(team_document, access_control),
                    None,
                )
            )

            # Channels are processed inline (not scheduled as separate pool tasks)
            # to avoid a nested fetchers.put that could deadlock the bounded pool.
            async for channels in self.client.get_team_channels(team_id):
                for channel in channels:
                    await self._produce_channel(
                        channel, team_id, team_title, members, access_control
                    )
        except Exception as exc:
            # ConcurrentTasks removes completed tasks before get_docs can inspect
            # them, so record the failure here and re-raise for logging.
            if self._producer_error is None:
                self._producer_error = exc
            self._logger.error(
                f"Unexpected error while syncing team '{team_title}' ({team_id}).",
                exc_info=exc,
            )
            raise
        finally:
            await self.queue.put(EndSignal.TEAM_TASK_FINISHED)

    def _chat_member_names(self, members):
        return ",".join(
            member.get("displayName")
            for member in members
            if member.get("displayName", "")
        )

    async def _process_chat_message(self, chat, message, member_names, access_control):
        if message.get("deletedDateTime"):
            return
        if "unknownFutureValue" in (message.get("messageType") or ""):
            return
        subject = _message_subject(message)
        message_content = _message_body_text(message)
        if not subject and not message_content and not message.get("attachments"):
            return
        attachments = await self._attachments_for_message(
            message,
            access_control,
            chat_id=chat.get("id"),
            chat_title=chat.get("topic") or member_names,
        )
        document = self.formatter.format_chat_message(
            chat=chat,
            message=message,
            message_content=message_content,
            subject=subject,
            members=member_names,
            attachments=attachments,
        )
        await self.queue.put(
            (self._decorate_with_access_control(document, access_control), None)
        )

    async def chat_producer(self, chat):
        chat_id = chat.get("id")

        try:
            members = []
            async for member_page in self.client.get_chat_members(chat_id):
                members.extend(member_page)
            access_control = self._access_control_for_members(members)
            member_names = self._chat_member_names(members)

            chat_document = self.formatter.format_doc(
                item=chat,
                document_type=self.schema.chat,
                document={
                    "type": TeamsObjectType.CHAT.value,
                    "member_ids": _member_ids(members),
                },
            )
            if not chat_document.get("title"):
                chat_document["title"] = member_names
            await self.queue.put(
                (
                    self._decorate_with_access_control(chat_document, access_control),
                    None,
                )
            )

            async for messages in self.client.get_chat_messages(chat_id):
                for message in messages:
                    await self._process_chat_message(
                        chat, message, member_names, access_control
                    )
        except Exception as exc:
            if self._producer_error is None:
                self._producer_error = exc
            self._logger.error(
                f"Unexpected error while syncing chat '{chat_id}'.",
                exc_info=exc,
            )
            raise
        finally:
            await self.queue.put(EndSignal.CHAT_TASK_FINISHED)

    async def _enumerate_producers(self):
        """Enumerates teams and chats and schedules their producers.

        Runs as a single top-level task so that ``_consumer`` can drain the queue
        concurrently. Enumerating everything up front (before consuming) can stall
        on a large tenant: producers fill the bounded ``MemQueue`` while nothing is
        draining it, and ``fetchers`` blocks once ``MAX_CONCURRENCY`` is reached.
        """
        self._teams_enumeration_failed = False
        self._chats_enumeration_failed = False
        self._enumeration_error = None
        self._producer_error = None
        self._file_acls = {}
        self._file_download_scheduled = set()
        self._file_parents = {}
        self._user_profiles = {}
        self._member_names = {}
        team_jobs = []
        team_member_ids = set()
        try:
            try:
                async for teams in self.client.get_teams():
                    for team in teams:
                        team_id = team.get("id")
                        members = []
                        if team_id:
                            async for member_page in self.client.get_team_members(
                                team_id
                            ):
                                members.extend(member_page)
                            self._remember_member_names(members)
                            team_member_ids.update(_member_ids(members))
                        team_jobs.append((team, members))

                await self._ensure_user_profiles(team_member_ids)

                for team, members in team_jobs:
                    await self.fetchers.put(
                        partial(self.team_producer, team, members)
                    )
                    self.tasks += 1

                for user_id in sorted(team_member_ids):
                    profile = self._user_profiles.get(user_id) or {}
                    user_document = self.formatter.format_user(
                        user_id=user_id,
                        name=profile.get("name") or self._member_names.get(user_id),
                        email=profile.get("email"),
                        upn=profile.get("user"),
                    )
                    user_document["_timestamp"] = iso_zulu()
                    # User docs are directory metadata: no DLS field (unrestricted
                    # within the search index). Membership content uses user_id: ACLs.
                    await self.queue.put((user_document, None))
            except PermissionsMissing:
                self._teams_enumeration_failed = True
                self._logger.warning(
                    "Unable to enumerate teams or resolve user profiles. Verify the "
                    "'Team.ReadBasic.All', 'TeamMember.Read.All', and "
                    "'User.ReadBasic.All' application permissions are granted."
                )

            try:
                async for chats in self.client.get_chats(team_member_ids):
                    for chat in chats:
                        await self.fetchers.put(partial(self.chat_producer, chat))
                        self.tasks += 1
            except PermissionsMissing:
                self._chats_enumeration_failed = True
                self._logger.warning(
                    "Unable to enumerate chats for team members. Verify the "
                    "'TeamMember.Read.All' and 'Chat.ReadBasic.All' application "
                    "permissions are granted."
                )
        except Exception as exc:
            # An unexpected (non-permission) error is treated as a connection-wide
            # failure: record it and abort so get_docs can re-raise it. The finally
            # below still emits ENUMERATION_FINISHED so the consumer never hangs.
            self._enumeration_error = exc
            self._logger.error(
                "Unexpected error while enumerating Microsoft Teams resources; aborting sync.",
                exc_info=exc,
            )
        finally:
            await self.queue.put(EndSignal.ENUMERATION_FINISHED)

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch Microsoft Teams objects in an async manner.

        Args:
            filtering (Filtering): Object of class Filtering.

        Yields:
            tuple: A document mapping and an optional coroutine to fetch its content.
        """
        await self.fetchers.put(partial(self._enumerate_producers))
        self.tasks += 1

        async for item in self._consumer():
            yield item

        await self.fetchers.join()

        self.client.log_skip_summary()

        if self._enumeration_error is not None:
            raise self._enumeration_error

        if self._producer_error is not None:
            raise self._producer_error

        if self._teams_enumeration_failed or self._chats_enumeration_failed:
            failed = []
            if self._teams_enumeration_failed:
                failed.append(
                    "teams/users ('Team.ReadBasic.All' / 'TeamMember.Read.All' / "
                    "'User.ReadBasic.All')"
                )
            if self._chats_enumeration_failed:
                failed.append("chats ('TeamMember.Read.All' / 'Chat.ReadBasic.All')")
            failed_list = " and ".join(failed)
            msg = (
                f"Enumeration failed for {failed_list}. Refusing to report a "
                f"successful sync, as this would delete previously indexed "
                f"documents from the failed corpus. Verify the listed application "
                f"permissions are granted."
            )
            raise PermissionsMissing(msg)
