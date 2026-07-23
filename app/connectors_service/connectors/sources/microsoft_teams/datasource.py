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


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_email(email):
    return prefix_identity("email", email)


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
        self.queue = MemQueue(maxmemsize=QUEUE_MEM_SIZE, refresh_timeout=120)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)
        self.schema = Schema()
        self.formatter = MicrosoftTeamsFormatter(self.schema)

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
                "tooltip": "Enable to fetch the content of channel and chat attachments. Requires the 'Files.Read.All' application permission.",
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

    def _access_control_for_members(self, members):
        access_control = set()
        for member in members or []:
            user_id = member.get("userId") or member.get("id")
            if user_id:
                access_control.add(_prefix_user_id(user_id))
            email = member.get("email")
            if email:
                access_control.add(_prefix_email(email))
        return list(access_control)

    def _user_access_control_doc(self, member):
        user_id = member.get("userId") or member.get("id")
        if not user_id:
            return None

        email = member.get("email")
        prefixed_user_id = _prefix_user_id(user_id)
        prefixed_email = _prefix_email(email) if email else None

        access_control = [prefixed_user_id]
        if prefixed_email:
            access_control.append(prefixed_email)

        return {
            "_id": user_id,
            "identity": {
                "user_id": prefixed_user_id,
                "email": prefixed_email,
            },
            "created_at": iso_zulu(),
        } | self.access_control_query(access_control)

    async def get_access_control(self):
        """Yields an access control document for every user participating in a synced team or chat."""
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping access control sync.")
            return

        seen = set()

        try:
            async for teams in self.client.get_teams():
                for team in teams:
                    async for members in self.client.get_team_members(team["id"]):
                        for member in members:
                            doc = self._user_access_control_doc(member)
                            if doc and doc["_id"] not in seen:
                                seen.add(doc["_id"])
                                yield doc
        except PermissionsMissing:
            self._logger.warning(
                "Unable to enumerate teams for access control. Verify the 'Team.ReadBasic.All' application permission is granted."
            )

        try:
            async for chats in self.client.get_chats():
                for chat in chats:
                    for member in chat.get("members", []):
                        doc = self._user_access_control_doc(member)
                        if doc and doc["_id"] not in seen:
                            seen.add(doc["_id"])
                            yield doc
        except PermissionsMissing:
            self._logger.warning(
                "Unable to enumerate chats for access control. Verify the 'Chat.ReadBasic.WhereInstalled' application permission is granted and the connector's Teams app is installed."
            )

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

        filename = attachment["name"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        document = {
            "_id": attachment["_id"],
            "_timestamp": attachment["_timestamp"],
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

    async def _process_channel_message(self, message, channel_name, access_control):
        if message.get("deletedDateTime"):
            return
        if "unknownFutureValue" in (message.get("messageType") or ""):
            return
        message_content = html_to_text(html=(message.get("body") or {}).get("content"))
        if not message_content and not message.get("attachments"):
            return
        document = self.formatter.format_channel_message(
            item=message, channel_name=channel_name, message_content=message_content
        )
        await self.queue.put(
            (self._decorate_with_access_control(document, access_control), None)
        )

    async def _process_channel_attachment(
        self, child, team_name, channel_name, access_control
    ):
        drive_id = child.get("parentReference", {}).get("driveId")
        if not drive_id:
            return
        document = self.formatter.format_doc(
            item=child,
            document_type=self.schema.channel_attachment,
            document={
                "type": TeamsObjectType.CHANNEL_ATTACHMENT.value,
                "team_name": team_name,
                "channel_name": channel_name,
            },
        )
        document = self._decorate_with_access_control(document, access_control)
        await self.queue.put(
            (
                document,
                partial(
                    self.get_content,
                    attachment=document,
                    drive_id=drive_id,
                    item_id=child["id"],
                ),
            )
        )

    async def _produce_channel(self, channel, team_id, team_name, access_control):
        channel_id = channel.get("id")
        channel_name = channel.get("displayName")

        channel_document = self.formatter.format_doc(
            item=channel,
            document_type=self.schema.channel,
            document={
                "type": TeamsObjectType.CHANNEL.value,
                "_timestamp": iso_zulu(),
                "team_name": team_name,
            },
        )
        await self.queue.put(
            (self._decorate_with_access_control(channel_document, access_control), None)
        )

        async for messages in self.client.get_channel_messages(team_id, channel_id):
            for message in messages:
                await self._process_channel_message(
                    message, channel_name, access_control
                )
                for reply in message.get("replies", []):
                    await self._process_channel_message(
                        reply, channel_name, access_control
                    )

        if self._attachments_enabled():
            files_folder = await self.client.get_channel_file(team_id, channel_id)
            if files_folder:
                drive_id = files_folder.get("parentReference", {}).get("driveId")
                item_id = files_folder.get("id")
                if drive_id and item_id:
                    async for child in self.client.get_channel_drive_children(
                        drive_id, item_id
                    ):
                        if child.get("file"):
                            await self._process_channel_attachment(
                                child, team_name, channel_name, access_control
                            )

    async def team_producer(self, team):
        team_id = team.get("id")
        team_name = team.get("displayName")

        try:
            members = []
            async for member_page in self.client.get_team_members(team_id):
                members.extend(member_page)

            access_control = self._access_control_for_members(members)

            team_document = self.formatter.format_doc(
                item=team,
                document_type=self.schema.team,
                document={
                    "type": TeamsObjectType.TEAM.value,
                    "_timestamp": iso_zulu(),
                },
            )
            await self.queue.put(
                (
                    self._decorate_with_access_control(team_document, access_control),
                    None,
                )
            )

            for member in members:
                member_document = self.formatter.format_team_member(
                    member, team_id, team_name
                )
                member_document["_timestamp"] = iso_zulu()
                await self.queue.put(
                    (
                        self._decorate_with_access_control(
                            member_document, access_control
                        ),
                        None,
                    )
                )

            # Channels are processed inline (not scheduled as separate pool tasks)
            # to avoid a nested fetchers.put that could deadlock the bounded pool.
            async for channels in self.client.get_team_channels(team_id):
                for channel in channels:
                    await self._produce_channel(
                        channel, team_id, team_name, access_control
                    )
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
        message_content = html_to_text(html=(message.get("body") or {}).get("content"))
        if not message_content and not message.get("attachments"):
            return
        document = self.formatter.format_chat_message(
            chat=chat,
            message=message,
            message_content=message_content,
            members=member_names,
        )
        await self.queue.put(
            (self._decorate_with_access_control(document, access_control), None)
        )

    async def _process_chat_attachment(
        self, sender_id, attachment_name, member_names, access_control
    ):
        async for attachments in self.client.get_chat_attachments(
            sender_id, attachment_name
        ):
            for attachment in attachments:
                if not attachment.get("file"):
                    continue
                drive_id = attachment.get("parentReference", {}).get("driveId")
                if not drive_id:
                    continue
                document = self.formatter.format_doc(
                    item=attachment,
                    document_type=self.schema.chat_attachment,
                    document={
                        "type": TeamsObjectType.CHAT_ATTACHMENT.value,
                        "members": member_names,
                    },
                )
                document = self._decorate_with_access_control(document, access_control)
                await self.queue.put(
                    (
                        document,
                        partial(
                            self.get_content,
                            attachment=document,
                            drive_id=drive_id,
                            item_id=attachment["id"],
                        ),
                    )
                )

    async def chat_producer(self, chat):
        chat_id = chat.get("id")
        members = chat.get("members", [])
        access_control = self._access_control_for_members(members)
        member_names = self._chat_member_names(members)

        try:
            chat_document = self.formatter.format_doc(
                item=chat,
                document_type=self.schema.chat,
                document={"type": TeamsObjectType.CHAT.value},
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

                    if self._attachments_enabled() and (
                        message.get("from") and message["from"].get("user")
                    ):
                        for attachment in message.get("attachments", []):
                            if (
                                attachment.get("name")
                                and attachment.get("contentType") == "reference"
                            ):
                                await self._process_chat_attachment(
                                    message["from"]["user"]["id"],
                                    attachment["name"],
                                    member_names,
                                    access_control,
                                )
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
        try:
            try:
                async for teams in self.client.get_teams():
                    for team in teams:
                        await self.fetchers.put(partial(self.team_producer, team))
                        self.tasks += 1
            except PermissionsMissing:
                self._teams_enumeration_failed = True
                self._logger.warning(
                    "Unable to enumerate teams. Verify the 'Team.ReadBasic.All' application permission is granted."
                )

            try:
                async for chats in self.client.get_chats():
                    for chat in chats:
                        await self.fetchers.put(partial(self.chat_producer, chat))
                        self.tasks += 1
            except PermissionsMissing:
                self._chats_enumeration_failed = True
                self._logger.warning(
                    "Unable to enumerate chats. Verify the 'Chat.ReadBasic.WhereInstalled' application permission is granted and the connector's Teams app is installed."
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

        if self._teams_enumeration_failed and self._chats_enumeration_failed:
            msg = (
                "Both team and chat enumeration failed due to missing permissions. "
                "Refusing to report a successful sync, as this would delete previously "
                "indexed documents. Verify 'Team.ReadBasic.All' and "
                "'Chat.ReadBasic.WhereInstalled' are granted and the connector's Teams "
                "app is installed."
            )
            raise PermissionsMissing(msg)
