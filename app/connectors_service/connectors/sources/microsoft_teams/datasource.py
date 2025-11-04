#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft Teams source module responsible to fetch documents from Microsoft Teams."""

import asyncio
import os
from datetime import datetime
from functools import cached_property, partial

import aiofiles
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from connectors_sdk.content_extraction import (
    TIKA_SUPPORTED_FILETYPES,
)
from connectors_sdk.source import BaseDataSource
from connectors_sdk.utils import (
    convert_to_b64,
)

from connectors.sources.microsoft_teams.client import (
    EndSignal,
    MicrosoftTeamsClient,
    Schema,
    TeamEndpointName,
    UserEndpointName,
)
from connectors.sources.microsoft_teams.formatter import MicrosoftTeamsFormatter
from connectors.utils import (
    ConcurrentTasks,
    MemQueue,
    html_to_text,
)

QUEUE_MEM_SIZE = 5 * 1024 * 1024  # Size in Megabytes
MAX_CONCURRENCY = 80
MAX_FILE_SIZE = 10485760


class MicrosoftTeamsDataSource(BaseDataSource):
    """Microsoft Teams"""

    name = "Microsoft Teams"
    service_type = "microsoft_teams"
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Set up the connection to the Microsoft Teams.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.tasks = 0
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
        client_secret = self.configuration["secret_value"]
        username = self.configuration["username"]
        password = self.configuration["password"]

        return MicrosoftTeamsClient(
            tenant_id, client_id, client_secret, username, password
        )

    async def _consumer(self):
        """Async generator to process entries of the queue

        Yields:
            dictionary: Documents from Microsoft Teams.
        """
        while self.tasks > 0:
            _, item = await self.queue.get()

            if isinstance(item, EndSignal):
                self.tasks -= 1
            else:
                yield item

    def verify_filename_for_extraction(self, filename):
        attachment_extension = os.path.splitext(filename)[-1]
        if attachment_extension == "":
            self._logger.debug(
                f"Files without extension are not supported, skipping {filename}."
            )
            return
        if attachment_extension.lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.debug(
                f"Files with the extension {attachment_extension} are not supported, skipping {filename}."
            )
            return
        return True

    async def _download_content_for_attachment(self, download_func, original_filename):
        attachment = None
        source_file_name = ""

        try:
            async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
                source_file_name = async_buffer.name
                await download_func(async_buffer)

            self._logger.debug(
                f"Download completed for file: {original_filename}. Calling convert_to_b64"
            )
            await asyncio.to_thread(
                convert_to_b64,
                source=source_file_name,
            )
            async with aiofiles.open(file=source_file_name, mode="r") as target_file:
                # base64 on macOS will add a EOL, so we strip() here
                attachment = (await target_file.read()).strip()

        finally:
            if source_file_name:
                await remove(str(source_file_name))

        return attachment

    async def validate_config(self):
        await super().validate_config()

        # Check that we can log in into Graph API
        await self.client._api_token.get_with_username_password()

    async def ping(self):
        """Verify the connection with Microsoft Teams"""
        try:
            await self.client.ping()
            self._logger.info("Successfully connected to Microsoft Teams")
        except Exception:
            self._logger.exception("Error while connecting to Microsoft Teams")
            raise

    async def update_user_chat_attachments(self, **kwargs):
        async for attachments in self.client.get_user_chat_attachments(
            sender_id=kwargs["sender_id"],
            attachment_name=kwargs["attachment_name"],
        ):
            for attachment in attachments:
                format_attachment = self.formatter.format_doc(
                    item=attachment,
                    document_type=self.schema.chat_attachments,
                    document={
                        "type": UserEndpointName.ATTACHMENT.value,
                        "members": kwargs.get("members", ""),
                    },
                )
                download_url = attachment.get("@microsoft.graph.downloadUrl")
                await self.queue.put(
                    (
                        format_attachment,
                        partial(
                            self.get_content,
                            user_attachment=format_attachment,
                            download_url=download_url,
                        ),
                    )
                )

    async def get_content(
        self, user_attachment, download_url, timestamp=None, doit=False
    ):
        """Extracts the content for allowed file types.

        Args:
            user_attachment (dictionary): Attachment object dictionary
            download_url (str): Attachment downloadable url
            timestamp (timestamp, optional): Timestamp of attachment last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and attachment content
        """
        document_size = int(user_attachment["size_in_bytes"])

        if not (doit and document_size):
            return
        filename = user_attachment["name"]
        if not self.verify_filename_for_extraction(filename=filename):
            return
        if user_attachment["size_in_bytes"] > MAX_FILE_SIZE:
            self._logger.warning(
                f"File size {document_size} of file {filename} is larger than {MAX_FILE_SIZE} bytes. Discarding file content"
            )
            return
        document = {
            "_id": user_attachment["id"],
            "_timestamp": user_attachment["_timestamp"],
        }
        self._logger.debug(f"Downloading {filename}")
        attachment_content = await self._download_content_for_attachment(
            partial(self.client.download_item, download_url),
            original_filename=filename,
        )
        document["_attachment"] = attachment_content

        return document

    async def get_messages(
        self, message, document_type=None, chat=None, channel_name=None, members=None
    ):
        if not message.get("deletedDateTime") and (
            "unknownFutureValue" not in message.get("messageType")
        ):
            if message_content := html_to_text(
                html=message.get("body", {}).get("content")
            ):
                if document_type == UserEndpointName.CHATS_MESSAGE.value:
                    message_document = await self.formatter.format_user_chat_messages(
                        chat=chat,
                        message=message,
                        message_content=message_content,
                        members=members,
                    )
                    await self.queue.put(
                        (
                            self.formatter.format_doc(
                                item=message_document,
                                document_type=self.schema.chat_messages,
                                document={"type": document_type},
                            ),
                            None,
                        )
                    )
                else:
                    await self.queue.put(
                        (
                            self.formatter.format_channel_message(
                                item=message,
                                channel_name=channel_name,
                                message_content=message_content,
                            ),
                            None,
                        )
                    )

    async def user_chat_meeting_recording(self, message):
        if (
            message.get("eventDetail")
            and message["eventDetail"].get("@odata.type")
            == "#microsoft.graph.callRecordingEventMessageDetail"
        ):
            url = message["eventDetail"].get("callRecordingUrl")

            if url and ".sharepoint.com" in url:
                await self.queue.put(
                    (
                        self.formatter.format_user_chat_meeting_recording(
                            item=message, url=url
                        ),
                        None,
                    )
                )

    def get_chat_members(self, members):
        return ",".join(
            member.get("displayName")
            for member in members
            if member.get("displayName", "")
        )

    async def user_chat_producer(self, chat):
        members = self.get_chat_members(chat.get("members", []))
        async for messages in self.client.get_user_chat_messages(chat_id=chat["id"]):
            for message in messages:
                await self.get_messages(
                    message=message,
                    document_type=UserEndpointName.CHATS_MESSAGE.value,
                    chat=chat,
                    members=members,
                )

                await self.user_chat_meeting_recording(message=message)

                if message.get("from") and message["from"].get("user"):
                    for attachment in message.get("attachments", []):
                        if (
                            attachment.get("name")
                            and attachment.get("contentType") == "reference"
                        ):
                            await self.update_user_chat_attachments(
                                sender_id=message["from"]["user"]["id"],
                                attachment_name=attachment["name"],
                                members=members,
                            )

        async for tabs in self.client.get_user_chat_tabs(chat_id=chat["id"]):
            for tab in tabs:
                await self.queue.put(
                    (
                        self.formatter.format_doc(
                            item=tab,
                            document_type=self.schema.chat_tabs,
                            document={
                                "type": UserEndpointName.TABS.value,
                                "url": tab.get("configuration", {}).get("websiteUrl"),
                                "_timestamp": chat["lastUpdatedDateTime"],
                                "members": members,
                            },
                        ),
                        None,
                    )
                )
        await self.queue.put(EndSignal.USER_CHAT_TASK_FINISHED)

    async def get_channel_messages(self, message, channel_name):
        await self.get_messages(message=message, channel_name=channel_name)
        meeting_document = {}
        for reply in message.get("replies", []):
            message_content = html_to_text(html=reply.get("body", {}).get("content"))
            if (
                not reply.get("deletedDateTime")
                and ("unknownFutureValue" not in reply.get("messageType"))
                and message_content
            ):
                await self.queue.put(
                    (
                        self.formatter.format_channel_message(
                            item=reply,
                            channel_name=channel_name,
                            message_content=message_content,
                        ),
                        None,
                    )
                )

            elif reply.get("eventDetail"):
                call_id = reply["eventDetail"].get("callId")
                if call_id not in meeting_document:
                    meeting_document[call_id] = {}
                document = self.formatter.format_channel_meeting(reply=reply)
                meeting_document[call_id].update(document)
        for document in meeting_document.values():
            await self.queue.put((document, None))

    async def team_channel_producer(self, channel, team_id, team_name):
        channel_name = channel.get("displayName")
        await self.queue.put(
            (
                self.formatter.format_doc(
                    item=channel,
                    document_type=self.schema.channel,
                    document={
                        "type": TeamEndpointName.CHANNEL.value,
                        "_timestamp": datetime.utcnow(),
                        "team_name": team_name,
                    },
                ),
                None,
            )
        )

        async for tabs in self.client.get_channel_tabs(
            team_id=team_id, channel_id=channel.get("id")
        ):
            for tab in tabs:
                await self.queue.put(
                    (
                        self.formatter.format_doc(
                            item=tab,
                            document_type=self.schema.channel_tab,
                            document={
                                "type": TeamEndpointName.TAB.value,
                                "_timestamp": datetime.utcnow(),
                                "team_name": team_name,
                                "channel_name": channel_name,
                            },
                        ),
                        None,
                    )
                )

        async for messages in self.client.get_channel_messages(
            team_id=team_id, channel_id=channel.get("id")
        ):
            for message in messages:
                await self.get_channel_messages(
                    message=message, channel_name=channel_name
                )

        file = await self.client.get_channel_file(
            team_id=team_id, channel_id=channel.get("id")
        )
        drive_id = file.get("parentReference", {}).get("driveId")
        await self.get_channel_drive_producer(
            drive_id=drive_id,
            item_id=file.get("id"),
            team_name=team_name,
            channel_name=channel_name,
        )

        await self.queue.put(EndSignal.CHANNEL_TASK_FINISHED)

    async def get_channel_drive_producer(
        self, drive_id, item_id, team_name, channel_name
    ):
        async for drive_child in self.client.get_channel_drive_childrens(
            drive_id=drive_id,
            item_id=item_id,
        ):
            if drive_child.get("file"):
                format_attachment = self.formatter.format_doc(
                    item=drive_child,
                    document_type=self.schema.channel_attachment,
                    document={
                        "type": TeamEndpointName.ATTACHMENT.value,
                        "team_name": team_name,
                        "channel_name": channel_name,
                    },
                )
                await self.queue.put(
                    (
                        format_attachment,
                        partial(
                            self.get_content,
                            user_attachment=format_attachment,
                            download_url=drive_child.get(
                                "@microsoft.graph.downloadUrl"
                            ),
                        ),
                    )
                )

    async def teams_producer(self, team):
        team_id = team.get("id")
        team_name = team.get("displayName")
        await self.queue.put(
            (
                self.formatter.format_doc(
                    item=team,
                    document_type=self.schema.teams,
                    document={
                        "type": TeamEndpointName.TEAMS.value,
                        "_timestamp": datetime.utcnow(),
                    },
                ),
                None,
            )
        )

        async for channels in self.client.get_team_channels(team_id=team["id"]):
            for channel in channels:
                await self.fetchers.put(
                    partial(self.team_channel_producer, channel, team_id, team_name)
                )
                self.tasks += 1

        await self.queue.put(EndSignal.TEAM_TASK_FINISHED)

    async def calendars_producer(self, user):
        async for event in self.client.get_calendars(user_id=user["id"]):
            if event and not event.get("isCancelled"):
                await self.queue.put(
                    (
                        self.formatter.format_user_calendars(
                            item=event,
                        ),
                        None,
                    )
                )
        await self.queue.put(EndSignal.CALENDAR_TASK_FINISHED)

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch Microsoft Teams objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """
        async for chats in self.client.get_user_chats():
            for chat in chats:
                await self.fetchers.put(partial(self.user_chat_producer, chat))
                self.tasks += 1

        async for users in self.client.users():
            for user in users:
                if user.get("mail"):
                    await self.fetchers.put(partial(self.calendars_producer, user))
                    self.tasks += 1

        async for teams in self.client.get_teams():
            for team in teams:
                await self.fetchers.put(partial(self.teams_producer, team))
                self.tasks += 1

        async for item in self._consumer():
            yield item

        await self.fetchers.join()

    async def close(self):
        """Closes unclosed client session"""
        await self.client.close()

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
            "secret_value": {
                "label": "Secret value",
                "order": 3,
                "sensitive": True,
                "type": "str",
            },
            "username": {
                "label": "Username",
                "order": 4,
                "type": "str",
            },
            "password": {
                "label": "Password",
                "order": 5,
                "sensitive": True,
                "type": "str",
            },
        }
