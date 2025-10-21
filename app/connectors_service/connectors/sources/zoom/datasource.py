#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Zoom source module responsible to fetch documents from Zoom."""

from functools import cached_property, partial

from connectors_sdk.source import BaseDataSource
from connectors_sdk.utils import (
    iso_utc,
)

from connectors.sources.zoom.client import ZoomClient


class ZoomDataSource(BaseDataSource):
    name = "Zoom"
    service_type = "zoom"
    incremental_sync_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self.configuration = configuration

    def _set_internal_logger(self):
        self.client.set_logger(self._logger)

    @cached_property
    def client(self):
        return ZoomClient(configuration=self.configuration)

    @classmethod
    def get_default_configuration(cls):
        return {
            "account_id": {
                "label": "Account ID",
                "order": 1,
                "type": "str",
            },
            "client_id": {
                "label": "Client ID",
                "order": 2,
                "type": "str",
            },
            "client_secret": {
                "label": "Client secret",
                "order": 3,
                "sensitive": True,
                "type": "str",
            },
            "fetch_past_meeting_details": {
                "display": "toggle",
                "label": "Fetch past meeting details",
                "order": 4,
                "tooltip": "Enable this option to fetch past past meeting details. This setting can increase sync time.",
                "type": "bool",
                "value": False,
            },
            "recording_age": {
                "display": "numeric",
                "label": "Recording Age Limit (Months)",
                "order": 5,
                "tooltip": "How far back in time to request recordings from zoom. Recordings older than this will not be indexed.",
                "type": "int",
                "validations": [{"type": "greater_than", "constraint": -1}],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 6,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }

    async def validate_config(self):
        await super().validate_config()
        await self.client.api_token.get()

    async def ping(self):
        try:
            await self.client.api_token.get()
            self._logger.debug("Successfully connected to Zoom.")
        except Exception:
            self._logger.debug("Error while connecting to Zoom.")
            raise

    async def close(self):
        await self.client.close()

    def _format_doc(self, doc, doc_time):
        doc = self.serialize(doc=doc)
        doc.update(
            {
                "_id": doc["id"],
                "_timestamp": doc_time,
            }
        )
        return doc

    async def get_content(self, chat_file, timestamp=None, doit=False):
        file_size = chat_file["file_size"]
        if not (doit and file_size > 0):
            return

        filename = chat_file["file_name"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(
            file_extension,
            filename,
            file_size,
        ):
            return

        document = {
            "_id": chat_file["id"],
            "_timestamp": chat_file["date_time"],
        }
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.client.get_file_content,
                download_url=chat_file["download_url"],
            ),
        )

    async def fetch_previous_meeting_details(self, meeting_id):
        previous_meeting = await self.client.get_past_meeting(meeting_id=meeting_id)

        if not previous_meeting:
            return

        participants = []
        async for participant in self.client.get_past_meeting_participants(
            meeting_id=meeting_id
        ):
            participants.append(participant)

        previous_meeting["participants"] = participants
        return previous_meeting

    async def get_docs(self, filtering=None):
        async for user in self.client.get_users():
            yield self._format_doc(doc=user, doc_time=iso_utc()), None

            async for live_meeting in self.client.get_meetings(
                user_id=user.get("id"), meeting_type="live"
            ):
                yield (
                    self._format_doc(
                        doc=live_meeting, doc_time=live_meeting.get("created_at")
                    ),
                    None,
                )

            async for upcoming_meeting in self.client.get_meetings(
                user_id=user.get("id"), meeting_type="upcoming_meetings"
            ):
                yield (
                    self._format_doc(
                        doc=upcoming_meeting,
                        doc_time=upcoming_meeting.get("created_at"),
                    ),
                    None,
                )

            async for previous_meeting in self.client.get_meetings(
                user_id=user.get("id"), meeting_type="previous_meetings"
            ):
                if self.configuration["fetch_past_meeting_details"]:
                    previous_meeting_details = (
                        await self.fetch_previous_meeting_details(
                            meeting_id=previous_meeting.get("id")
                        )
                    )
                    if not previous_meeting_details:
                        yield (
                            self._format_doc(
                                doc=previous_meeting,
                                doc_time=previous_meeting.get("created_at"),
                            ),
                            None,
                        )
                    else:
                        yield (
                            self._format_doc(
                                doc=previous_meeting_details,
                                doc_time=previous_meeting_details.get("end_time"),
                            ),
                            None,
                        )
                else:
                    yield (
                        self._format_doc(
                            doc=previous_meeting,
                            doc_time=previous_meeting.get("created_at"),
                        ),
                        None,
                    )

            async for recording in self.client.get_recordings(user_id=user.get("id")):
                yield (
                    self._format_doc(
                        doc=recording, doc_time=recording.get("start_time")
                    ),
                    None,
                )

            async for channel in self.client.get_channels(user_id=user.get("id")):
                yield self._format_doc(doc=channel, doc_time=iso_utc()), None

            async for chat_message in self.client.get_chats(
                user_id=user.get("id"), chat_type="message"
            ):
                yield (
                    self._format_doc(
                        doc=chat_message, doc_time=chat_message.get("date_time")
                    ),
                    None,
                )

            async for chat_file in self.client.get_chats(
                user_id=user.get("id"), chat_type="file"
            ):
                chat_file["id"] = chat_file.get("file_id")
                yield (
                    self._format_doc(
                        doc=chat_file, doc_time=chat_file.get("date_time")
                    ),
                    partial(self.get_content, chat_file=chat_file.copy()),
                )
