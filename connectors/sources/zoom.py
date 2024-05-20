#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Zoom source module responsible to fetch documents from Zoom."""
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import cached_property, partial

import aiohttp
from aiohttp.client_exceptions import ClientResponseError

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    CacheWithTimeout,
    CancellableSleeps,
    RetryStrategy,
    get_base64_value,
    iso_utc,
    retryable,
)

RETRIES = 3
RETRY_INTERVAL = 2
CHAT_PAGE_SIZE = 50
MEETING_PAGE_SIZE = 300

if "OVERRIDE_URL" in os.environ:
    override_url = os.environ["OVERRIDE_URL"]
    BASE_URL = override_url
    BASE_AUTH_URL = override_url
else:
    BASE_URL = "https://api.zoom.us/v2"
    BASE_AUTH_URL = "https://zoom.us"

AUTH = (
    "{base_auth_url}/oauth/token?grant_type=account_credentials&account_id={account_id}"
)
APIS = {
    "USERS": "{base_url}/users?page_size={page_size}",
    "MEETINGS": "{base_url}/users/{user_id}/meetings?page_size={page_size}&type={meeting_type}",
    "PAST_MEETING": "{base_url}/past_meetings/{meeting_id}",
    "PAST_MEETING_PARTICIPANT": "{base_url}/past_meetings/{meeting_id}/participants?page_size={page_size}",
    "RECORDING": "{base_url}/users/{user_id}/recordings?page_size={page_size}&from={date_from}&to={date_to}",
    "CHANNEL": "{base_url}/chat/users/{user_id}/channels?page_size={page_size}",
    "CHAT": "{base_url}/chat/users/{user_id}/messages?page_size={page_size}&search_key=%20&search_type={chat_type}&from={date_from}&to={date_to}",
}


def format_recording_date(date):
    return date.strftime("%Y-%m-%d")


def format_chat_date(date):
    return date.strftime("%Y-%m-%dT%H:%M:%SZ")


class TokenError(Exception):
    pass


class ZoomResourceNotFound(Exception):
    pass


class ZoomAPIToken:
    def __init__(self, http_session, configuration, logger_):
        self._http_session = http_session
        self._token_cache = CacheWithTimeout()
        self._logger = logger_
        self.account_id = configuration["account_id"]
        self.client_id = configuration["client_id"]
        self.client_secret = configuration["client_secret"]

    def set_logger(self, logger_):
        self._logger = logger_

    async def get(self, is_cache=True):
        cached_value = self._token_cache.get_value() if is_cache else None

        if cached_value:
            return cached_value

        now = datetime.utcnow()
        access_token, expires_in = await self._fetch_token()
        self._token_cache.set_value(access_token, now + timedelta(seconds=expires_in))

        return access_token

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _fetch_token(self):
        self._logger.debug("Generating access token.")
        url = AUTH.format(base_auth_url=BASE_AUTH_URL, account_id=self.account_id)
        content = f"{self.client_id}:{self.client_secret}"
        base64_credentials = get_base64_value(content=content.encode("utf-8"))
        request_headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Authorization": f"Basic {base64_credentials}",
        }

        try:
            async with self._http_session.post(
                url=url, headers=request_headers
            ) as response:
                json_response = await response.json()
                return json_response.get("access_token"), json_response.get(
                    "expires_in", 3599
                )
        except Exception as exception:
            msg = f"Error while generating access token. Exception {exception}."
            raise TokenError(msg) from exception


class ZoomAPISession:
    def __init__(self, http_session, api_token, logger_):
        self._sleeps = CancellableSleeps()
        self._logger = logger_

        self._http_session = http_session
        self._api_token = api_token

    def set_logger(self, logger_):
        self._logger = logger_

    def close(self):
        self._sleeps.cancel()

    @asynccontextmanager
    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=ZoomResourceNotFound,
    )
    async def _get(self, absolute_url):
        try:
            token = await self._api_token.get()
            headers = {
                "authorization": f"Bearer {token}",
                "content-type": "application/json",
            }

            async with self._http_session.get(
                url=absolute_url, headers=headers
            ) as response:
                yield response
        except ClientResponseError as exception:
            if exception.status == 401:
                await self._api_token.get(is_cache=False)
                raise
            elif exception.status == 404:
                msg = "Resource Not Found"
                raise ZoomResourceNotFound(msg) from exception
            else:
                raise
        except Exception:
            raise

    async def fetch(self, url):
        try:
            async with self._get(absolute_url=url) as response:
                return await response.json()
        except Exception as exception:
            self._logger.warning(
                f"Data for {url} is being skipped. Error: {exception}."
            )

    async def content(self, url):
        try:
            async with self._get(absolute_url=url) as response:
                return await response.text()
        except Exception as exception:
            self._logger.warning(
                f"Content for {url} is being skipped. Error: {exception}."
            )

    async def scroll(self, url):
        scroll_url = url

        while True:
            response = await self.fetch(url=scroll_url)

            if not response:
                break

            yield response

            if next_page_token := response.get("next_page_token", None):
                scroll_url = f"{url}&next_page_token={next_page_token}"
            else:
                break


class ZoomClient:
    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self._logger = logger

        self.configuration = configuration
        self.http_session = aiohttp.ClientSession(
            trust_env=True,
            raise_for_status=True
        )

        self.api_token = ZoomAPIToken(
            http_session=self.http_session,
            configuration=configuration,
            logger_=self._logger,
        )
        self.api_client = ZoomAPISession(
            http_session=self.http_session,
            api_token=self.api_token,
            logger_=self._logger,
        )

    def set_logger(self, logger_):
        self._logger = logger_
        self.api_token.set_logger(self._logger)
        self.api_client.set_logger(self._logger)

    async def close(self):
        await self.http_session.close()
        self.api_client.close()

    async def get_users(self):
        url = APIS["USERS"].format(base_url=BASE_URL, page_size=MEETING_PAGE_SIZE)
        async for users in self.api_client.scroll(url=url):
            for user in users.get("users", []) or []:
                yield user

    async def get_meetings(self, user_id, meeting_type):
        url = APIS["MEETINGS"].format(
            base_url=BASE_URL,
            user_id=user_id,
            meeting_type=meeting_type,
            page_size=MEETING_PAGE_SIZE,
        )
        async for meetings in self.api_client.scroll(url=url):
            for meeting in meetings.get("meetings", []) or []:
                yield meeting

    async def get_past_meeting(self, meeting_id):
        url = APIS["PAST_MEETING"].format(base_url=BASE_URL, meeting_id=meeting_id)
        return await self.api_client.fetch(url=url)

    async def get_past_meeting_participants(self, meeting_id):
        url = APIS["PAST_MEETING_PARTICIPANT"].format(
            base_url=BASE_URL, meeting_id=meeting_id, page_size=MEETING_PAGE_SIZE
        )
        async for participants in self.api_client.scroll(url=url):
            for participant in participants.get("participants", []) or []:
                yield participant

    async def get_recordings(self, user_id):
        # Zoom recording does not retrieve data that is more than 4 months old.
        end_date = datetime.utcnow()
        for _ in range(self.configuration["recording_age"]):
            start_date = end_date + timedelta(days=-30)
            url = APIS["RECORDING"].format(
                base_url=BASE_URL,
                user_id=user_id,
                date_from=format_recording_date(date=start_date),
                date_to=format_recording_date(date=end_date),
                page_size=MEETING_PAGE_SIZE,
            )
            async for recordings in self.api_client.scroll(url=url):
                for recording in recordings.get("meetings", []) or []:
                    yield recording
            end_date = start_date

    async def get_channels(self, user_id):
        url = APIS["CHANNEL"].format(
            base_url=BASE_URL, user_id=user_id, page_size=CHAT_PAGE_SIZE
        )
        async for channels in self.api_client.scroll(url=url):
            for channel in channels.get("channels", []) or []:
                yield channel

    async def get_chats(self, user_id, chat_type):
        # Zoom chat does not retrieve data that is more than 6 months old.
        end_date = datetime.utcnow()
        start_date = end_date + timedelta(days=-180)
        url = APIS["CHAT"].format(
            base_url=BASE_URL,
            user_id=user_id,
            chat_type=chat_type,
            date_from=format_chat_date(date=start_date),
            date_to=format_chat_date(date=end_date),
            page_size=CHAT_PAGE_SIZE,
        )
        async for chats in self.api_client.scroll(url=url):
            for chat in chats.get("messages", []) or []:
                yield chat

    async def get_file_content(self, download_url):
        content = await self.api_client.content(url=download_url)
        yield content.encode("utf-8")


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
                yield self._format_doc(
                    doc=live_meeting, doc_time=live_meeting.get("created_at")
                ), None

            async for upcoming_meeting in self.client.get_meetings(
                user_id=user.get("id"), meeting_type="upcoming_meetings"
            ):
                yield self._format_doc(
                    doc=upcoming_meeting,
                    doc_time=upcoming_meeting.get("created_at"),
                ), None

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
                        yield self._format_doc(
                            doc=previous_meeting,
                            doc_time=previous_meeting.get("created_at"),
                        ), None
                    else:
                        yield self._format_doc(
                            doc=previous_meeting_details,
                            doc_time=previous_meeting_details.get("end_time"),
                        ), None
                else:
                    yield self._format_doc(
                        doc=previous_meeting,
                        doc_time=previous_meeting.get("created_at"),
                    ), None

            async for recording in self.client.get_recordings(user_id=user.get("id")):
                yield self._format_doc(
                    doc=recording, doc_time=recording.get("start_time")
                ), None

            async for channel in self.client.get_channels(user_id=user.get("id")):
                yield self._format_doc(doc=channel, doc_time=iso_utc()), None

            async for chat_message in self.client.get_chats(
                user_id=user.get("id"), chat_type="message"
            ):
                yield self._format_doc(
                    doc=chat_message, doc_time=chat_message.get("date_time")
                ), None

            async for chat_file in self.client.get_chats(
                user_id=user.get("id"), chat_type="file"
            ):
                chat_file["id"] = chat_file.get("file_id")
                yield self._format_doc(
                    doc=chat_file, doc_time=chat_file.get("date_time")
                ), partial(self.get_content, chat_file=chat_file.copy())
