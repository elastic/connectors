#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from contextlib import asynccontextmanager
from datetime import datetime

import aiohttp
from aiohttp import ClientResponseError
from connectors_sdk.logger import logger

from connectors.utils import CancellableSleeps, retryable


class ThrottledError(Exception):
    """Internal exception class to indicate that request was throttled by the API"""

    pass


class SlackAPIError(Exception):
    """Internal exception class that wraps all non-ok responses from Slack"""

    def __init__(self, reason):
        self.__reason = reason

    @property
    def reason(self):
        return self.__reason


class SlackClient:
    def __init__(self, configuration):
        self.token = configuration["token"]
        self._http_session = aiohttp.ClientSession(
            headers=self._headers(),
            timeout=aiohttp.ClientTimeout(total=None),
            raise_for_status=True,
        )
        self._logger = logger
        self._sleeps = CancellableSleeps()

    def set_logger(self, logger_):
        self._logger = logger_

    async def ping(self):
        url = f"{BASE_URL}/auth.test"
        try:
            await self._get_json(url)
            return True
        except SlackAPIError:
            return False

    async def close(self):
        await self._http_session.close()
        self._sleeps.cancel()

    async def list_channels(self, only_my_channels):
        self._logger.debug("Iterating over all channels")
        cursor = None
        if only_my_channels:
            self._logger.debug("Will only yield channels the bot belongs to")
        while True:
            url = f"{BASE_URL}/conversations.list?limit={PAGE_SIZE}"
            if cursor:
                url = self._add_cursor(url, cursor)
            response = await self._get_json(url)

            for channel in response["channels"]:
                if only_my_channels and not channel.get("is_member", False):
                    continue
                self._logger.debug(f"Yielding channel '{channel['name']}'")
                yield channel
            cursor = self._get_next_cursor(response)
            if not cursor:
                break

    async def join_channel(self, channel_id):
        url = f"{BASE_URL}/conversations.join?channel={channel_id}"
        return await self._get_json(url)

    async def list_messages(self, channel, oldest, latest):
        channel_id = channel["id"]

        self._logger.info(
            f"Fetching messages between {datetime.utcfromtimestamp(oldest)} and {datetime.utcfromtimestamp(latest)} from channel: '{channel['name']}'"
        )
        while True:
            url = f"{BASE_URL}/conversations.history?channel={channel_id}&limit={PAGE_SIZE}&oldest={oldest}&latest={latest}"
            response = await self._get_json(url)
            for message in response.get("messages", []):
                latest = message.get("ts")
                if message.get("type") == "message":
                    if message.get("reply_count", 0) > 0:
                        async for thread_message in self.list_thread_messages(
                            channel_id, message["ts"]
                        ):
                            yield thread_message
                    else:
                        yield message
            if not response.get("has_more"):
                break

    async def list_thread_messages(self, channel_id, parent_ts):
        cursor = None
        while True:
            url = f"{BASE_URL}/conversations.replies?channel={channel_id}&ts={parent_ts}&limit={PAGE_SIZE}"
            if cursor:
                url = self._add_cursor(url, cursor)
            response = await self._get_json(url)
            for message in response["messages"]:
                if message["type"] == "message":
                    yield message
            cursor = self._get_next_cursor(response)
            if not cursor:
                break

    async def list_users(self, cursor=None):
        while True:
            url = f"{BASE_URL}/users.list?limit={PAGE_SIZE}"
            if cursor:
                url = self._add_cursor(url, cursor)
            response = await self._get_json(url)
            for member in response["members"]:
                yield member
            cursor = self._get_next_cursor(response)
            if not cursor:
                break

    def _add_cursor(self, url, cursor):
        return f"{url}&{CURSOR}={cursor}"

    def _get_next_cursor(self, response):
        return response.get(RESPONSE_METADATA, {}).get(NEXT_CURSOR)

    async def _get_json(self, absolute_url):
        self._logger.debug(f"Fetching url: {absolute_url}")
        async with self._call_api(absolute_url) as resp:
            json_content = await resp.json()

            if json_content.get("ok", False) is False:
                raise SlackAPIError(json_content.get("error", "unknown"))

            return json_content

    @asynccontextmanager
    @retryable(retries=3)
    async def _call_api(self, absolute_url):
        try:
            async with self._http_session.get(
                absolute_url,
                headers=self._headers(),
            ) as resp:
                yield resp
        except ClientResponseError as e:
            await self._handle_client_response_error(e)

    async def _handle_client_response_error(self, e):
        if e.status == 429:
            response_headers = e.headers or {}
            if "Retry-After" in response_headers:
                retry_seconds = int(
                    response_headers.get("Retry-After", DEFAULT_RETRY_SECONDS)
                )
            else:
                self._logger.warning(
                    f"Response Code from Slack is {e.status} but Retry-After header is not found, using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
                )
                retry_seconds = DEFAULT_RETRY_SECONDS
            self._logger.debug(
                f"Rate Limited by Slack: waiting at least {retry_seconds} seconds before retry..."
            )

            await self._sleeps.sleep(retry_seconds)
            raise ThrottledError from e  # will get retried - important thing is that we just slept
        else:
            raise

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "accept": "application/json",
        }


BASE_URL = "https://slack.com/api"
RESPONSE_METADATA = "response_metadata"
NEXT_CURSOR = "next_cursor"
DEFAULT_RETRY_SECONDS = 3
PAGE_SIZE = 200
CURSOR = "cursor"
