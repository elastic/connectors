#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import aiohttp
from aiohttp.client_exceptions import ClientResponseError

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import CancellableSleeps, dict_slice, retryable

BASE_URL = "https://slack.com/api"

CURSOR = "cursor"
RESPONSE_METADATA = "response_metadata"
NEXT_CURSOR = "next_cursor"
DEFAULT_RETRY_SECONDS = 3
DEFAULT_RETRY_SLEEP = 30
PAGE_SIZE = 200
USER_ID_PATTERN = re.compile(r"<@([A-Z0-9]+)>")

# TODO list
# Nice to haves:
# - expand links
#   - links:read scope needed?
# - configure which channels to sync via deny list
# - write an ftest


class ThrottledError(Exception):
    """Internal exception class to indicate that request was throttled by the API"""

    pass


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

    def ping(self):
        return self.test_auth()

    def close(self):
        self._http_session.close()
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

    async def list_messages(self, channel_id, oldest, latest):
        self._logger.info(
            f"Fetching messages between {oldest} and {latest} from channel: '{channel_id}'"
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

    async def test_auth(self):
        url = f"{BASE_URL}/auth.test"
        response = await self._get_json(url)
        return response.get("ok", False)

    def _add_cursor(self, url, cursor):
        return f"{url}&{CURSOR}={cursor}"

    def _get_next_cursor(self, response):
        return response.get(RESPONSE_METADATA, {}).get(NEXT_CURSOR)

    async def _get_json(self, absolute_url):
        self._logger.debug(f"Fetching url: {absolute_url}")
        async with self._call_api(absolute_url) as resp:
            return await resp.json()

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


class SlackDataSource(BaseDataSource):
    name = "Slack"
    service_type = "slack"

    def __init__(self, configuration):
        """Set up the connection to the Amazon S3.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.slack_client = SlackClient(configuration)
        self.auto_join_channels = configuration["auto_join_channels"]
        self.n_days_to_fetch = configuration["fetch_last_n_days"]
        self.usernames = {}

    def _set_internal_logger(self):
        self.slack_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        return {
            "token": {
                "label": "Authentication Token",
                "tooltip": "The Slack Authentication Token for the slack application you created. See the docs for details.",
                "order": 1,
                "type": "str",
                "sensitive": True,
                "required": True,
                "value": "",
            },
            "fetch_last_n_days": {
                "label": "Days of message history to fetch",
                "tooltip": "How far back in time to request message history from slack. Messages older than this will not be indexed.",
                "order": 2,
                "type": "int",
                "value": 180,
                "display": "numeric",
            },
            "auto_join_channels": {
                "label": "Automatically join channels",
                "tooltip": "The Slack application bot will only be able to read conversation history from channels it has joined. The default requires it to be manually invited to channels. Enabling this allows it to automatically invite itself into all public channels.",
                "order": 3,
                "type": "bool",
                "display": "toggle",
                "value": False,
            },
            "sync_users": {
                "label": "Sync users",
                "tooltip": "Whether or not Slack Users should be indexed as documents in Elasticsearch.",
                "order": 4,
                "type": "bool",
                "display": "toggle",
                "value": True,
            },
        }

    async def ping(self):
        if not self.slack_client.ping():
            raise Exception("Could not connect to Slack")

    async def close(self):
        self.slack_client.close()

    async def get_docs(self, filtering=None):
        self._logger.info("Fetching all users")
        async for user in self.slack_client.list_users():
            self.usernames[user["id"]] = self.get_username(user)
            if self.configuration["sync_users"]:
                yield self.remap_user(user), None
        async for message in self.channels_and_messages():
            yield message, None

    async def channels_and_messages(self):
        delta = timedelta(days=self.n_days_to_fetch)
        past_unix_timestamp = time.mktime((datetime.utcnow() - delta).timetuple())
        current_unix_timestamp = time.mktime(datetime.utcnow().timetuple())
        async for channel in self.slack_client.list_channels(
            not self.auto_join_channels
        ):
            self._logger.info(f"Listed channel: '{channel['name']}'")
            yield self.remap_channel(channel)
            channel_id = channel["id"]
            join_response = {}
            if self.auto_join_channels:
                join_response = await self.slack_client.join_channel(
                    channel_id
                )  # can't get channel content if the bot is not in the channel
            if not self.auto_join_channels or join_response.get("ok"):
                async for message in self.slack_client.list_messages(
                    channel_id, past_unix_timestamp, current_unix_timestamp
                ):
                    yield self.remap_message(message, channel)
            else:
                self._logger.warning(
                    f"Not syncing channel: '{channel['name']}' because: {join_response.get('error')}"
                )

    def get_username(self, user):
        """
        Given a user record from slack, try to find a good username for it.
        This is hard, because no one property is reliably present and optimal.
        This function goes through a variety of options, from most normalized and readable to least.
        :param user:
        :return: an identifier for the user - hopefully a human-readable name.
        """
        user_profile = user.get("profile", {})
        if user_profile.get("display_name_normalized"):
            return user_profile["display_name_normalized"]
        elif user_profile.get("real_name_normalized"):
            return user_profile["real_name_normalized"]
        elif user.get("real_name"):
            return user["real_name"]
        elif user.get("name"):
            return user["name"]
        else:
            return user[
                "id"
            ]  # Some Users do not have any names (like Bots). For these, we fall back on ID

    def remap_user(self, user):
        user_profile = user.get("profile", {})
        return {
            "_id": user["id"],
            "type": "user",
            "real_name_normalized": user_profile.get("real_name_normalized"),
            "profile_real_name": user_profile.get("real_name"),
            "display_name": user_profile.get("display_name"),
            "display_name_normalized": user_profile.get("display_name_normalized"),
            "first_name": user_profile.get("first_name"),
            "last_name": user_profile.get("last_name"),
        } | dict_slice(
            user,
            (
                "id",
                "name",
                "deleted",
                "real_name",
                "is_admin",
                "is_owner",
                "is_bot",
            ),
        )

    def remap_channel(self, channel):
        topic = channel.get("topic", {})
        topic_creator_id = topic.get("creator")
        purpose = channel.get("purpose", {})
        purpose_creator_id = purpose.get("creator")
        return {
            "_id": channel["id"],
            "type": "channel",
            "topic": topic.get("value"),
            "topic_author": self.usernames.get(topic_creator_id, topic_creator_id),
            "topic_last_updated": topic.get("last_set"),
            "purpose": purpose.get("value"),
            "purpose_author": self.usernames.get(
                purpose_creator_id, purpose_creator_id
            ),
            "purpose_last_updated": purpose.get("last_set"),
            "last_updated": channel.get("updated"),
        } | dict_slice(
            channel,
            (
                "id",
                "name",
                "name_normalized",
                "created",
            ),
        )

    def remap_message(self, message, channel):
        user_id = message.get("user", message.get("bot_id"))

        def convert_usernames(
            match_obj,
        ):  # used below in an re.sub to map on matching messages
            id_ = match_obj.group(1)
            return f"<@{self.usernames.get(id_, id_)}>"  # replace the ID with the mapped username, if there is one

        return {
            "_id": message["client_msg_id"],
            "channel": channel["name"],
            "author": self.usernames.get(user_id, user_id),
            "text": re.sub(USER_ID_PATTERN, convert_usernames, message["text"]),
            "edited_ts": message.get("edited", {}).get("ts"),
        } | dict_slice(
            message,
            (
                "client_msg_id",
                "ts",
                "type",
                "subtype",
                "reply_count",
                "latest_reply",
                "thread_ts",
            ),
        )
