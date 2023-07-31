#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import wraps

import aiohttp
from aiohttp.client_exceptions import ClientResponseError

from connectors.source import BaseDataSource
from connectors.utils import CancellableSleeps

BASE_URL = "https://slack.com/api"

CURSOR = "cursor"
RESPONSE_METADATA = "response_metadata"
NEXT_CURSOR = "next_cursor"
DEFAULT_RETRY_SECONDS = 30

LIST_CONVERSATIONS = "list_convos"
JOIN_CONVERSATIONS = "join_convos"
CONVERSATION_HISTORY = "convo_history"
CONVERSATION_REPLIES = "threads"
LIST_USERS = "list_users"

ENDPOINTS = {
    LIST_CONVERSATIONS: "conversations.list",
    JOIN_CONVERSATIONS: "conversations.join",
    CONVERSATION_HISTORY: "conversations.history",
    CONVERSATION_REPLIES: "conversations.replies",
    LIST_USERS: "users.list",
}

# TODO list
# Must haves:
# - document required scopes
# - set up vault creds for slack
# Nice to haves:
# - expand links
#   - links:read scope needed?
# - configure which channels to sync via deny list
# - write unit tests
# - write an ftest
# - replace square brackest with get()
# - better error handling/retrying


def retryable_aiohttp_call(retries):  # TODO, make a base client class
    # TODO: improve utils.retryable to allow custom logic
    # that can help choose what to retry
    def wrapper(func):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            retry = 1
            while retry <= retries:
                try:
                    async for item in func(*args, **kwargs):
                        yield item
                    break
                except Exception as e:
                    if retry >= retries:
                        raise e

                    retry += 1

        return wrapped

    return wrapper


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
        self._sleeps = CancellableSleeps()

    def set_logger(self, logger_):
        self._logger = logger_

    def ping(self):
        return

    def close(self):
        self._http_session.close()
        return

    async def list_channels(self, only_my_channels, cursor=None):
        conversation_batch_limit = 200
        self._logger.debug("Iterating over all channels")
        if only_my_channels:
            self._logger.debug("Will only yield channels the bot belongs to")
        while True:
            url = f"{BASE_URL}/{ENDPOINTS[LIST_CONVERSATIONS]}?limit={conversation_batch_limit}"
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
        url = f"{BASE_URL}/{ENDPOINTS[JOIN_CONVERSATIONS]}?channel={channel_id}"
        return await self._get_json(url)

    async def list_messages(self, channel_id, oldest, latest):
        self._logger.info(
            f"Fetching messages between {oldest} and {latest} from channel: '{channel_id}'"
        )
        message_batch_limit = 200
        while True:
            url = f"{BASE_URL}/{ENDPOINTS[CONVERSATION_HISTORY]}?channel={channel_id}&limit={message_batch_limit}&oldest={oldest}&latest={latest}"
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
        message_batch_limit = 200
        cursor = None
        while True:
            url = f"{BASE_URL}/{ENDPOINTS[CONVERSATION_REPLIES]}?channel={channel_id}&ts={parent_ts}&limit={message_batch_limit}"
            if cursor:
                url = self._add_cursor(url, cursor)
            resonse = await self._get_json(url)
            for message in resonse["messages"]:
                if message["type"] == "message":
                    yield message
            cursor = self._get_next_cursor(resonse)
            if not cursor:
                break

    async def list_users(self, cursor=None):
        user_batch_limit = 200
        while True:
            url = f"{BASE_URL}/{ENDPOINTS[LIST_USERS]}?limit={user_batch_limit}"
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
            return await resp.json()

    @asynccontextmanager
    @retryable_aiohttp_call(retries=3)
    async def _call_api(self, absolute_url):
        try:
            async with self._http_session.get(
                absolute_url,
                headers=self._headers(),
            ) as resp:
                yield resp
                return
        except ClientResponseError as e:
            await self._handle_client_response_error(absolute_url, e)

    async def _handle_client_response_error(self, absolute_url, e):
        if e.status == 429:
            response_headers = e.headers or {}
            if "Retry-After" in response_headers:
                retry_seconds = int(response_headers["Retry-After"])
            else:
                self._logger.warning(
                    f"Response Code from Slack is {e.status} but Retry-After header is not found, using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
                )
                retry_seconds = DEFAULT_RETRY_SECONDS
            self._logger.debug(
                f"Rate Limited by Slack: waiting {retry_seconds} seconds before retry..."
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
        self.slack_client.ping()

    async def close(self):
        self.slack_client.close()

    async def get_docs(self, filtering=None):
        self._logger.info("Fetching all users")
        self.usernames = {}
        async for user in self.slack_client.list_users():
            self.usernames[user["id"]] = self.get_username(user)
            if self.configuration["sync_users"]:
                yield self.adapt_user(user), None
        async for message in self.channels_and_messages(self.usernames):
            yield message, None

    async def channels_and_messages(self, usernames):
        """
        Get all the messages from all the slack channels
        :param usernames: a dictionary of userID -> username mappings, to facilitate replacing message references to userids with usernames
        :return: generator for messages
        """
        delta = timedelta(days=self.n_days_to_fetch)
        past_unix_timestamp = time.mktime((datetime.utcnow() - delta).timetuple())
        current_unix_timestamp = time.mktime(datetime.utcnow().timetuple())
        async for channel in self.slack_client.list_channels(
            not self.auto_join_channels
        ):
            self._logger.info(f"Listed channel: '{channel['name']}'")
            yield self.adapt_channel(channel, usernames)
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
                    yield self.adapt_message(message, channel, usernames)
            else:
                self._logger.warning(
                    f"Not syncing channel: '{channel['name']}' because: {join_response.get('error')}"
                )

    def get_username(self, user):
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
            return user["id"]

    def adapt_user(self, user):
        user_profile = user.get("profile", {})
        user["real_name_normalized"] = user_profile.get("real_name_normalized")
        user["profile_real_name"] = user_profile.get("real_name")
        user["display_name"] = user_profile.get("display_name")
        user["display_name_normalized"] = user_profile.get("display_name_normalized")
        user["first_name"] = user_profile.get("first_name")
        user["last_name"] = user_profile.get("last_name")
        user = self._dict_slice(
            user,
            (
                "id",
                "name",
                "deleted",
                "real_name",
                "real_name_normalized",
                "profile_real_name",
                "display_name",
                "display_name_normalized",
                "first_name",
                "last_name",
                "is_admin",
                "is_owner",
                "is_bot",
            ),
        )
        user["_id"] = user["id"]
        user["type"] = "user"
        return user

    def adapt_channel(self, channel, usernames):
        topic = channel.get("topic", {})
        topic_creator_id = topic.get("creator")
        purpose = channel.get("purpose", {})
        purpose_creator_id = purpose.get("creator")
        channel["topic"] = topic.get("value")
        channel["topic_author"] = usernames.get(topic_creator_id, topic_creator_id)
        channel["topic_last_updated"] = topic.get("last_set")
        channel["purpose"] = purpose.get("value")
        channel["purpose_author"] = usernames.get(
            purpose_creator_id, purpose_creator_id
        )
        channel["purpose_last_updated"] = purpose.get("last_set")
        channel["last_updated"] = channel.get("updated")
        channel = self._dict_slice(
            channel,
            (
                "id",
                "name",
                "name_normalized",
                "topic",
                "topic_author",
                "topic_last_updated",
                "purpose",
                "purpose_author",
                "purpose_last_updated",
                "last_updated",
                "created",
            ),
        )
        channel["_id"] = channel["id"]
        channel["type"] = "channel"
        return channel

    def adapt_message(self, message, channel, usernames):
        user_id = message.get("user", message.get("bot_id"))
        message["author"] = usernames.get(user_id, user_id)
        message["text"] = re.sub(
            r"<@([A-Z0-9]+)>", self.convert_usernames, message["text"]  # type: ignore
        )
        message["edited_ts"] = message.get("edited", {}).get("ts")
        message = self._dict_slice(
            message,
            (
                "client_msg_id",
                "text",
                "author",
                "ts",
                "edited_ts",
                "type",
                "subtype",
                "reply_count",
                "latest_reply",
                "thread_ts",
            ),
        )
        message["_id"] = message["client_msg_id"]
        return message | {"channel": channel["name"]}

    def convert_usernames(self, match_obj):
        if match_obj.group(1) is not None:
            return f"<@{self.usernames.get(match_obj.group(1), match_obj.group(1))}>"

    def _dict_slice(self, hsh, keys):
        return {k: hsh.get(k) for k in keys}
