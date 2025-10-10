#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import re
import time

from connectors_sdk.source import BaseDataSource

from connectors.sources.slack.client import SlackAPIError, SlackClient
from connectors.utils import dict_slice

USER_ID_PATTERN = re.compile(r"<@([A-Z0-9]+)>")

# TODO list
# Nice to haves:
# - expand links
#   - links:read scope needed?
# - configure which channels to sync via deny list
# - write an ftest


class SlackDataSource(BaseDataSource):
    name = "Slack"
    service_type = "slack"

    def __init__(self, configuration):
        """Set up the connection to the Slack.

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
                "sensitive": True,
                "type": "str",
            },
            "fetch_last_n_days": {
                "display": "numeric",
                "label": "Days of message history to fetch",
                "order": 2,
                "tooltip": "How far back in time to request message history from slack. Messages older than this will not be indexed.",
                "type": "int",
            },
            "auto_join_channels": {
                "display": "toggle",
                "label": "Automatically join channels",
                "order": 3,
                "tooltip": "The Slack application bot will only be able to read conversation history from channels it has joined. The default requires it to be manually invited to channels. Enabling this allows it to automatically invite itself into all public channels.",
                "type": "bool",
                "value": False,
            },
            "sync_users": {
                "display": "toggle",
                "label": "Sync users",
                "order": 4,
                "tooltip": "Whether or not Slack Users should be indexed as documents in Elasticsearch.",
                "type": "bool",
                "value": True,
            },
        }

    async def ping(self):
        if not await self.slack_client.ping():
            msg = "Could not connect to Slack"
            raise Exception(msg)

    async def close(self):
        await self.slack_client.close()

    async def get_docs(self, filtering=None):
        self._logger.info("Fetching all users")
        async for user in self.slack_client.list_users():
            self.usernames[user["id"]] = self.get_username(user)
            if self.configuration["sync_users"]:
                yield self.remap_user(user), None
        self._logger.info("Fetching all channels and messages")
        async for message in self.channels_and_messages():
            yield message, None

    async def channels_and_messages(self):
        current_unix_timestamp = time.time()
        past_unix_timestamp = current_unix_timestamp - self.n_days_to_fetch * 24 * 3600
        async for channel in self.slack_client.list_channels(
            not self.auto_join_channels
        ):
            self._logger.info(f"Listed channel: '{channel['name']}'")
            yield self.remap_channel(channel)
            channel_id = channel["id"]
            if self.auto_join_channels and not channel.get("is_member", False):
                try:
                    await self.slack_client.join_channel(
                        channel_id
                    )  # can't get channel content if the bot is not in the channel
                except SlackAPIError as e:
                    self._logger.warning(
                        f"Not syncing channel: '{channel['name']}' because: {e.reason}"
                    )
                    continue
            async for message in self.slack_client.list_messages(
                channel, past_unix_timestamp, current_unix_timestamp
            ):
                yield self.remap_message(message, channel)

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

        # Message does not have id - `ts` is unique id of the message in the channel
        # Full message unique id is workspace.id -> channel.id -> message.ts
        message_id = f"{channel['id']}-{message['ts']}"

        return {
            "_id": message_id,
            "channel": channel["name"],
            "author": self.usernames.get(user_id, user_id),
            "text": re.sub(USER_ID_PATTERN, convert_usernames, message["text"]),
            "edited_ts": message.get("edited", {}).get("ts"),
        } | dict_slice(
            message,
            (
                "ts",
                "type",
                "subtype",
                "reply_count",
                "latest_reply",
                "thread_ts",
            ),
        )
