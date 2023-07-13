from connectors.source import BaseDataSource

import aiohttp
from contextlib import asynccontextmanager
from functools import partial, wraps


BASE_URL = "https://slack.com/api"

LIST_CONVERSATIONS = "list_convos"
JOIN_CONVERSATIONS = "join_convos"
CONVERSATION_HISTORY = "convo_history"

ENDPOINTS = {
    LIST_CONVERSATIONS: "conversations.list",
    JOIN_CONVERSATIONS: "conversations.join",
    CONVERSATION_HISTORY: "conversations.history",
}

# TODO list
# - write unit tests
# - write an ftest
# - replace square brackest with get()
# - better error handling/retrying
# - sync users to doctor author and content
# - do something to fix emojis?
# - document required scopes
# - set up vault creds for slack
# - configure which channels to sync?
# - configure how far back to sync?

def retryable_aiohttp_call(retries): # TODO, make a base client class
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

class SlackClient:
    def __init__(self, token):
        self.token = token
        self._http_session = aiohttp.ClientSession(  # TODO: lazy create this
            headers=self._headers(),
            timeout=aiohttp.ClientTimeout(total=None),
            raise_for_status=True,
        )

    def set_logger(self, logger_):
        self._logger = logger_
    def ping(self):
        return


    def close(self):
        return

    async def get_all_messages(self):
        async for channel in self.list_channels():
            self._logger.info(f"Listed channel: '{channel['name']}'")
            channel_id = channel['id']
            join_response = await self.join_channel(channel_id) # can't get channel content if the bot is not in the channel
            if join_response.get("ok"):
                async for message in self.list_messages(channel_id):
                    yield message | {"channel": channel['name']}
            else:
                self._logger.warning(f"Not syncing channel: '{channel['name']}' because: {join_response.get('error')}")


    async def list_channels(self):
        url = f"{BASE_URL}/{ENDPOINTS[LIST_CONVERSATIONS]}"
        response = await self._get_json(url)
        for channel in response['channels']:
            yield channel

    async def join_channel(self, channel_id):
        url = f"{BASE_URL}/{ENDPOINTS[JOIN_CONVERSATIONS]}?channel={channel_id}"
        return await self._get_json(url)

    async def list_messages(self, channel_id, cursor=None):
        while True:
            url = f"{BASE_URL}/{ENDPOINTS[CONVERSATION_HISTORY]}?channel={channel_id}"
            if cursor:
                url = f"{url}&cursor={cursor}"
            response = await self._get_json(url)
            for message in response['messages']:
                if message['type'] == 'message':
                    yield {"text": message["text"], "author": message["user"], "_id": message["ts"], "ts": message["ts"]} # TODO: other fields too?
            cursor = response.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

    async def _get_json(self, absolute_url):
        self._logger.debug(f"Fetching url: {absolute_url}")
        async with self._call_api(absolute_url) as resp:
            return await resp.json()

    @asynccontextmanager
    @retryable_aiohttp_call(retries=3)
    async def _call_api(self, absolute_url):
        async with self._http_session.get(
                absolute_url,
                headers=self._headers(),
        ) as resp:
            yield resp
            return

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
        token = configuration["token"]
        self.slack_client = SlackClient(token)

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
            # TODO, configure which channels?
        }

    async def ping(self):
        self.slack_client.ping()

    async def close(self):
        self.slack_client.close()

    async def get_docs(self, filtering=None):
        async for message in self.slack_client.get_all_messages():
            yield message, None