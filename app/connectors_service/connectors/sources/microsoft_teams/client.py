#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
from datetime import datetime, timedelta
from enum import Enum

import aiohttp
from aiohttp import ClientResponseError

from msal import ConfidentialClientApplication

from connectors.utils import CacheWithTimeout, retryable, RetryStrategy, CancellableSleeps, url_encode
from connectors_sdk.logger import logger

SCOPE = [
    "User.Read.All",
    "TeamMember.Read.All",
    "ChannelMessage.Read.All",
    "Chat.Read",
    "Chat.ReadBasic",
    "Calendars.Read",
]
FILE_WRITE_CHUNK_SIZE = 1024 * 64  # 64KB default SSD page size
TOKEN_EXPIRES = 3599
RETRY_COUNT = 3
RETRY_SECONDS = 30
RETRY_INTERVAL = 2
RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.
override_url = os.environ["OVERRIDE_URL"]
BASE_URL = override_url
GRAPH_API_AUTH_URL = override_url
GRAPH_ACQUIRE_TOKEN_URL = override_url

if "OVERRIDE_URL" in os.environ:
    logger.warning("x" * 50)
    logger.warning(
        f"MICROSOFT TEAMS CONNECTOR CALLS ARE REDIRECTED TO {os.environ['OVERRIDE_URL']}"
    )
    logger.warning("IT'S SUPPOSED TO BE USED ONLY FOR TESTING")
    logger.warning("x" * 50)
else:
    GRAPH_API_AUTH_URL = "https://login.microsoftonline.com"
    GRAPH_ACQUIRE_TOKEN_URL = "https://graph.microsoft.com/.default"  # noqa S105
    BASE_URL = "https://graph.microsoft.com/v1.0"


class UserEndpointName(Enum):
    PING = "Ping"
    USERS = "Users"
    CHAT = "User Chat"
    CHATS_MESSAGE = "User Chat Messages"
    MEETING_RECORDING = "User Chat Meeting Recording"
    TABS = "User Chat Tabs"
    DRIVE = "User Drive"
    DRIVE_CHILDREN = "User Drive Children"
    ATTACHMENT = "User Chat Attachment"
    MEETING = "User Meeting"


class TeamEndpointName(Enum):
    TEAMS = "Teams"
    CHANNEL = "Team Channel"
    TAB = "Channel Tab"
    MESSAGE = "Channel Message"
    MEETING = "Channel Meeting"
    ATTACHMENT = "Channel Attachment"
    FILE = "File"
    ROOT_DRIVE_CHILDREN = "Root Drive Children"


class EndSignal(Enum):
    USER_CHAT_TASK_FINISHED = "USER_CHAT_TASK_FINISHED"
    TEAM_TASK_FINISHED = "TEAM_TASK_FINISHED"
    CHANNEL_TASK_FINISHED = "CHANNEL_TASK_FINISHED"
    CALENDAR_TASK_FINISHED = "CALENDAR_TASK_FINISHED"


URLS = {
    UserEndpointName.PING.value: "{base_url}/me",
    UserEndpointName.USERS.value: "{base_url}/users?$top=999",
    UserEndpointName.CHAT.value: "{base_url}/chats?$expand=members&$top=50",
    UserEndpointName.CHATS_MESSAGE.value: "{base_url}/chats/{chat_id}/messages?$top=50",
    UserEndpointName.TABS.value: "{base_url}/chats/{chat_id}/tabs",
    UserEndpointName.DRIVE.value: "{base_url}/users/{sender_id}/drive",
    UserEndpointName.DRIVE_CHILDREN.value: "{base_url}/drives/{drive_id}/items/root/children",
    UserEndpointName.ATTACHMENT.value: "{base_url}/drives/{drive_id}/items/{child_id}/children?$filter=name eq '{attachment_name}'",
    UserEndpointName.MEETING.value: "{base_url}/users/{user_id}/events?$top=50",
    TeamEndpointName.TEAMS.value: "{base_url}/teams?$top=999",
    TeamEndpointName.CHANNEL.value: "{base_url}/teams/{team_id}/channels",
    TeamEndpointName.TAB.value: "{base_url}/teams/{team_id}/channels/{channel_id}/tabs",
    TeamEndpointName.MESSAGE.value: "{base_url}/teams/{team_id}/channels/{channel_id}/messages?$expand=replies",
    TeamEndpointName.FILE.value: "{base_url}/teams/{team_id}/channels/{channel_id}/filesFolder",
    TeamEndpointName.ROOT_DRIVE_CHILDREN.value: "{base_url}/drives/{drive_id}/items/{item_id}/children?$top=5000",
}


class Schema:
    def chat_messages(self):
        return {
            "_id": "id",
            "_timestamp": "lastModifiedDateTime",
            "creation_time": "createdDateTime",
            "webUrl": "webUrl",
            "title": "title",
            "chatType": "chatType",
            "sender": "sender",
            "message": "message",
        }

    def chat_tabs(self):
        return {
            "_id": "id",
            "title": "displayName",
        }

    def chat_attachments(self):
        return {
            "_id": "id",
            "name": "name",
            "weburl": "webUrl",
            "size_in_bytes": "size",
            "_timestamp": "lastModifiedDateTime",
            "creation_time": "createdDateTime",
        }

    def meeting(self):
        return {
            "_id": "id",
            "creation_time": "createdDateTime",
            "_timestamp": "lastModifiedDateTime",
            "title": "subject",
            "Cancelled_status": "isCancelled",
            "web_link": "webLink",
            "reminder": "isReminderOn",
            "reminder_time": "reminderMinutesBeforeStart",
            "categories": "categories",
            "original_start_timezone": "originalStartTimeZone",
            "original_end_timezone": "originalEndTimeZone",
        }

    def teams(self):
        return {
            "_id": "id",
            "title": "displayName",
            "description": "description",
        }

    def channel(self):
        return {
            "_id": "id",
            "url": "webUrl",
            "title": "displayName",
            "description": "description",
            "creation_time": "createdDateTime",
        }

    def channel_tab(self):
        return {"_id": "id", "title": "displayName", "url": "webUrl"}

    def channel_message(self):
        return {
            "_id": "id",
            "url": "webUrl",
            "_timestamp": "lastModifiedDateTime",
            "creation_time": "createdDateTime",
        }

    def channel_attachment(self):
        return {
            "_id": "id",
            "name": "name",
            "weburl": "webUrl",
            "size_in_bytes": "size",
            "_timestamp": "lastModifiedDateTime",
            "creation_time": "createdDateTime",
        }


class NotFound(Exception):
    """Internal exception class to handle 404s from the API that has a meaning, that collection
    for specific object is empty.

    It's not an exception for us, we just want to return [], and this exception class facilitates it.
    """

    pass


class ThrottledError(Exception):
    """Internal exception class to indicate that request was throttled by the API"""

    pass


class InternalServerError(Exception):
    """Exception class to indicate that something went wrong on the server side."""

    pass


class PermissionsMissing(Exception):
    """Exception class to notify that specific Application Permission is missing for the credentials used.
    See: https://learn.microsoft.com/en-us/graph/permissions-reference
    """

    pass


class TokenFetchFailed(Exception):
    """Exception class to notify that connector was unable to fetch authentication token from either
    Microsoft Teams Graph API.

    Error message will indicate human-readable reason.
    """

    pass


class GraphAPIToken:
    """Class for handling access token for Microsoft Graph APIs"""

    def __init__(self, tenant_id, client_id, client_secret, username, password):
        """Initializer.

        Args:
            tenant_id (str): Azure AD Tenant Id
            client_id (str): Azure App Client Id
            client_secret (str): Azure App Client Secret Value
            username (str): Username of the Azure account to fetch the access_token
            password (str): Password of the Azure account to fetch the access_token"""

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password

        self._token_cache_with_client = CacheWithTimeout()
        self._token_cache_with_username = CacheWithTimeout()

    async def get_with_client(self):
        """Get bearer token for provided credentials.

        If token has been retrieved, it'll be taken from the cache.
        Otherwise, call to `_fetch_token` is made to fetch the token
        from 3rd-party service.

        Returns:
            str: bearer token for one of Microsoft services"""
        if RUNNING_FTEST:
            return

        cached_value = self._token_cache_with_client.get_value()

        if cached_value:
            return cached_value

        # We measure now before request to be on a pessimistic side
        now = datetime.utcnow()
        access_token, expires_in = await self._fetch_token(is_acquire_for_client=True)

        self._token_cache_with_client.set_value(
            access_token, now + timedelta(seconds=expires_in)
        )

        return access_token

    async def get_with_username_password(self):
        """Get bearer token for provided credentials.

        If token has been retrieved, it'll be taken from the cache.
        Otherwise, call to `_fetch_token` is made to fetch the token
        from 3rd-party service.

        Returns:
            str: bearer token for one of Microsoft services"""
        if RUNNING_FTEST:
            return

        cached_value = self._token_cache_with_username.get_value()
        if cached_value:
            return cached_value

        # We measure now before request to be on a pessimistic side
        now = datetime.utcnow()
        access_token, expires_in = await self._fetch_token()

        self._token_cache_with_username.set_value(
            access_token, now + timedelta(seconds=expires_in)
        )

        return access_token

    @retryable(
        retries=RETRY_COUNT,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _fetch_token(self, is_acquire_for_client=False):
        """Generate API token for usage with Graph API
        Args:
        is_acquire_for_client (boolean): True if token needs be generated using client. Default to false

        Returns:
            (str, int) - a tuple containing access token as a string and number of seconds it will be valid for as an integer
        """
        authority = f"{GRAPH_API_AUTH_URL}/{self.tenant_id}"

        auth_context = ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=authority,
        )
        if is_acquire_for_client:
            token_metadata = auth_context.acquire_token_for_client(
                scopes=[GRAPH_ACQUIRE_TOKEN_URL]
            )
        else:
            token_metadata = auth_context.acquire_token_by_username_password(
                username=self.username, password=self.password, scopes=SCOPE
            )
        if not token_metadata.get("access_token"):
            msg = f"Failed to authorize to Graph API. Please verify, that provided details are valid. Error: {token_metadata.get('error_description')}"
            raise TokenFetchFailed(msg)

        access_token = token_metadata.get("access_token")
        expires_in = int(token_metadata.get("expires_in", TOKEN_EXPIRES))
        return access_token, expires_in


class MicrosoftTeamsClient:
    """Client Class for API calls to Microsoft Teams"""

    def __init__(self, tenant_id, client_id, client_secret, username, password):
        self._sleeps = CancellableSleeps()
        self._http_session = aiohttp.ClientSession(
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=None),
            raise_for_status=True,
        )
        self._api_token = GraphAPIToken(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
        )

        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

    async def fetch(self, url):
        return await self._get_json(absolute_url=url)

    async def pipe(self, url, stream):
        try:
            async for response in self._get(absolute_url=url, use_token=False):
                async for data in response.content.iter_chunked(FILE_WRITE_CHUNK_SIZE):
                    await stream.write(data)
        except Exception as exception:
            self._logger.warning(
                f"Data for {url} is being skipped. Error: {exception}."
            )

    async def scroll(self, url):
        scroll_url = url

        while True:
            if graph_data := await self._get_json(scroll_url):
                # We're yielding the whole page here, not one item
                yield graph_data.get("value", [])

                if not graph_data.get("@odata.nextLink"):
                    break
                scroll_url = graph_data.get("@odata.nextLink")
            else:
                break

    async def _get_json(self, absolute_url):
        try:
            async for response in self._get(absolute_url=absolute_url):
                return await response.json()
        except Exception as exception:
            self._logger.warning(
                f"Data for {absolute_url} is being skipped. Error: {exception}."
            )

    @retryable(
        retries=RETRY_COUNT,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[NotFound, PermissionsMissing],
    )
    async def _get(self, absolute_url, use_token=True):
        try:
            if use_token:
                if any(
                    substring in absolute_url
                    for substring in [
                        "/drive",
                        "/items/root/children",
                        "/children?$filter=name eq",
                        "/events",
                    ]
                ):
                    token = await self._api_token.get_with_client()
                else:
                    token = await self._api_token.get_with_username_password()
                self._logger.debug(f"Calling Microsoft Teams Endpoint: {absolute_url}")
                async with self._http_session.get(
                    url=absolute_url,
                    headers={"authorization": f"Bearer {token}"},
                ) as resp:
                    yield resp
            else:
                async with self._http_session.get(
                    url=absolute_url,
                ) as resp:
                    yield resp
        except aiohttp.client_exceptions.ClientOSError:
            self._logger.error(
                "Graph API dropped the connection. It might indicate, that connector makes too many requests - decrease concurrency settings, otherwise Graph API can block this app."
            )
            raise
        except ClientResponseError as e:
            await self._handle_client_response_error(absolute_url, e)

    async def _handle_client_response_error(self, absolute_url, e):
        if e.status == 429 or e.status == 503:
            response_headers = e.headers or {}
            updated_response_headers = {
                key.lower(): value for key, value in response_headers.items()
            }
            retry_seconds = int(
                updated_response_headers.get("retry-after", RETRY_SECONDS)
            )
            self._logger.debug(
                f"Rate limited by Microsoft Teams. Retrying after {retry_seconds} seconds"
            )
            await self._sleeps.sleep(retry_seconds)
            msg = f"Service is throttled because too many requests have been made to {absolute_url}: Exception: {e}"
            raise ThrottledError(msg) from e
        elif e.status == 403:
            msg = f"Received Unauthorized response for {absolute_url}.\nVerify that the correct Graph API and Microsoft Teams permissions are granted to the app and admin consent is given. If the permissions and consent are correct, wait for several minutes and try again."
            raise PermissionsMissing(msg) from e
        elif e.status == 404:
            raise NotFound from e
        elif e.status == 500:
            msg = (
                f"Received InternalServerError error for {absolute_url}: Exception: {e}"
            )
            raise InternalServerError(msg) from e
        else:
            raise

    async def ping(self):
        return await self.fetch(
            url=URLS[UserEndpointName.PING.value].format(base_url=BASE_URL)
        )

    async def users(self):
        async for users in self.scroll(
            url=URLS[UserEndpointName.USERS.value].format(base_url=BASE_URL)
        ):
            yield users

    async def get_user_chats(self):
        async for chats in self.scroll(
            url=URLS[UserEndpointName.CHAT.value].format(base_url=BASE_URL)
        ):
            yield chats

    async def get_user_chat_messages(self, chat_id):
        async for messages in self.scroll(
            url=URLS[UserEndpointName.CHATS_MESSAGE.value].format(
                base_url=BASE_URL, chat_id=chat_id
            )
        ):
            yield messages

    async def get_user_chat_tabs(self, chat_id):
        async for tabs in self.scroll(
            url=URLS[UserEndpointName.TABS.value].format(
                base_url=BASE_URL, chat_id=chat_id
            )
        ):
            yield tabs

    async def get_user_drive(self, sender_id):
        return await self.fetch(
            url=URLS[UserEndpointName.DRIVE.value].format(
                base_url=BASE_URL, sender_id=sender_id
            ),
        )

    async def get_user_drive_root_children(self, drive_id):
        async for root_children_data in self.scroll(
            url=URLS[UserEndpointName.DRIVE_CHILDREN.value].format(
                base_url=BASE_URL, drive_id=drive_id
            ),
        ):
            for child in root_children_data:
                if child["name"].lower() == "microsoft teams chat files":
                    return child

    async def get_user_chat_attachments(self, sender_id, attachment_name):
        drive = await self.get_user_drive(sender_id=sender_id)
        child = await self.get_user_drive_root_children(drive_id=drive.get("id"))
        async for attachment in self.scroll(
            url=URLS[UserEndpointName.ATTACHMENT.value].format(
                base_url=BASE_URL,
                drive_id=drive.get("id"),
                child_id=child.get("id"),
                attachment_name=url_encode(attachment_name).replace("'", "''"),
            ),
        ):
            yield attachment

    async def get_calendars(self, user_id):
        async for events in self.scroll(
            url=URLS[UserEndpointName.MEETING.value].format(
                base_url=BASE_URL, user_id=user_id
            ),
        ):
            for event in events:
                yield event

    async def download_item(self, url, async_buffer):
        await self.pipe(url=url, stream=async_buffer)

    async def get_teams(self):
        async for teams in self.scroll(
            url=URLS[TeamEndpointName.TEAMS.value].format(base_url=BASE_URL)
        ):
            yield teams

    async def get_team_channels(self, team_id):
        async for channels in self.scroll(
            url=URLS[TeamEndpointName.CHANNEL.value].format(
                base_url=BASE_URL, team_id=team_id
            )
        ):
            yield channels

    async def get_channel_tabs(self, team_id, channel_id):
        async for channel_tabs in self.scroll(
            url=URLS[TeamEndpointName.TAB.value].format(
                base_url=BASE_URL, team_id=team_id, channel_id=channel_id
            )
        ):
            yield channel_tabs

    async def get_channel_messages(self, team_id, channel_id):
        async for channel_messages in self.scroll(
            url=URLS[TeamEndpointName.MESSAGE.value].format(
                base_url=BASE_URL, team_id=team_id, channel_id=channel_id
            )
        ):
            yield channel_messages

    async def get_channel_file(self, team_id, channel_id):
        file = await self.fetch(
            url=URLS[TeamEndpointName.FILE.value].format(
                base_url=BASE_URL, team_id=team_id, channel_id=channel_id
            )
        )
        return file

    async def get_channel_drive_childrens(self, drive_id, item_id):
        async for root_childrens in self.scroll(
            url=URLS[TeamEndpointName.ROOT_DRIVE_CHILDREN.value].format(
                base_url=BASE_URL, drive_id=drive_id, item_id=item_id
            )
        ):
            for child in root_childrens:
                if child.get("folder"):
                    async for documents in self.get_channel_drive_childrens(  # pyright: ignore
                        drive_id=drive_id, item_id=child["id"]
                    ):
                        yield documents
                yield child

    async def close(self):
        self._sleeps.cancel()
        await self._http_session.close()
