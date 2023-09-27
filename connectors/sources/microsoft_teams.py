#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft Teams source module responsible to fetch documents from Microsoft Teams.
"""
import asyncio
import os
from calendar import month_name
from datetime import datetime, timedelta
from enum import Enum
from functools import cached_property, partial

import aiofiles
import aiohttp
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ClientResponseError
from msal import ConfidentialClientApplication

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CacheWithTimeout,
    CancellableSleeps,
    ConcurrentTasks,
    MemQueue,
    RetryStrategy,
    convert_to_b64,
    html_to_text,
    retryable,
    url_encode,
)

QUEUE_MEM_SIZE = 5 * 1024 * 1024  # Size in Megabytes
MAX_CONCURRENCY = 80
FILE_WRITE_CHUNK_SIZE = 1024 * 64  # 64KB default SSD page size
MAX_FILE_SIZE = 10485760
TOKEN_EXPIRES = 3599
RETRY_COUNT = 3
RETRY_SECONDS = 30
RETRY_INTERVAL = 2

RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.

if "OVERRIDE_URL" in os.environ:
    logger.warning("x" * 50)
    logger.warning(
        f"MICROSOFT TEAMS CONNECTOR CALLS ARE REDIRECTED TO {os.environ['OVERRIDE_URL']}"
    )
    logger.warning("IT'S SUPPOSED TO BE USED ONLY FOR TESTING")
    logger.warning("x" * 50)
    override_url = os.environ["OVERRIDE_URL"]
    BASE_URL = override_url
    GRAPH_API_AUTH_URL = override_url
    GRAPH_ACQUIRE_TOKEN_URL = override_url
else:
    GRAPH_API_AUTH_URL = "https://login.microsoftonline.com"
    GRAPH_ACQUIRE_TOKEN_URL = "https://graph.microsoft.com/.default"
    BASE_URL = "https://graph.microsoft.com/v1.0"

SCOPE = [
    "User.Read.All",
    "TeamMember.Read.All",
    "ChannelMessage.Read.All",
    "Chat.Read",
    "Chat.ReadBasic",
    "Calendars.Read",
]
USER_MEETING_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


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
            raise TokenFetchFailed(
                f"Failed to authorize to Graph API. Please verify, that provided details are valid. Error: {token_metadata.get('error_description')}"
            )

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
            raise ThrottledError(
                f"Service is throttled because too many requests have been made to {absolute_url}: Exception: {e}"
            ) from e
        elif e.status == 403:
            raise PermissionsMissing(
                f"Received Unauthorized response for {absolute_url}.\nVerify that the correct Graph API and Microsoft Teams permissions are granted to the app and admin consent is given. If the permissions and consent are correct, wait for several minutes and try again."
            ) from e
        elif e.status == 404:
            raise NotFound from e
        elif e.status == 500:
            raise InternalServerError(
                f"Received InternalServerError error for {absolute_url}: Exception: {e}"
            ) from e
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


class MicrosoftTeamsFormatter:
    """Format documents"""

    def __init__(self, schema):
        self.schema = schema

    def map_document_with_schema(
        self,
        document,
        item,
        document_type,
    ):
        """Prepare key mappings for documents

        Args:
            document(dictionary): Modified document
            item (dictionary): Document from Microsoft Teams.
            document_type(string): Name of function to be called for fetching the mapping.

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        for elasticsearch_field, sharepoint_field in document_type().items():
            document[elasticsearch_field] = item.get(sharepoint_field)

    def format_doc(self, item, document_type, **kwargs):
        document = {}
        for elasticsearch_field, sharepoint_field in kwargs["document"].items():
            document[elasticsearch_field] = sharepoint_field
        self.map_document_with_schema(
            document=document, item=item, document_type=document_type
        )
        return document

    def format_user_chat_meeting_recording(self, item, url):
        document = {"type": UserEndpointName.MEETING_RECORDING.value}
        document.update(
            {
                "_id": item.get("eventDetail", {}).get("callId"),
                "title": item.get("eventDetail", {}).get("callRecordingDisplayName"),
                "url": url,
                "_timestamp": item.get("lastModifiedDateTime"),
            }
        )
        return document

    def get_calendar_detail(self, calendar):
        body = ""
        organizer = calendar.get("organizer", {}).get("emailAddress").get("name")
        calendar_recurrence = calendar.get("recurrence")
        if calendar_recurrence:
            recurrence_range = calendar_recurrence.get("range")
            pattern = calendar_recurrence.get("pattern")
            occurrence = f"{pattern['interval']}" if pattern.get("interval") else ""
            pattern_type = pattern.get("type", "")

            # In case type of meeting is daily so body will be: Recurrence: Occurs every 1 day starting {startdate}
            # until {enddate}
            if pattern_type == "daily":
                days = f"{occurrence} day"

            # If type of meeting  is yearly so body will be: Recurrence: Occurs every year on day 5 of march starting
            # {date} until {enddate}
            elif pattern_type in ["absoluteYearly", "relativeYearly"]:
                day_pattern = (
                    f"on day {pattern['dayOfMonth']}"
                    if pattern.get("dayOfMonth")
                    else f"on {pattern['index']} {','.join(pattern['daysOfWeek'])}"
                )
                days = f"year {day_pattern} of {month_name[pattern['month']]}"

            # If type of meeting  is monthly so body will be: Recurrence: Occurs every month on day 5 of march
            # starting {date} until {enddate}
            elif pattern_type in ["absoluteMonthly", "relativeMonthly"]:
                days_pattern = (
                    f"on day {pattern['dayOfMonth']}"
                    if pattern.get("dayOfMonth")
                    else f"on {pattern['index']} {','.join(pattern['daysOfWeek'])}"
                )
                days = f"{occurrence} month {days_pattern}"

            # Else goes in weekly situation where body will be: Recurrence: Occurs Every 3 week on monday,tuesday,
            # wednesday starting {date} until {enddate}
            else:
                week = ",".join(pattern.get("daysOfWeek"))
                days = f"{occurrence} week on {week}"

            date = (
                f"{recurrence_range.get('startDate')}"
                if recurrence_range.get("type", "") == "noEnd"
                else f"{recurrence_range.get('startDate')} until {recurrence_range.get('endDate')}"
            )
            recurrence = f"Occurs Every {days} starting {date}"
            body = f"Recurrence: {recurrence} Organizer: {organizer}"

        else:
            start_time = datetime.strptime(
                calendar["start"]["dateTime"][:-4], USER_MEETING_DATETIME_FORMAT
            ).strftime("%d %b, %Y at %H:%M")
            end_time = datetime.strptime(
                calendar["end"]["dateTime"][:-4], USER_MEETING_DATETIME_FORMAT
            ).strftime("%d %b, %Y at %H:%M")
            body = f"Schedule: {start_time} to {end_time} Organizer: {organizer}"
        return body

    def format_user_calendars(self, item):
        document = {"type": UserEndpointName.MEETING.value}
        attendee_list = (
            [
                f"{attendee.get('emailAddress', {}).get('name')}({attendee.get('emailAddress', {}).get('address')})"
                for attendee in item["attendees"]
            ]
            if item.get("attendees")
            else []
        )
        document.update(
            {  # pyright: ignore
                "attendees": attendee_list,
                "online_meeting_url": item["onlineMeeting"].get("joinUrl")
                if item.get("onlineMeeting")
                else "",
                "description": item.get("bodyPreview"),
                "meeting_detail": self.get_calendar_detail(calendar=item),
                "location": [
                    f"{location['displayName']}" for location in item["locations"]
                ]
                if item.get("locations")
                else [],
            }
        )
        self.map_document_with_schema(
            document=document, item=item, document_type=self.schema.meeting
        )
        return document

    def format_channel_message(self, item, channel_name, message_content):
        document = {"type": TeamEndpointName.MESSAGE.value}
        document.update(
            {  # pyright: ignore
                "sender": item.get("from", {}).get("user", {}).get("displayName", ""),
                "channel": channel_name,
                "message": message_content,
                "attached_documents": self.format_attachment_names(
                    attachments=item.get("attachments")
                ),
            }
        )
        self.map_document_with_schema(
            document=document, item=item, document_type=self.schema.channel_message
        )
        return document

    def format_channel_meeting(self, reply):
        document = {"type": TeamEndpointName.MEETING.value}
        event = reply["eventDetail"]
        if event.get("@odata.type") == "#microsoft.graph.callEndedEventMessageDetail":
            participant_list = []
            for participant in event.get("callParticipants", []):
                user = participant.get("participant", {}).get("user")
                if user:
                    participant_list.append(user.get("displayName"))
            participant_names = ", ".join(participant_list)
            document.update(
                {
                    "_id": event.get("callId"),
                    "_timestamp": reply.get("lastModifiedDateTime"),
                    "participants": participant_names,
                }
            )
        elif (
            event.get("@odata.type")
            == "#microsoft.graph.callRecordingEventMessageDetail"
        ):
            if event.get("callRecordingUrl") and (
                ".sharepoint.com" in event["callRecordingUrl"]
            ):
                document.update(
                    {
                        "title": event.get("callRecordingDisplayName"),
                        "recording_url": event.get("callRecordingUrl"),
                    }
                )
        return document

    async def format_user_chat_messages(self, chat, message, message_content, members):
        if chat.get("topic"):
            message.update({"title": chat["topic"]})
        else:
            message.update({"title": members})
        message.update(
            {
                "webUrl": chat.get("webUrl"),
                "chatType": chat.get("chatType"),
                "sender": message.get("from", {})
                .get("user", {})
                .get("displayName", ""),
                "message": message_content,
            }
        )
        return message

    def format_attachment_names(self, attachments):
        if not attachments:
            return ""

        return ",".join(
            attachment.get("name")
            for attachment in attachments
            if attachment.get("name", "")
        )


class MicrosoftTeamsDataSource(BaseDataSource):
    """Microsoft Teams"""

    name = "Microsoft Teams"
    service_type = "microsoft_teams"

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
        await self.queue.put(EndSignal.TEAM_TASK_FINISHED)

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
