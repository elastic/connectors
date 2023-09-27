import base64
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
import pytest_asyncio
from aiohttp.client_exceptions import ClientOSError, ClientResponseError

from connectors.logger import logger
from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.microsoft_teams import (
    GraphAPIToken,
    InternalServerError,
    MicrosoftTeamsClient,
    MicrosoftTeamsDataSource,
    NotFound,
    PermissionsMissing,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source

USER_CHATS = [
    {
        "id": "19:2ea91886",
        "topic": "topic1",
        "createdDateTime": "2023-08-03T12:19:27.57Z",
        "lastUpdatedDateTime": "2023-08-09T07:17:26.482Z",
        "chatType": "oneOnOne",
        "webUrl": "https://teams.microsoft.com/l/chat/19:2ea91886",
        "tenantId": "a57a7700",
        "members": [
            {
                "id": "123-3=",
                "displayName": "Duumy3",
                "userId": "82e32463",
                "email": "dummy3@3hm.onmicrosoft.com",
            },
            {
                "id": "123-2=",
                "displayName": "Dummy2",
                "userId": "2ea91886",
                "email": "dummy2@3hm.onmicrosoft.com",
            },
        ],
    },
    {
        "id": "19:2ea918861",
        "topic": "dummy chat",
        "createdDateTime": "2023-08-03T12:19:27.57Z",
        "lastUpdatedDateTime": "2023-08-09T07:17:26.482Z",
        "chatType": "oneOnOne",
        "webUrl": "https://teams.microsoft.com/l/chat/19:2ea918861",
        "tenantId": "a57a7700",
        "members": [
            {
                "id": "123-1=",
                "displayName": "Duumy1",
                "userId": "82e32462",
                "email": "dummy1@3hm.onmicrosoft.com",
            },
            {
                "id": "123-2=",
                "displayName": "Dummy2",
                "userId": "2ea91886",
                "email": "dummy2@3hm.onmicrosoft.com",
            },
        ],
    },
]

MESSAGES = [
    {
        "id": "1691582610121",
        "messageType": "message",
        "createdDateTime": "2023-08-09T12:03:30.121Z",
        "lastModifiedDateTime": "2023-08-09T12:03:30.121Z",
        "deletedDateTime": None,
        "subject": None,
        "summary": None,
        "eventDetail": None,
        "webUrl": "https://3hm-my.sharepoint.com/11_dummy",
        "from": {
            "user": {
                "@odata.type": "#microsoft.graph.teamworkUserIdentity",
                "id": "82e32462",
                "displayName": "Dummy",
                "userIdentityType": "aadUser",
                "tenantId": "a57a7700",
            }
        },
        "body": {
            "contentType": "html",
            "content": '<attachment id="c5fddd7c"></attachment>',
        },
        "attachments": [
            {
                "id": "c5fddd7c",
                "contentType": "reference",
                "contentUrl": "https://3hmn-my.sharepoint.com//Microsoft%20Teams%20Chat%20Files/~%60!@$%5E8()_+-=%5B%5D%7B%7D;%27,k.txt",
                "content": None,
                "name": "~`!@$^8()_+-=[]{};',k.txt",
            }
        ],
    },
    {
        "id": "1691565463404",
        "messageType": "unknownFutureValue",
        "createdDateTime": "2023-08-09T07:17:43.404Z",
        "lastModifiedDateTime": "2023-08-09T07:17:44.668Z",
        "deletedDateTime": None,
        "subject": None,
        "summary": None,
        "chatId": "19:2ea91886",
        "webUrl": None,
        "from": None,
        "body": {"contentType": "html", "content": "<systemEventMessage/>"},
        "attachments": [],
        "eventDetail": {
            "@odata.type": "#microsoft.graph.callRecordingEventMessageDetail",
            "callId": "7ed8a6cb",
            "callRecordingDisplayName": "Call with alex wilber-20230809_124714-Meeting Recording.mp4",
            "callRecordingUrl": "https://3hm-my.sharepoint.com/:v:/g/personal/dummy_3hm_onmicrosoft_com/123",
            "meetingOrganizer": None,
            "initiator": {
                "application": None,
                "device": None,
                "user": {
                    "@odata.type": "#microsoft.graph.teamworkUserIdentity",
                    "id": "82e32462-782f-4988-b5f1-60f14b09a33c",
                    "displayName": None,
                    "userIdentityType": "aadUser",
                },
            },
        },
        "replies": [
            {
                "id": "1691565463406",
                "messageType": "dummy",
                "createdDateTime": "2023-08-09T07:17:43.404Z",
                "lastModifiedDateTime": "2023-08-09T07:17:44.668Z",
                "deletedDateTime": "2023-08-09T07:17:44.668Z",
                "subject": None,
                "summary": None,
                "chatId": "19:2ea91886",
                "webUrl": None,
                "from": None,
                "body": {"contentType": "html", "content": "<systemEventMessage/>"},
                "attachments": [],
                "eventDetail": {
                    "@odata.type": "#microsoft.graph.callEndedEventMessageDetail",
                    "callId": "7ed8a6cb",
                    "callRecordingDisplayName": "Call with alex wilber-20230809_124714-Meeting Recording.mp4",
                    "meetingOrganizer": None,
                    "callParticipants": [
                        {
                            "participant": {
                                "user": {
                                    "id": "2ea91886-",
                                    "displayName": "alex wilber",
                                    "userIdentityType": "aadUser",
                                }
                            }
                        },
                    ],
                },
            }
        ],
    },
]

ATTACHMENTS = [
    {
        "@microsoft.graph.downloadUrl": "https://3hm-my.sharepoint.com/personal/_layouts/15/download.aspx?UniqueId=c5fddd7c",
        "createdDateTime": "2023-08-09T12:03:07Z",
        "id": "01EL4RL6L43X64K4ATD5H34UUQPOMMTRQX",
        "lastModifiedDateTime": "2023-08-09T12:03:20Z",
        "name": "~`!@$^8()_+-=[]{};',k.txt",
        "webUrl": "https://3hm-my.sharepoint.com/personal/~%60!@$()_+-=%5B%5D%7B%7D;%27,k.txt",
        "size": 10914302891111,
        "file": {
            "mimeType": "text/plain",
            "hashes": {"quickXorHash": "6eO+6QCH+yCKU+k+tDEuDazGwkU="},
        },
    }
]

TABS = [
    {
        "id": "400960d1",
        "displayName": "testing Whiteboard",
        "webUrl": "https://teams.microsoft.com/l/entity/95de633a?webUrl=app.whiteboard.microsoft.com",
        "configuration": {
            "websiteUrl": "https://app.whiteboard.microsoft.com/tabappboard"
        },
    }
]

USERS = [
    {
        "displayName": "Adele Vance",
        "givenName": "Adele",
        "jobTitle": "Retail Manager",
        "mail": "AdeleV@w076v.onmicrosoft.com",
        "officeLocation": "18/2111",
        "preferredLanguage": "en-US",
        "id": "3fada5d6-125c-4aaa-bc23-09b5d301b2a7",
    },
    {
        "displayName": "Alex Wilber",
        "givenName": "Alex",
        "jobTitle": "Marketing Assistant",
        "mail": "AlexW@w076v.onmicrosoft.com",
        "officeLocation": "131/1104",
        "preferredLanguage": "en-US",
        "id": "8e083933-1720-4d2f-80d0-3976669b40ee",
    },
]

EVENTS = [
    {
        "id": "AAMkADkyYTNmOTcw",
        "createdDateTime": "2023-08-10T05:47:41.5466652Z",
        "lastModifiedDateTime": "2023-08-10T05:48:53.6618239Z",
        "originalStartTimeZone": "India Standard Time",
        "originalEndTimeZone": "India Standard Time",
        "reminderMinutesBeforeStart": 15,
        "isReminderOn": True,
        "hasAttachments": False,
        "subject": "Transitive connector meeting",
        "bodyPreview": "Hey all attend the meeting without a fail...",
        "isAllDay": True,
        "isCancelled": False,
        "isOrganizer": True,
        "webLink": "https://outlook.office365.com/owa/?itemid=AAMkADkyYTNm?path=/calendar/item",
        "onlineMeetingUrl": None,
        "isOnlineMeeting": True,
        "categories": ["Red category"],
        "body": {"contentType": "html", "content": "<html><head>dummy</head></html>"},
        "start": {"dateTime": "2023-08-10T06:30:00.0000000", "timeZone": "UTC"},
        "end": {"dateTime": "2023-08-10T07:00:00.0000000", "timeZone": "UTC"},
        "locations": [
            {
                "displayName": "Microsoft Teams Meeting",
            }
        ],
        "recurrence": None,
        "attendees": [
            {
                "emailAddress": {
                    "name": "alex wilber",
                    "address": "alexwilber@3hm.onmicrosoft.com",
                }
            }
        ],
        "organizer": {
            "emailAddress": {"name": "Dummy", "address": "dummy@3hm.onmicrosoft.com"}
        },
        "onlineMeeting": {"joinUrl": "https://teams.microsoft.com/l/meetup-join/123"},
    },
    {
        "id": "AAMkADkyYTNmOTcw1",
        "createdDateTime": "2023-08-10T05:47:41.5466652Z",
        "lastModifiedDateTime": "2023-08-10T05:48:53.6618239Z",
        "originalStartTimeZone": "India Standard Time",
        "originalEndTimeZone": "India Standard Time",
        "reminderMinutesBeforeStart": 15,
        "isReminderOn": True,
        "hasAttachments": False,
        "subject": "Transitive connector meeting",
        "bodyPreview": "Hey all attend the meeting without a fail...",
        "isAllDay": False,
        "isCancelled": False,
        "isOrganizer": True,
        "webLink": "https://outlook.office365.com/owa/?itemid=AAMkADkyYTNm?path=/calendar/item",
        "onlineMeetingUrl": None,
        "isOnlineMeeting": True,
        "categories": ["Red category"],
        "body": {"contentType": "html", "content": "<html><head>dummy</head></html>"},
        "start": {"dateTime": "2023-08-10T06:30:00.0000000", "timeZone": "UTC"},
        "end": {"dateTime": "2023-08-10T07:00:00.0000000", "timeZone": "UTC"},
        "locations": [
            {
                "displayName": "Microsoft Teams Meeting",
            }
        ],
        "recurrence": {
            "pattern": {
                "type": "relativeMonthly",
                "interval": 1,
                "month": 0,
                "dayOfMonth": 0,
                "daysOfWeek": ["thursday"],
                "firstDayOfWeek": "sunday",
                "index": "second",
            },
            "range": {
                "type": "noEnd",
                "startDate": "2023-08-10",
                "endDate": "0001-01-01",
                "recurrenceTimeZone": "India Standard Time",
                "numberOfOccurrences": 0,
            },
        },
        "attendees": [
            {
                "emailAddress": {
                    "name": "alex wilber",
                    "address": "alexwilber@3hm.onmicrosoft.com",
                }
            }
        ],
        "organizer": {
            "emailAddress": {"name": "Dummy", "address": "dummy@3hm.onmicrosoft.com"}
        },
        "onlineMeeting": {"joinUrl": "https://teams.microsoft.com/l/meetup-join/123"},
    },
    {
        "id": "AAMkADkyYTNmOTcw2",
        "createdDateTime": "2023-08-10T05:47:41.5466652Z",
        "lastModifiedDateTime": "2023-08-10T05:48:53.6618239Z",
        "originalStartTimeZone": "India Standard Time",
        "originalEndTimeZone": "India Standard Time",
        "reminderMinutesBeforeStart": 15,
        "isReminderOn": True,
        "hasAttachments": False,
        "subject": "Transitive connector meeting",
        "bodyPreview": "Hey all attend the meeting without a fail...",
        "isAllDay": False,
        "isCancelled": False,
        "isOrganizer": True,
        "webLink": "https://outlook.office365.com/owa/?itemid=AAMkADkyYTNm?path=/calendar/item",
        "onlineMeetingUrl": None,
        "isOnlineMeeting": True,
        "categories": ["Red category"],
        "body": {"contentType": "html", "content": "<html><head>dummy</head></html>"},
        "start": {"dateTime": "2023-08-10T06:30:00.0000000", "timeZone": "UTC"},
        "end": {"dateTime": "2023-08-10T07:00:00.0000000", "timeZone": "UTC"},
        "locations": [
            {
                "displayName": "Microsoft Teams Meeting",
            }
        ],
        "recurrence": {
            "pattern": {
                "type": "absoluteYearly",
                "interval": 1,
                "month": 0,
                "dayOfMonth": 0,
                "daysOfWeek": ["thursday"],
                "firstDayOfWeek": "sunday",
                "index": "second",
            },
            "range": {
                "type": "noEnd",
                "startDate": "2023-08-10",
                "endDate": "0001-01-01",
                "recurrenceTimeZone": "India Standard Time",
                "numberOfOccurrences": 0,
            },
        },
        "attendees": [
            {
                "emailAddress": {
                    "name": "alex wilber",
                    "address": "alexwilber@3hm.onmicrosoft.com",
                }
            }
        ],
        "organizer": {
            "emailAddress": {"name": "Dummy", "address": "dummy@3hm.onmicrosoft.com"}
        },
        "onlineMeeting": {"joinUrl": "https://teams.microsoft.com/l/meetup-join/123"},
    },
]

TEAMS = [
    {
        "id": "25ab782d",
        "createdDateTime": None,
        "displayName": "team1",
        "description": "Welcome to the team that we've assembled to create the Mark 8.",
        "visibility": "public",
        "webUrl": None,
        "summary": None,
    }
]

CHANNELS = [
    {
        "id": "19:36b3f1125",
        "createdDateTime": "2023-08-08T08:23:13.984Z",
        "displayName": "channel2",
        "description": None,
        "tenantId": "a57a7700",
        "webUrl": "https://teams.microsoft.com/l/channel/19%3A36b3f1125c82456",
        "membershipType": "standard",
    }
]
MOCK_ATTACHMENT = {
    "created_at": "2023-05-01T09:09:31Z",
    "id": "123",
    "_timestamp": "2023-05-01T09:10:21Z",
    "name": "Document.docx",
    "type": "file",
    "size_in_bytes": 10484,
}
MOCK_ATTACHMENT_WITHOUT_EXTENSION = {
    "created_at": "2023-05-01T09:09:31Z",
    "id": "123",
    "_timestamp": "2023-05-01T09:10:21Z",
    "name": "Document",
    "type": "file",
    "size_in_bytes": 10484,
}

MOCK_ATTACHMENT_WITH_LARGE_DATA = {
    "created_at": "2023-05-01T09:09:31Z",
    "id": "123",
    "_timestamp": "2023-05-01T09:10:21Z",
    "name": "Document.docx",
    "type": "file",
    "size_in_bytes": 23000000,
}

MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION = {
    "created_at": "2023-05-01T09:09:31Z",
    "id": "123",
    "_timestamp": "2023-05-01T09:10:21Z",
    "name": "Document.xyz",
    "type": "file",
    "size_in_bytes": 10484,
}
MOCK_ATTACHMENT_WITH_ZERO_SIZE = {
    "created_at": "2023-05-01T09:09:31Z",
    "id": "123",
    "_timestamp": "2023-05-01T09:10:21Z",
    "name": "Document.xyz",
    "type": "file",
    "size_in_bytes": 0,
}
DOWNLOAD_URL = "https://attachment.com"
CHANNEL_MESSAGE = {
    "id": "1691588610121",
    "messageType": "message",
    "createdDateTime": "2023-08-09T12:03:30.121Z",
    "lastModifiedDateTime": "2023-08-09T12:03:30.121Z",
    "deletedDateTime": None,
    "subject": None,
    "summary": None,
    "eventDetail": None,
    "webUrl": "https://3hm-my.sharepoint.com/11_dummy",
    "from": {
        "user": {
            "@odata.type": "#microsoft.graph.teamworkUserIdentity",
            "id": "82e32462",
            "displayName": "Dummy",
            "userIdentityType": "aadUser",
            "tenantId": "a57a7700",
        }
    },
    "body": {
        "contentType": "html",
        "content": "<div>hello</div>",
    },
    "attachments": [],
    "replies": [
        {
            "id": "1691588680121",
            "messageType": "message",
            "createdDateTime": "2023-08-09T12:03:30.121Z",
            "lastModifiedDateTime": "2023-08-09T12:03:30.121Z",
            "deletedDateTime": None,
            "subject": None,
            "summary": None,
            "eventDetail": None,
            "webUrl": "https://3hm-my.sharepoint.com/11_dummy",
            "from": {
                "user": {
                    "@odata.type": "#microsoft.graph.teamworkUserIdentity",
                    "id": "82e32462",
                    "displayName": "Dummy",
                    "userIdentityType": "aadUser",
                    "tenantId": "a57a7700",
                }
            },
            "body": {
                "contentType": "html",
                "content": "<div>hello</div>",
            },
            "attachments": [],
        }
    ],
}
FILESFOLDER = {
    "id": "123",
    "name": "root",
    "size": 351660,
    "parentReference": {
        "driveId": "b!NJX42spQh0CsSMeITDaWt",
        "driveType": "documentLibrary",
    },
}


class StubAPIToken:
    async def get_with_username_password(self):
        return "something"


@pytest_asyncio.fixture
async def microsoft_client():
    yield MicrosoftTeamsClient(None, None, None, None, None)


class ClientErrorException:
    real_url = ""


@pytest_asyncio.fixture
async def patch_scroll():
    with patch.object(
        MicrosoftTeamsClient, "scroll", return_value=AsyncMock()
    ) as scroll:
        yield scroll


@pytest_asyncio.fixture
async def client():
    client = MicrosoftTeamsClient(
        "tenant_id", "client_id", "client_secret", "username", "password"
    )

    yield client


class ClientSession:
    """Mock Client Session Class"""

    async def close(self):
        """Close method of Mock Client Session Class"""
        pass


async def create_fake_coroutine(item):
    """create a method for returning fake coroutine value for
    Args:
        item: Value for converting into coroutine
    """
    return item


def test_get_configuration():
    config = DataSourceConfiguration(
        config=MicrosoftTeamsDataSource.get_default_configuration()
    )

    assert config["client_id"] == ""


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extras",
    [
        (
            {
                "client_id": "",
                "client_secret": "",
                "tenant_id": "",
            }
        ),
    ],
)
async def test_validate_configuration_with_invalid_fields_raises_error(
    extras,
):
    async with create_source(MicrosoftTeamsDataSource, **extras) as source:
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    async with create_source(MicrosoftTeamsDataSource) as source:
        DUMMY_RESPONSE = {}
        source.client.fetch = Mock(
            return_value=create_fake_coroutine(item=DUMMY_RESPONSE)
        )
        await source.ping()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_ping_for_failed_connection_exception(mock_get):
    async with create_source(MicrosoftTeamsDataSource) as source:
        with patch.object(
            MicrosoftTeamsClient,
            "fetch",
            side_effect=Exception("Something went wrong"),
        ):
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
async def test_get_docs_for_user_chats():
    async with create_source(MicrosoftTeamsDataSource) as source:
        source.client.get_user_chats = Mock(return_value=AsyncIterator([USER_CHATS]))
        source.client.get_user_chat_messages = Mock(
            return_value=AsyncIterator([MESSAGES])
        )
        source.client.get_user_chat_attachments = Mock(
            return_value=AsyncIterator([ATTACHMENTS])
        )
        source.client.get_user_chat_tabs = Mock(return_value=AsyncIterator([TABS]))
        source.client.users = Mock(return_value=AsyncIterator([]))
        source.client.get_teams = Mock(return_value=AsyncIterator([]))
        async for item, _ in source.get_docs():
            assert item["_id"] in [
                "1691582610121",
                "7ed8a6cb",
                "400960d1",
                "01EL4RL6L43X64K4ATD5H34UUQPOMMTRQX",
            ]


@pytest.mark.asyncio
async def test_get_docs_for_events():
    async with create_source(MicrosoftTeamsDataSource) as source:
        source.client.get_user_chats = Mock(return_value=AsyncIterator([]))
        source.client.get_teams = Mock(return_value=AsyncIterator([]))
        source.client.users = Mock(return_value=AsyncIterator([USERS]))
        source.client.get_calendars = Mock(return_value=AsyncIterator(EVENTS))

        async for item, _ in source.get_docs():
            assert item["_id"] in [
                "AAMkADkyYTNmOTcw",
                "AAMkADkyYTNmOTcw1",
                "AAMkADkyYTNmOTcw2",
            ]
            assert item["title"] == "Transitive connector meeting"


@pytest.mark.asyncio
async def test_get_docs_for_teams():
    async with create_source(MicrosoftTeamsDataSource) as source:
        source.client.get_user_chats = Mock(return_value=AsyncIterator([]))
        source.client.users = Mock(return_value=AsyncIterator([]))
        source.client.get_teams = Mock(return_value=AsyncIterator([TEAMS]))
        source.client.get_team_channels = Mock(return_value=AsyncIterator([CHANNELS]))
        source.client.get_channel_tabs = Mock(return_value=AsyncIterator([TABS]))
        source.client.get_channel_messages = Mock(
            return_value=AsyncIterator([MESSAGES])
        )
        source.client.get_channel_file = Mock(
            return_value=create_fake_coroutine(item=FILESFOLDER)
        )
        source.client.get_channel_drive_parent_children = Mock(
            return_value=AsyncIterator(ATTACHMENTS)
        )
        source.client.get_channel_drive_childrens = Mock(
            return_value=AsyncIterator(ATTACHMENTS)
        )

        async for item, _ in source.get_docs():
            assert item["_id"] in [
                "400960d1",
                "7ed8a6cb",
                "1691565463406",
                "19:36b3f1125",
                "25ab782d",
                "1691582610121",
                "1691565463404",
                "01EL4RL6L43X64K4ATD5H34UUQPOMMTRQX",
            ]
            if item.get("type") == "Channel Message":
                assert item["attached_documents"] == "~`!@$^8()_+-=[]{};',k.txt"


@pytest.mark.asyncio
async def test_get_content():
    message = b"This is content of attachment"

    async def download_func(url, async_buffer):
        await async_buffer.write(message)

    async with create_source(MicrosoftTeamsDataSource) as source:
        source.client.download_item = download_func
        download_result = await source.get_content(
            MOCK_ATTACHMENT, DOWNLOAD_URL, doit=True
        )
        assert download_result["_attachment"] == base64.b64encode(message).decode()


@pytest.mark.parametrize(
    "attachment, download_url",
    [
        (MOCK_ATTACHMENT_WITHOUT_EXTENSION, DOWNLOAD_URL),
        (MOCK_ATTACHMENT_WITH_LARGE_DATA, DOWNLOAD_URL),
        (MOCK_ATTACHMENT_WITH_UNSUPPORTED_EXTENSION, DOWNLOAD_URL),
        (MOCK_ATTACHMENT_WITH_ZERO_SIZE, DOWNLOAD_URL),
    ],
)
@pytest.mark.asyncio
async def test_get_content_negative(attachment, download_url):
    message = b"This is content of attachment"

    async def download_func(url, async_buffer):
        await async_buffer.write(message)

    async with create_source(MicrosoftTeamsDataSource) as source:
        source.client.download_item = download_func
        download_result = await source.get_content(attachment, download_url, doit=True)
        assert download_result is None


@pytest.mark.asyncio
async def test_get_channel_file():
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source.client.fetch = Mock(return_value=create_fake_coroutine(item=FILESFOLDER))
        returned_items = await source.client.get_channel_file(
            team_id="team_id", channel_id="channel_id"
        )

        assert returned_items["id"] == "123"


@pytest.mark.asyncio
async def test_get_channel_messages(patch_scroll, client):
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source.client.scroll = AsyncIterator([MESSAGES])
        async for messages in source.client.get_channel_messages(
            "team_id", "channel_id"
        ):
            for message in messages:
                assert message["id"] in ["1691582610121", "1691565463404"]


@pytest.mark.asyncio
async def test_format_user_chat_messages(patch_scroll, client):
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        message = await source.formatter.format_user_chat_messages(
            chat=USER_CHATS[0], message={}, message_content="hello", members="dummy"
        )
        assert message["title"] == "topic1"


@pytest.mark.asyncio
async def test_format_user_chat_messages_for_members(patch_scroll, client):
    user_chat_request = {
        "id": "19:2ea91886",
        "topic": None,
        "createdDateTime": "2023-08-03T12:19:27.57Z",
        "lastUpdatedDateTime": "2023-08-09T07:17:26.482Z",
        "chatType": "oneOnOne",
        "webUrl": "https://teams.microsoft.com/l/chat/19:2ea91886",
        "tenantId": "a57a7700",
        "members": [
            {
                "id": "123-3=",
                "displayName": "Duumy3",
                "userId": "82e32463",
                "email": "dummy3@3hm.onmicrosoft.com",
            },
            {
                "id": "123-2=",
                "displayName": "Dummy2",
                "userId": "2ea91886",
                "email": "dummy2@3hm.onmicrosoft.com",
            },
        ],
    }
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        message = await source.formatter.format_user_chat_messages(
            chat=user_chat_request,
            message={},
            message_content="hello",
            members="Duumy3,Dummy2",
        )
        assert message["title"] == "Duumy3,Dummy2"


@pytest.mark.asyncio
async def test_get_channel_messages_for_base_class(patch_scroll, client):
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        await source.get_channel_messages(
            message=CHANNEL_MESSAGE, channel_name="channel_name"
        )


@pytest.mark.asyncio
async def test_get_messages(patch_scroll, client):
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        await source.get_messages(
            message=CHANNEL_MESSAGE,
            document_type="User Chat Messages",
            chat=USER_CHATS[0],
            channel_name="channel_name",
        )


@pytest.mark.asyncio
async def test_get_channel_tabs():
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source.client.scroll = AsyncIterator([TABS])
        async for tabs in source.client.get_channel_tabs("team_id", "channel_id"):
            for tab in tabs:
                assert tab["id"] == "400960d1"


@pytest.mark.asyncio
async def test_get_team_channels():
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source.client.scroll = AsyncIterator([CHANNELS])
        async for channels in source.client.get_team_channels("team_id"):
            for channel in channels:
                assert channel["id"] == "19:36b3f1125"


@pytest.mark.asyncio
async def test_get_teams():
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source.client.scroll = AsyncIterator([TEAMS])
        async for teams in source.client.get_teams():
            for team in teams:
                assert team["id"] == "25ab782d"


@pytest.mark.asyncio
async def test_get_calendars():
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source.client.scroll = AsyncIterator([[EVENTS]])
        async for events in source.client.get_calendars("user_id"):
            for event in events:
                assert event["id"] in [
                    "AAMkADkyYTNmOTcw",
                    "AAMkADkyYTNmOTcw1",
                    "AAMkADkyYTNmOTcw2",
                ]


@pytest.mark.asyncio
async def test_get_user_chat_tabs():
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source.client.scroll = AsyncIterator([TABS])
        async for tabs in source.client.get_user_chat_tabs("chat_id"):
            for tab in tabs:
                assert tab["id"] == "400960d1"


@pytest.mark.asyncio
async def test_get_user_chat_messages():
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source.client.scroll = AsyncIterator([MESSAGES])
        async for message in source.client.get_user_chat_messages("chat_id"):
            assert len(message) == len(MESSAGES)


@pytest.mark.asyncio
async def test_get_user_chats():
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source.client.scroll = AsyncIterator([USER_CHATS])
        async for chats in source.client.get_user_chats():
            assert len(chats) == len(USER_CHATS)


@pytest.mark.asyncio
async def test_users():
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source.client.scroll = AsyncIterator([USERS])
        async for users in source.client.users():
            assert len(users) == len(USERS)


@pytest.mark.asyncio
async def test_set_internal_logger():
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source._set_internal_logger()
        assert source.client._logger == logger


@pytest.mark.asyncio
async def test_scroll(microsoft_client, mock_responses):
    url = "http://localhost:1234/url"
    first_page = ["1", "2", "3"]

    next_url = "http://localhost:1234/url/page-two"
    second_page = ["4", "5", "6"]

    first_payload = {"value": first_page, "@odata.nextLink": next_url}
    second_payload = {"value": second_page}

    mock_responses.get(url, payload=first_payload)
    mock_responses.get(next_url, payload=second_payload)

    pages = []
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        async for page in microsoft_client.scroll(url):
            pages.append(page)

    assert first_page in pages
    assert second_page in pages


@pytest.mark.asyncio
async def test_pipe(microsoft_client, mock_responses):
    class AsyncStream:
        def __init__(self):
            self.stream = BytesIO()

        async def write(self, data):
            self.stream.write(data)

        def read(self):
            return self.stream.getvalue().decode()

    url = "http://localhost:1234/download-some-sample-file"
    file_content = "hello world, this is content of downloaded file"
    stream = AsyncStream()
    mock_responses.get(url, body=file_content)
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        await microsoft_client.pipe(url, stream)

    assert stream.read() == file_content


@pytest.mark.asyncio
async def test_call_api_with_403(
    microsoft_client,
    mock_responses,
):
    url = "http://localhost:1234/download-some-sample-file"

    unauthorized_error = ClientResponseError(None, None)
    unauthorized_error.status = 403
    unauthorized_error.message = "Something went wrong"

    mock_responses.get(url, exception=unauthorized_error)
    mock_responses.get(url, exception=unauthorized_error)
    mock_responses.get(url, exception=unauthorized_error)
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        with pytest.raises(PermissionsMissing) as e:
            async for _ in microsoft_client._get(url):
                pass

        assert e is not None


@pytest.mark.asyncio
async def test_call_api_with_404(
    microsoft_client,
    mock_responses,
):
    url = "http://localhost:1234/download-some-sample-file"

    not_found_error = ClientResponseError(None, None)
    not_found_error.status = 404
    not_found_error.message = "Something went wrong"

    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        with pytest.raises(NotFound) as e:
            async for _ in microsoft_client._get(url):
                pass

        assert e is not None


@pytest.mark.asyncio
async def test_call_api_with_os_error(
    microsoft_client,
    mock_responses,
    patch_sleep,
):
    url = "http://localhost:1234/download-some-sample-file"

    not_found_error = ClientOSError()
    not_found_error.message = "Something went wrong"

    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        with pytest.raises(ClientOSError) as e:
            async for _ in microsoft_client._get(url):
                pass

        assert e is not None


@pytest.mark.asyncio
async def test_call_api_with_500(
    microsoft_client,
    mock_responses,
    patch_sleep,
):
    url = "http://localhost:1234/download-some-sample-file"

    not_found_error = ClientResponseError(
        history="history", request_info=ClientErrorException
    )
    not_found_error.status = 500
    not_found_error.message = "Something went wrong"

    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        with pytest.raises(InternalServerError) as e:
            async for _ in microsoft_client._get(url):
                pass

        assert e is not None


class JSONAsyncMock(AsyncMock):
    def __init__(self, json, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._json = json

    async def json(self):
        return self._json


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_call_api_with_429(
    microsoft_client,
    mock_responses,
):
    initial_response = ClientResponseError(None, None)
    initial_response.status = 429
    initial_response.message = "rate-limited"
    url = "http://localhost:1234/download-some-sample-file"

    retried_response = AsyncMock()
    payload = {"value": "Test rate limit"}

    retried_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(payload))
    with patch("connectors.sources.microsoft_teams.RETRY_SECONDS", 0.3):
        with patch.object(
            GraphAPIToken, "get_with_username_password", return_value="abc"
        ):
            with patch(
                "aiohttp.ClientSession.get",
                side_effect=[initial_response, retried_response],
            ):
                async for response in microsoft_client._get(url):
                    actual_payload = await response.json()
                    assert actual_payload == payload


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_call_api_with_429_with_retry_after(
    microsoft_client,
    mock_responses,
):
    initial_response = ClientResponseError(None, None)
    initial_response.status = 429
    initial_response.message = "rate-limited"
    initial_response.headers = {"Retry-After": "0"}
    url = "http://localhost:1234/download-some-sample-file"

    retried_response = AsyncMock()
    payload = {"value": "Test rate limit"}

    retried_response.__aenter__ = AsyncMock(return_value=JSONAsyncMock(payload))
    with patch("connectors.sources.microsoft_teams.RETRY_SECONDS", 0.3):
        with patch.object(
            GraphAPIToken, "get_with_username_password", return_value="abc"
        ):
            with patch(
                "aiohttp.ClientSession.get",
                side_effect=[initial_response, retried_response],
            ):
                async for response in microsoft_client._get(url):
                    actual_payload = await response.json()
                    assert actual_payload == payload


@pytest.mark.asyncio
async def test_call_api_with_unhandled_status(
    microsoft_client, mock_responses, patch_sleep
):
    url = "http://localhost:1234/download-some-sample-file"

    error_message = "Something went wrong"

    not_found_error = ClientResponseError(MagicMock(), MagicMock())
    not_found_error.status = 420
    not_found_error.message = error_message

    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    mock_responses.get(url, exception=not_found_error)
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        with pytest.raises(ClientResponseError) as e:
            async for _ in microsoft_client._get(url):
                pass

        assert e.match(error_message)


@pytest.mark.asyncio
async def test_get_for_client():
    bearer = "hello"
    source = GraphAPIToken(
        "tenant_id", "client_id", "client_secret", "username", "password"
    )
    source._fetch_token = Mock(return_value=create_fake_coroutine(("hello", 15)))
    actual_token = await source.get_with_client()

    assert actual_token == bearer


@pytest.mark.asyncio
async def test_get_for_username_password():
    bearer = "hello"
    source = GraphAPIToken(
        "tenant_id", "client_id", "client_secret", "username", "password"
    )
    source._fetch_token = Mock(return_value=create_fake_coroutine(("hello", 15)))
    actual_token = await source.get_with_username_password()

    assert actual_token == bearer


@pytest.mark.asyncio
async def test_fetch(microsoft_client, mock_responses):
    with patch.object(
        GraphAPIToken,
        "_fetch_token",
        return_value=await create_fake_coroutine(("hello", 15)),
    ):
        url = "http://localhost:1234/url"

        response = {"displayName": "Dummy", "id": "123"}

        mock_responses.get(url, payload=response)

        fetch_response = await microsoft_client.fetch(url)
        assert response == fetch_response


@pytest.mark.asyncio
async def test_get_user_drive_root_children():
    async with create_source(
        MicrosoftTeamsDataSource,
    ) as source:
        source.client.scroll = AsyncIterator([[{"name": "microsoft teams chat files"}]])
        child = await source.client.get_user_drive_root_children(drive_id=1)
        assert child["name"] == "microsoft teams chat files"
