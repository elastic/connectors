#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Zoom source class methods"""
from contextlib import asynccontextmanager
from unittest import mock
from unittest.mock import Mock, patch

import aiohttp
import pytest
from aiohttp.client_exceptions import ClientResponseError
from freezegun import freeze_time

from connectors.source import ConfigurableFieldValueError
from connectors.sources.zoom import TokenError, ZoomDataSource
from tests.sources.support import create_source

# Access token document
SAMPLE_ACCESS_TOKEN_RESPONSE = {"access_token": "token#123", "expires_in": 3599}

# User document
SAMPLE_USER_PAGE1 = {
    "next_page_token": "page2",
    "users": [{"id": "user1", "type": "user", "name": "admin"}],
}
SAMPLE_USER_PAGE2 = {
    "next_page_token": None,
    "users": None,
}
USER_EXPECTED_RESPONSE = [
    {
        "id": "user1",
        "type": "user",
        "name": "admin",
        "_id": "user1",
        "_timestamp": "2023-03-09T00:00:00+00:00",
    }
]

# Meeting document
SAMPLE_LIVE_MEETING = {
    "next_page_token": None,
    "meetings": [
        {
            "id": "meeting1",
            "type": "live_meeting",
            "created_at": "2023-03-09T00:00:00Z",
        },
        {
            "id": "meeting2",
            "type": "live_meeting",
            "created_at": "2023-04-09T00:00:00Z",
        },
    ],
}
SAMPLE_UPCOMING_MEETING = {
    "next_page_token": None,
    "meetings": [
        {
            "id": "meeting3",
            "type": "upcoming_meeting",
            "created_at": "2023-03-11T00:00:00Z",
        },
        {
            "id": "meeting4",
            "type": "upcoming_meeting",
            "created_at": "2023-04-11T00:00:00Z",
        },
    ],
}
SAMPLE_PREVIOUS_MEETING = {
    "next_page_token": None,
    "meetings": [
        {
            "id": "meeting5",
            "type": "previous_meeting",
            "created_at": "2023-03-13T00:00:00Z",
        },
        {
            "id": "meeting6",
            "type": "previous_meeting",
            "created_at": "2023-04-13T00:00:00Z",
        },
    ],
}

SAMPLE_PREVIOUS_MEETING_DETAIL1 = {
    "id": "meeting5",
    "type": "previous_meeting_detail",
    "created_at": "2023-03-03T00:00:00Z",
}
SAMPLE_PREVIOUS_MEETING_DETAIL2 = {
    "id": "meeting6",
    "type": "previous_meeting_detail",
    "created_at": "2023-03-03T00:00:00Z",
}
SAMPLE_PREVIOUS_MEETING_PARTICIPANTS = {
    "next_page_token": None,
    "participants": [
        {"id": "participant1", "type": "participant"},
        {"id": "participant2", "type": "participant"},
        {"id": "participant3", "type": "participant"},
    ],
}
EXPECTED_PREVIOUS_MEETING = {
    "id": "meeting5",
    "type": "previous_meeting_detail",
    "created_at": "2023-03-03T00:00:00Z",
    "participants": [
        {"id": "participant1", "type": "participant"},
        {"id": "participant2", "type": "participant"},
        {"id": "participant3", "type": "participant"},
    ],
}
MEETING_EXPECTED_RESPONSE = [
    {
        "id": "meeting1",
        "type": "live_meeting",
        "created_at": "2023-03-09T00:00:00Z",
        "_id": "meeting1",
        "_timestamp": "2023-03-09T00:00:00Z",
    },
    {
        "id": "meeting2",
        "type": "live_meeting",
        "created_at": "2023-04-09T00:00:00Z",
        "_id": "meeting2",
        "_timestamp": "2023-04-09T00:00:00Z",
    },
    {
        "id": "meeting3",
        "type": "upcoming_meeting",
        "created_at": "2023-03-11T00:00:00Z",
        "_id": "meeting3",
        "_timestamp": "2023-03-11T00:00:00Z",
    },
    {
        "id": "meeting4",
        "type": "upcoming_meeting",
        "created_at": "2023-04-11T00:00:00Z",
        "_id": "meeting4",
        "_timestamp": "2023-04-11T00:00:00Z",
    },
    {
        "id": "meeting5",
        "type": "previous_meeting_detail",
        "created_at": "2023-03-03T00:00:00Z",
        "participants": [
            {"id": "participant1", "type": "participant"},
            {"id": "participant2", "type": "participant"},
            {"id": "participant3", "type": "participant"},
        ],
        "_id": "meeting5",
        "_timestamp": None,
    },
    {
        "id": "meeting6",
        "type": "previous_meeting_detail",
        "created_at": "2023-03-03T00:00:00Z",
        "participants": [
            {"id": "participant1", "type": "participant"},
            {"id": "participant2", "type": "participant"},
            {"id": "participant3", "type": "participant"},
        ],
        "_id": "meeting6",
        "_timestamp": None,
    },
]

# Recording document
SAMPLE_RECORDING_PAGE1 = {
    "next_page_token": None,
    "meetings": [
        {"id": "recording1", "type": "recording", "start_time": "2023-03-01T00:00:00Z"}
    ],
}
SAMPLE_RECORDING_PAGE2 = {
    "next_page_token": None,
    "meetings": [
        {"id": "recording2", "type": "recording", "start_time": "2023-02-01T00:00:00Z"}
    ],
}
SAMPLE_RECORDING_PAGE3 = {
    "next_page_token": None,
    "meetings": [
        {"id": "recording3", "type": "recording", "start_time": "2023-01-01T00:00:00Z"}
    ],
}
SAMPLE_RECORDING_PAGE4 = {
    "next_page_token": None,
    "meetings": [
        {"id": "recording4", "type": "recording", "start_time": "2023-12-01T00:00:00Z"}
    ],
}
RECORDING_EXPECTED_RESPONSE = [
    {
        "id": "recording1",
        "type": "recording",
        "start_time": "2023-03-01T00:00:00Z",
        "_id": "recording1",
        "_timestamp": "2023-03-01T00:00:00Z",
    },
    {
        "id": "recording2",
        "type": "recording",
        "start_time": "2023-02-01T00:00:00Z",
        "_id": "recording2",
        "_timestamp": "2023-02-01T00:00:00Z",
    },
    {
        "id": "recording3",
        "type": "recording",
        "start_time": "2023-01-01T00:00:00Z",
        "_id": "recording3",
        "_timestamp": "2023-01-01T00:00:00Z",
    },
    {
        "id": "recording4",
        "type": "recording",
        "start_time": "2023-12-01T00:00:00Z",
        "_id": "recording4",
        "_timestamp": "2023-12-01T00:00:00Z",
    },
]

# Channel document
SAMPLE_CHANNEL = {
    "next_page_token": None,
    "channels": [
        {"id": "channel1", "type": "chat", "date_time": "2023-03-09T00:00:00Z"},
        {"id": "channel2", "type": "channel", "date_time": "2023-02-09T00:00:00Z"},
    ],
}
CHANNEL_EXPECTED_RESPONSE = [
    {
        "id": "channel1",
        "type": "chat",
        "date_time": "2023-03-09T00:00:00Z",
        "_id": "channel1",
        "_timestamp": "2023-03-09T00:00:00+00:00",
    },
    {
        "id": "channel2",
        "type": "channel",
        "date_time": "2023-02-09T00:00:00Z",
        "_id": "channel2",
        "_timestamp": "2023-03-09T00:00:00+00:00",
    },
]

# Chat document
SAMPLE_CHAT = {
    "next_page_token": None,
    "messages": [
        {"id": "chat1", "type": "chat", "date_time": "2023-03-09T00:00:00Z"},
        {"id": "chat2", "type": "chat", "date_time": "2023-02-09T00:00:00Z"},
    ],
}
CHAT_EXPECTED_RESPONSE = [
    {
        "id": "chat1",
        "type": "chat",
        "date_time": "2023-03-09T00:00:00Z",
        "_id": "chat1",
        "_timestamp": "2023-03-09T00:00:00Z",
    },
    {
        "id": "chat2",
        "type": "chat",
        "date_time": "2023-02-09T00:00:00Z",
        "_id": "chat2",
        "_timestamp": "2023-02-09T00:00:00Z",
    },
]

# File document
SAMPLE_CONTENT = "Content"
FILE = {
    "id": "file1",
    "file_id": "file1",
    "type": "file",
    "date_time": "2023-03-09T00:00:00Z",
    "file_size": 100,
    "file_name": "file1.txt",
    "download_url": "https://api.zoom.us/v2/download_url",
}
FILE_EXPECTED_CONTENT = {
    "_id": "file1",
    "_timestamp": "2023-03-09T00:00:00Z",
    "_attachment": "Q29udGVudA==",
}
FILE_EXPECTED_CONTENT_EXTRACTED = {
    "_id": "file1",
    "_timestamp": "2023-03-09T00:00:00Z",
    "body": SAMPLE_CONTENT,
}
FILE_EXPECTED_RESPONSE = {
    "_id": "file1",
    "_timestamp": "2023-03-09T00:00:00Z",
    "file_id": "file1",
    "type": "file",
    "date_time": "2023-03-09T00:00:00Z",
    "file_size": 100,
    "file_name": "file1.txt",
    "download_url": "https://api.zoom.us/v2/download_url",
}
FILE_WITHOUT_EXTENSION = {
    "file_id": "file2",
    "type": "file",
    "date_time": "2023-02-09T00:00:00Z",
    "file_size": 300,
    "file_name": "file4",
    "download_url": "https://api.zoom.us/v2/download_url",
}
FILE_WITH_LARGE_DATA = {
    "file_id": "file3",
    "type": "file",
    "date_time": "2023-02-09T00:00:00Z",
    "file_size": 10485761,
    "file_name": "file5.txt",
    "download_url": "https://api.zoom.us/v2/download_url",
}
FILE_WITH_UNSUPPORTED_EXTENSION = {
    "file_id": "file4",
    "type": "file",
    "date_time": "2023-02-09T00:00:00Z",
    "file_size": 300,
    "file_name": "file3.png",
    "download_url": "https://api.zoom.us/v2/download_url",
}
SAMPLE_FILE = {
    "next_page_token": None,
    "messages": [
        FILE,
        FILE_WITHOUT_EXTENSION,
        FILE_WITH_LARGE_DATA,
        FILE_WITH_UNSUPPORTED_EXTENSION,
    ],
}
EXPECTED_FILES_RESPONSE = [
    {
        "file_id": "file1",
        "type": "file",
        "date_time": "2023-03-09T00:00:00Z",
        "file_size": 100,
        "file_name": "file1.txt",
        "download_url": "https://api.zoom.us/v2/download_url",
        "id": "file1",
        "_id": "file1",
        "_timestamp": "2023-03-09T00:00:00Z",
    },
    {
        "file_id": "file2",
        "type": "file",
        "date_time": "2023-02-09T00:00:00Z",
        "file_size": 300,
        "file_name": "file4",
        "download_url": "https://api.zoom.us/v2/download_url",
        "id": "file2",
        "_id": "file2",
        "_timestamp": "2023-02-09T00:00:00Z",
    },
    {
        "file_id": "file3",
        "type": "file",
        "date_time": "2023-02-09T00:00:00Z",
        "file_size": 10485761,
        "file_name": "file5.txt",
        "download_url": "https://api.zoom.us/v2/download_url",
        "id": "file3",
        "_id": "file3",
        "_timestamp": "2023-02-09T00:00:00Z",
    },
    {
        "file_id": "file4",
        "type": "file",
        "date_time": "2023-02-09T00:00:00Z",
        "file_size": 300,
        "file_name": "file3.png",
        "download_url": "https://api.zoom.us/v2/download_url",
        "id": "file4",
        "_id": "file4",
        "_timestamp": "2023-02-09T00:00:00Z",
    },
]


class ZoomAsyncMock(mock.AsyncMock):
    def __init__(self, data, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._data = data

    async def json(self):
        return self._data

    async def text(self):
        return self._data


def get_mock(mock_response):
    async_mock = mock.AsyncMock()
    async_mock.__aenter__ = mock.AsyncMock(
        return_value=ZoomAsyncMock(data=mock_response)
    )
    return async_mock


@asynccontextmanager
async def create_zoom_source(
    fetch_past_meeting_details=False, use_text_extraction_service=False
):
    async with create_source(
        ZoomDataSource,
        account_id="123",
        client_id="id@123",
        client_secret="secret#123",
        recording_age=4,
        fetch_past_meeting_details=fetch_past_meeting_details,
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source


def mock_zoom_apis(url, headers):
    # Users APIS
    if url == "https://api.zoom.us/v2/users?page_size=300":
        return get_mock(mock_response=SAMPLE_USER_PAGE1)
    elif url == "https://api.zoom.us/v2/users?page_size=300&next_page_token=page2":
        return get_mock(mock_response=SAMPLE_USER_PAGE2)

    # Meeting APIS
    elif url == "https://api.zoom.us/v2/users/user1/meetings?page_size=300&type=live":
        return get_mock(mock_response=SAMPLE_LIVE_MEETING)
    elif (
        url
        == "https://api.zoom.us/v2/users/user1/meetings?page_size=300&type=upcoming_meetings"
    ):
        return get_mock(mock_response=SAMPLE_UPCOMING_MEETING)
    elif (
        url
        == "https://api.zoom.us/v2/users/user1/meetings?page_size=300&type=previous_meetings"
    ):
        return get_mock(mock_response=SAMPLE_PREVIOUS_MEETING)

    # Previous Meeting APIS
    elif url == "https://api.zoom.us/v2/past_meetings/meeting5":
        return get_mock(mock_response=SAMPLE_PREVIOUS_MEETING_DETAIL1)
    elif url == "https://api.zoom.us/v2/past_meetings/meeting6":
        return get_mock(mock_response=SAMPLE_PREVIOUS_MEETING_DETAIL2)

    # Participants APIS
    elif (
        url
        == "https://api.zoom.us/v2/past_meetings/meeting5/participants?page_size=300"
    ):
        return get_mock(mock_response=SAMPLE_PREVIOUS_MEETING_PARTICIPANTS)
    elif (
        url
        == "https://api.zoom.us/v2/past_meetings/meeting6/participants?page_size=300"
    ):
        return get_mock(mock_response=SAMPLE_PREVIOUS_MEETING_PARTICIPANTS)

    # Recording APIS
    elif (
        url
        == "https://api.zoom.us/v2/users/user1/recordings?page_size=300&from=2023-02-07&to=2023-03-09"
    ):
        return get_mock(mock_response=SAMPLE_RECORDING_PAGE1)
    elif (
        url
        == "https://api.zoom.us/v2/users/user1/recordings?page_size=300&from=2023-01-08&to=2023-02-07"
    ):
        return get_mock(mock_response=SAMPLE_RECORDING_PAGE2)
    elif (
        url
        == "https://api.zoom.us/v2/users/user1/recordings?page_size=300&from=2022-12-09&to=2023-01-08"
    ):
        return get_mock(mock_response=SAMPLE_RECORDING_PAGE3)
    elif (
        url
        == "https://api.zoom.us/v2/users/user1/recordings?page_size=300&from=2022-11-09&to=2022-12-09"
    ):
        return get_mock(mock_response=SAMPLE_RECORDING_PAGE4)

    # Channel APIS
    elif url == "https://api.zoom.us/v2/chat/users/user1/channels?page_size=50":
        return get_mock(mock_response=SAMPLE_CHANNEL)

    # Chat APIS
    elif (
        url
        == "https://api.zoom.us/v2/chat/users/user1/messages?page_size=50&search_key=%20&search_type=message&from=2022-09-10T00:00:00Z&to=2023-03-09T00:00:00Z"
    ):
        return get_mock(mock_response=SAMPLE_CHAT)
    elif (
        url
        == "https://api.zoom.us/v2/chat/users/user1/messages?page_size=50&search_key=%20&search_type=file&from=2022-09-10T00:00:00Z&to=2023-03-09T00:00:00Z"
    ):
        return get_mock(mock_response=SAMPLE_FILE)

    # Content APIS
    elif url == "https://api.zoom.us/v2/download_url":
        return get_mock(mock_response=SAMPLE_CONTENT)

    # Emtpy Response
    else:
        return get_mock(mock_response=None)


def mock_token_response():
    return get_mock(mock_response=SAMPLE_ACCESS_TOKEN_RESPONSE)


@pytest.mark.asyncio
async def test_fetch_for_successful_call():
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=mock_token_response(),
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                side_effect=mock_zoom_apis,
            ):
                response = await source.client.api_client.fetch(
                    url="https://api.zoom.us/v2/past_meetings/meeting5"
                )
                assert response == SAMPLE_PREVIOUS_MEETING_DETAIL1


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_fetch_for_unsuccessful_call():
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=mock_token_response(),
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                side_effect=Exception("Something went wrong"),
            ):
                response = await source.client.api_client.fetch(
                    url="https://api.zoom.us/v2/past_meetings/meeting5"
                )
                assert response is None


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_fetch_for_unauthorized_error():
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=mock_token_response(),
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                side_effect=ClientResponseError(
                    status=401,
                    request_info=aiohttp.RequestInfo(
                        real_url="", method=None, headers=None, url=""
                    ),
                    history=None,
                ),
            ):
                response = await source.client.api_client.fetch(
                    url="https://api.zoom.us/v2/past_meetings/meeting5"
                )
                assert response is None


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_fetch_for_notfound_error():
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=mock_token_response(),
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                side_effect=ClientResponseError(
                    status=404,
                    request_info=aiohttp.RequestInfo(
                        real_url="", method=None, headers=None, url=""
                    ),
                    history=None,
                ),
            ):
                response = await source.client.api_client.fetch(
                    url="https://api.zoom.us/v2/past_meetings/meeting5"
                )
                assert response is None


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_fetch_for_other_client_error():
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=mock_token_response(),
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                side_effect=ClientResponseError(
                    status=405,
                    request_info=aiohttp.RequestInfo(
                        real_url="", method=None, headers=None, url=""
                    ),
                    history=None,
                ),
            ):
                response = await source.client.api_client.fetch(
                    url="https://api.zoom.us/v2/past_meetings/meeting5"
                )
                assert response is None


@pytest.mark.asyncio
async def test_content_for_successful_call():
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=mock_token_response(),
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                side_effect=mock_zoom_apis,
            ):
                response = await source.client.api_client.content(
                    url="https://api.zoom.us/v2/download_url"
                )
                assert response == SAMPLE_CONTENT


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_content_for_unsuccessful_call():
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=mock_token_response(),
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                side_effect=Exception("Something went wrong"),
            ):
                response = await source.client.api_client.content(
                    url="https://api.zoom.us/v2/download_url"
                )
                assert response is None


@pytest.mark.asyncio
async def test_scroll_for_successful_response():
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=mock_token_response(),
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                side_effect=mock_zoom_apis,
            ):
                response = []
                async for res in source.client.api_client.scroll(
                    url="https://api.zoom.us/v2/users?page_size=300"
                ):
                    response.append(res)
                assert response == [SAMPLE_USER_PAGE1, SAMPLE_USER_PAGE2]


@pytest.mark.asyncio
async def test_scroll_for_empty_response():
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=mock_token_response(),
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                side_effect=mock_zoom_apis,
            ):
                response = []
                async for res in source.client.api_client.scroll(
                    url="https://api.zoom.us/v2/users?page_size=300&next_page_token=page3"
                ):
                    response.append(res)
                assert response == []


@pytest.mark.asyncio
async def test_validate_config():
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=mock_token_response(),
        ):
            try:
                await source.validate_config()
            except ConfigurableFieldValueError:
                msg = "Should've been a valid config"
                raise AssertionError(msg) from None


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["account_id", "client_id", "client_secret"])
async def test_validate_config_missing_fields_then_raise(field):
    async with create_zoom_source() as source:
        source.configuration.get_field(field).value = ""

        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=mock_token_response(),
        ):
            try:
                await source.ping()
            except Exception as e:
                msg = "Ping should've been successful"
                raise AssertionError(msg) from e


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_ping_for_unsuccessful_connection():
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=get_mock(mock_response=TokenError),
        ):
            with pytest.raises(TokenError):
                await source.ping()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attachment, doit, expected_content",
    [
        (FILE, True, FILE_EXPECTED_CONTENT),
        (FILE, False, None),
        (FILE_WITHOUT_EXTENSION, True, None),
        (FILE_WITH_LARGE_DATA, True, None),
        (FILE_WITH_UNSUPPORTED_EXTENSION, True, None),
    ],
)
async def test_get_content(attachment, doit, expected_content):
    async with create_zoom_source() as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=mock_token_response(),
        ):
            with mock.patch(
                "aiohttp.ClientSession.get",
                side_effect=mock_zoom_apis,
            ):
                response = await source.get_content(
                    chat_file=attachment,
                    doit=doit,
                )
                assert response == expected_content


@pytest.mark.asyncio
async def test_get_content_with_extraction_service():
    with patch(
        "connectors.content_extraction.ContentExtraction.extract_text",
        return_value=SAMPLE_CONTENT,
    ), patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
    ):
        async with create_zoom_source(use_text_extraction_service=True) as source:
            with mock.patch(
                "aiohttp.ClientSession.post",
                return_value=mock_token_response(),
            ):
                with mock.patch(
                    "aiohttp.ClientSession.get",
                    side_effect=mock_zoom_apis,
                ):
                    response = await source.get_content(
                        chat_file=FILE,
                        doit=True,
                    )
                    assert response == FILE_EXPECTED_CONTENT_EXTRACTED


@pytest.mark.asyncio
@freeze_time("2023-03-09T00:00:00")
async def test_get_docs():
    document_without_attachment = []
    document_with_attachment = []
    async with create_zoom_source(fetch_past_meeting_details=True) as source:
        with mock.patch(
            "aiohttp.ClientSession.post",
            return_value=get_mock(mock_response=SAMPLE_ACCESS_TOKEN_RESPONSE),
        ):
            with mock.patch("aiohttp.ClientSession.get", side_effect=mock_zoom_apis):
                async for (doc, content) in source.get_docs():
                    document_without_attachment.append(doc)
                    if content:
                        res = await content(doit=True)
                        document_with_attachment.append(res)

    assert all(
        element in document_without_attachment for element in USER_EXPECTED_RESPONSE
    )  # Users
    assert all(
        element in document_without_attachment for element in MEETING_EXPECTED_RESPONSE
    )  # Meetings
    assert all(
        element in document_without_attachment
        for element in RECORDING_EXPECTED_RESPONSE
    )  # Recordings
    assert all(
        element in document_without_attachment for element in CHANNEL_EXPECTED_RESPONSE
    )  # Channels
    assert all(
        element in document_without_attachment for element in CHAT_EXPECTED_RESPONSE
    )  # Chats
    assert all(
        element in document_without_attachment for element in EXPECTED_FILES_RESPONSE
    )  # Files
    assert all(
        element in document_with_attachment for element in [FILE_EXPECTED_CONTENT]
    )  # Contents
