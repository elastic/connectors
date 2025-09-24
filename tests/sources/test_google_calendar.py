#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Google Calendar source class methods."""

import asyncio
from contextlib import asynccontextmanager
from unittest import mock
from unittest.mock import patch

import pytest
from aiogoogle import Aiogoogle

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.google_calendar import (
    GoogleCalendarClient,
    GoogleCalendarDataSource,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source

SERVICE_ACCOUNT_CREDENTIALS = '{"project_id": "dummy123"}'


@asynccontextmanager
async def create_gcal_source(**kwargs):
    """Create a Google Calendar source for testing"""
    async with create_source(
        GoogleCalendarDataSource,
        service_account_credentials=SERVICE_ACCOUNT_CREDENTIALS,
        subject="test@example.com",
        **kwargs,
    ) as source:
        yield source


@pytest.mark.asyncio
async def test_empty_configuration():
    """Tests the validity of the configurations passed to the Google Calendar source class."""

    configuration = DataSourceConfiguration({"service_account_credentials": ""})
    gcal_object = GoogleCalendarDataSource(configuration=configuration)

    with pytest.raises(
        ConfigurableFieldValueError,
        match="Field validation errors: 'Service_account_credentials' cannot be empty.",
    ):
        await gcal_object.validate_config()


@pytest.mark.asyncio
async def test_raise_on_invalid_configuration():
    """Test if invalid configuration raises an expected Exception"""

    configuration = DataSourceConfiguration(
        {"service_account_credentials": "{'abc':'bcd','cd'}"}
    )
    gcal_object = GoogleCalendarDataSource(configuration=configuration)

    with pytest.raises(
        ConfigurableFieldValueError,
        match="Google Calendar service account is not a valid JSON",
    ):
        await gcal_object.validate_config()


@pytest.mark.asyncio
async def test_get_default_configuration():
    """Test the default configuration for Google Calendar connector"""
    config = GoogleCalendarDataSource.get_default_configuration()

    assert "service_account_credentials" in config
    assert config["service_account_credentials"]["type"] == "str"
    assert config["service_account_credentials"]["sensitive"] is True

    assert "subject" in config
    assert config["subject"]["type"] == "str"

    assert "days_back" in config
    assert config["days_back"]["type"] == "int"
    assert config["days_back"]["value"] == 30

    assert "days_forward" in config
    assert config["days_forward"]["type"] == "int"
    assert config["days_forward"]["value"] == 30


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    """Tests the ping functionality for ensuring connection to Google Calendar."""

    expected_response = {
        "kind": "calendar#calendarList",
        "items": [],
    }
    async with create_gcal_source() as source:
        as_service_account_response = asyncio.Future()
        as_service_account_response.set_result(expected_response)

        with mock.patch.object(
            Aiogoogle, "as_service_account", return_value=as_service_account_response
        ):
            await source.ping()


@patch("connectors.utils.time_to_sleep_between_retries", mock.Mock(return_value=0))
@pytest.mark.asyncio
async def test_ping_for_failed_connection():
    """Tests the ping functionality when connection can not be established to Google Calendar."""

    async with create_gcal_source() as source:
        with mock.patch.object(
            Aiogoogle, "discover", side_effect=Exception("Something went wrong")
        ):
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
async def test_get_docs():
    """Tests the module responsible to fetch and yield documents from Google Calendar."""

    async with create_gcal_source() as source:
        # Mock responses for calendar list, calendar details, and events
        calendar_list_response = {
            "kind": "calendar#calendarList",
            "items": [
                {
                    "id": "calendar1",
                    "summary": "Calendar 1",
                    "summaryOverride": "Calendar 1 Override",
                    "colorId": "1",
                    "backgroundColor": "#ffffff",
                    "foregroundColor": "#000000",
                    "accessRole": "owner",
                    "primary": True,
                }
            ],
        }

        calendar_response = {
            "id": "calendar1",
            "summary": "Calendar 1",
            "description": "Calendar 1 Description",
            "location": "Location 1",
            "timeZone": "UTC",
        }

        events_response = {
            "kind": "calendar#events",
            "items": [
                {
                    "id": "event1",
                    "summary": "Event 1",
                    "description": "Event 1 Description",
                    "location": "Event Location",
                    "colorId": "1",
                    "start": {"dateTime": "2025-07-23T10:00:00Z"},
                    "end": {"dateTime": "2025-07-23T11:00:00Z"},
                    "created": "2025-07-20T10:00:00Z",
                    "updated": "2025-07-21T10:00:00Z",
                    "status": "confirmed",
                    "organizer": {"email": "organizer@example.com"},
                    "creator": {"email": "creator@example.com"},
                    "attendees": [
                        {"displayName": "John Doe", "email": "attendee@example.com"}
                    ],
                }
            ],
        }

        # Mock the client methods
        mock_client = mock.MagicMock()
        mock_client.list_calendar_list = AsyncIterator([calendar_list_response])
        mock_client.get_calendar = mock.AsyncMock(return_value=calendar_response)
        mock_client.list_events = AsyncIterator([events_response])

        # Mock the calendar_client method to return our mock client
        with mock.patch.object(source, "calendar_client", return_value=mock_client):
            # Collect the documents yielded by get_docs
            documents = []
            async for doc, _ in source.get_docs():
                documents.append(doc)

            # We should have 3 documents: 1 calendar list entry, 1 calendar, and 1 event
            assert len(documents) == 3

            # Verify the calendar list entry
            calendar_list_doc = next(
                (doc for doc in documents if doc["type"] == "calendar_list"), None
            )
            assert calendar_list_doc is not None
            assert calendar_list_doc["calendar_id"] == "calendar1"
            assert calendar_list_doc["summary"] == "Calendar 1"
            assert calendar_list_doc["summary_override"] == "Calendar 1 Override"

            # Verify the calendar
            calendar_doc = next(
                (doc for doc in documents if doc["type"] == "calendar"), None
            )
            assert calendar_doc is not None
            assert calendar_doc["calendar_id"] == "calendar1"
            assert calendar_doc["summary"] == "Calendar 1"
            assert calendar_doc["description"] == "Calendar 1 Description"

            # Verify the event
            event_doc = next((doc for doc in documents if doc["type"] == "event"), None)
            assert event_doc is not None
            assert event_doc["event_id"] == "event1"
            assert event_doc["summary"] == "Event 1"
            assert event_doc["description"] == "Event 1 Description"

            # Verify the event has the simplified fields
            assert "attendees" in event_doc
            assert "attachments" in event_doc
            assert "meeting_link" in event_doc
            # Check that attendees structure is simplified
            assert len(event_doc["attendees"]) == 1
            assert event_doc["attendees"][0]["name"] == "John Doe"
            assert event_doc["attendees"][0]["email"] == "attendee@example.com"
            # Check that unnecessary fields are removed
            assert "color_id" not in event_doc
            assert "transparency" not in event_doc
            assert "visibility" not in event_doc
            assert "conference_data" not in event_doc


@pytest.mark.asyncio
async def test_get_docs_with_time_range():
    """Tests the get_docs method with time range configuration."""

    async with create_gcal_source(days_back=7, days_forward=14) as source:
        # Mock responses for calendar list, calendar details, and events
        calendar_list_response = {
            "kind": "calendar#calendarList",
            "items": [{"id": "calendar1", "summary": "Calendar 1"}],
        }

        calendar_response = {"id": "calendar1", "summary": "Calendar 1"}

        events_response = {"kind": "calendar#events", "items": []}

        # Mock the client methods
        mock_client = mock.MagicMock()
        mock_client.list_calendar_list = AsyncIterator([calendar_list_response])
        mock_client.get_calendar = mock.AsyncMock(return_value=calendar_response)
        mock_client.list_events = AsyncIterator([events_response])

        # Mock the calendar_client method to return our mock client
        with mock.patch.object(source, "calendar_client", return_value=mock_client):
            # Collect the documents yielded by get_docs
            documents = []
            async for doc, _ in source.get_docs():
                documents.append(doc)

            # We should have 2 documents: 1 calendar list entry and 1 calendar
            assert len(documents) == 2

            # Verify the calendar list entry
            calendar_list_doc = next(
                (doc for doc in documents if doc["type"] == "calendar_list"), None
            )
            assert calendar_list_doc is not None
            assert calendar_list_doc["calendar_id"] == "calendar1"
            assert calendar_list_doc["summary"] == "Calendar 1"

            # Verify the calendar
            calendar_doc = next(
                (doc for doc in documents if doc["type"] == "calendar"), None
            )
            assert calendar_doc is not None
            assert calendar_doc["calendar_id"] == "calendar1"
            assert calendar_doc["summary"] == "Calendar 1"

            # Verify that list_events was called with time parameters
            mock_client.list_events.assert_called_once()
            call_args = mock_client.list_events.call_args
            assert len(call_args[0]) == 3  # calendar_id, time_min, time_max
            assert call_args[0][0] == "calendar1"
            # Verify time_min and time_max are provided (exact values depend on when test runs)
            assert call_args[0][1] is not None  # time_min
            assert call_args[0][2] is not None  # time_max


@pytest.mark.asyncio
async def test_client_methods():
    """Test the GoogleCalendarClient methods."""

    client = GoogleCalendarClient(
        json_credentials={"project_id": "dummy123"}, subject="test@example.com"
    )

    # Mock the api_call and api_call_paged methods
    with mock.patch.object(
        client, "api_call", mock.AsyncMock(return_value={"id": "calendar1"})
    ):
        with mock.patch.object(
            client,
            "api_call_paged",
            side_effect=lambda *args, **kwargs: AsyncIterator([{"items": []}]),
        ):
            # Test ping
            result = await client.ping()
            assert result == {"id": "calendar1"}
            client.api_call.assert_called_once_with(
                resource="calendarList", method="list", maxResults=1
            )

            # Reset mock
            client.api_call.reset_mock()

            # Test get_calendar
            result = await client.get_calendar("calendar1")
            assert result == {"id": "calendar1"}
            client.api_call.assert_called_once_with(
                resource="calendars", method="get", calendarId="calendar1"
            )

            # Test list_calendar_list
            async for page in client.list_calendar_list():
                assert page == {"items": []}
            client.api_call_paged.assert_called_once_with(
                resource="calendarList", method="list", maxResults=100
            )

            # Reset mock
            client.api_call_paged.reset_mock()

            # Test list_events with time range
            async for page in client.list_events(
                "calendar1", "2025-07-23T00:00:00Z", "2025-07-30T23:59:59Z"
            ):
                assert page == {"items": []}
            client.api_call_paged.assert_called_once_with(
                resource="events",
                method="list",
                calendarId="calendar1",
                maxResults=100,
                timeMin="2025-07-23T00:00:00Z",
                timeMax="2025-07-30T23:59:59Z",
            )

            # Reset mock
            client.api_call.reset_mock()


@pytest.mark.asyncio
async def test_close():
    """Test the close method"""
    async with create_gcal_source() as source:
        # close should not raise an exception
        await source.close()
