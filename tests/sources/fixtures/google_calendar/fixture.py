#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
"""Module to handle API calls received from Google Calendar connector."""

import os
import time
from datetime import datetime, timedelta

from flask import Flask, request

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

match DATA_SIZE:
    case "small":
        CALENDARS_COUNT = 5
        EVENTS_PER_CALENDAR = 50
    case "medium":
        CALENDARS_COUNT = 10
        EVENTS_PER_CALENDAR = 100
    case "large":
        CALENDARS_COUNT = 20
        EVENTS_PER_CALENDAR = 250

# Total docs = CALENDARS_COUNT (calendar_list) + CALENDARS_COUNT (calendar) + (CALENDARS_COUNT * EVENTS_PER_CALENDAR)
DOCS_COUNT = CALENDARS_COUNT + CALENDARS_COUNT + (CALENDARS_COUNT * EVENTS_PER_CALENDAR)

app = Flask(__name__)

PRE_REQUEST_SLEEP = float(os.environ.get("PRE_REQUEST_SLEEP", "0.1"))


def get_num_docs():
    print(DOCS_COUNT)


@app.before_request
def before_request():
    time.sleep(PRE_REQUEST_SLEEP)


# Mock OAuth2 token endpoint
@app.route("/token", methods=["POST"])
def post_auth_token():
    """OAuth2 token endpoint mock."""
    return {
        "access_token": "ya29.a0AfH6SMBxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        "token_type": "Bearer",
        "expires_in": 3600,
        "refresh_token": "1//0xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    }


# Calendar API endpoints
@app.route("/calendar/v3/users/me/calendarList", methods=["GET"])
def calendar_list():
    """Mock calendar list endpoint."""
    calendars = []
    for i in range(CALENDARS_COUNT):
        calendar_id = f"cal_{i}@example.com"  # Simple calendar ID
        calendars.append({
            "kind": "calendar#calendarListEntry",
            "etag": f"\"etag_{i}\"",
            "id": calendar_id,
            "summary": f"Calendar {i}",
            "description": f"Test calendar {i} for functional testing",
            "timeZone": "America/Los_Angeles",
            "colorId": str((i % 24) + 1),  # Google Calendar has 24 color options
            "backgroundColor": f"#{'%06x' % (hash(str(i)) & 0xFFFFFF)}",
            "foregroundColor": "#000000",
            "selected": True,
            "accessRole": "owner" if i == 0 else "reader",
            "primary": i == 0,
            "defaultReminders": [
                {"method": "popup", "minutes": 30}
            ]
        })

    return {
        "kind": "calendar#calendarList",
        "etag": "\"calendar_list_etag\"",
        "items": calendars
    }


@app.route("/calendar/v3/calendars/<path:calendar_id>", methods=["GET"])
def get_calendar(calendar_id):
    """Mock get calendar endpoint."""
    # Extract index from calendar_id (e.g., "cal_0@example.com" -> 0)
    try:
        index = int(calendar_id.split("_")[-1].split("@")[0])
    except (IndexError, ValueError):
        index = 0

    # Return the same calendar ID as the calendar list entry
    # The _id collision will be handled by document type differentiation
    return {
        "kind": "calendar#calendar",
        "etag": f"\"calendar_etag_{index}\"",
        "id": calendar_id,
        "summary": f"Calendar {index}",
        "description": f"Test calendar {index} for functional testing",
        "location": f"Building {index}, Test City",
        "timeZone": "America/Los_Angeles"
    }


@app.route("/calendar/v3/calendars/<path:calendar_id>/events", methods=["GET"])
def list_events(calendar_id):
    """Mock events list endpoint."""
    # Extract index from calendar_id (e.g., "cal_0@example.com" -> 0)
    try:
        calendar_index = int(calendar_id.split("_")[-1].split("@")[0])
    except (IndexError, ValueError):
        calendar_index = 0

    # Get query parameters
    time_min = request.args.get("timeMin")
    time_max = request.args.get("timeMax")
    max_results = int(request.args.get("maxResults", 100))

    # Generate events for this calendar
    events = []
    now = datetime.utcnow()

    # Parse time range if provided by connector
    if time_min and time_max:
        try:
            start_time = datetime.fromisoformat(time_min.replace('Z', '+00:00'))
            end_time = datetime.fromisoformat(time_max.replace('Z', '+00:00'))
            time_range_days = (end_time - start_time).days
        except:
            start_time = now - timedelta(days=30)
            end_time = now + timedelta(days=30)
            time_range_days = 60
    else:
        start_time = now - timedelta(days=30)
        end_time = now + timedelta(days=30)
        time_range_days = 60

    for i in range(min(EVENTS_PER_CALENDAR, max_results)):
        event_id = f"event_{calendar_index}_{i}"
        # Spread events evenly across the requested time range
        if EVENTS_PER_CALENDAR > 1:
            days_offset = (i / (EVENTS_PER_CALENDAR - 1)) * time_range_days
            event_start = start_time + timedelta(days=days_offset, hours=i % 24)
        else:
            event_start = start_time + timedelta(hours=i % 24)
        event_end = event_start + timedelta(hours=1)

        # Create some variety in event types
        event_type = i % 4

        if event_type == 0:
            # All-day event
            event = {
                "kind": "calendar#event",
                "etag": f"\"event_etag_{calendar_index}_{i}\"",
                "id": event_id,
                "status": "confirmed",
                "htmlLink": f"https://calendar.google.com/event?eid={event_id}",
                "created": (now - timedelta(days=30)).isoformat() + "Z",
                "updated": (now - timedelta(days=1)).isoformat() + "Z",
                "summary": f"All-day Event {i} in Calendar {calendar_index}",
                "description": f"This is a test all-day event {i} for functional testing",
                "location": f"Conference Room {i}",
                "creator": {
                    "email": "test@example.com",
                    "displayName": "Test User"
                },
                "organizer": {
                    "email": "test@example.com",
                    "displayName": "Test User"
                },
                "start": {
                    "date": event_start.date().isoformat(),
                    "timeZone": "America/Los_Angeles"
                },
                "end": {
                    "date": (event_start + timedelta(days=1)).date().isoformat(),
                    "timeZone": "America/Los_Angeles"
                },
                "transparency": "transparent",
                "visibility": "default"
            }
        elif event_type == 1:
            # Meeting with attendees and conference data
            event = {
                "kind": "calendar#event",
                "etag": f"\"event_etag_{calendar_index}_{i}\"",
                "id": event_id,
                "status": "confirmed",
                "htmlLink": f"https://calendar.google.com/event?eid={event_id}",
                "created": (now - timedelta(days=30)).isoformat() + "Z",
                "updated": (now - timedelta(days=1)).isoformat() + "Z",
                "summary": f"Team Meeting {i} - Calendar {calendar_index}",
                "description": f"This is a test meeting {i} for functional testing with agenda items",
                "location": f"Zoom Meeting Room {i}",
                "creator": {
                    "email": "test@example.com",
                    "displayName": "Test User"
                },
                "organizer": {
                    "email": "test@example.com",
                    "displayName": "Test User"
                },
                "start": {
                    "dateTime": event_start.isoformat() + "Z",
                    "timeZone": "America/Los_Angeles"
                },
                "end": {
                    "dateTime": event_end.isoformat() + "Z",
                    "timeZone": "America/Los_Angeles"
                },
                "attendees": [
                    {
                        "email": f"attendee1_{i}@example.com",
                        "displayName": f"Attendee One {i}",
                        "responseStatus": "accepted"
                    },
                    {
                        "email": f"attendee2_{i}@example.com",
                        "displayName": f"Attendee Two {i}",
                        "responseStatus": "tentative"
                    }
                ],
                "conferenceData": {
                    "entryPoints": [
                        {
                            "entryPointType": "video",
                            "uri": f"https://zoom.us/j/{1234567890 + i}",
                            "label": f"zoom.us/j/{1234567890 + i}"
                        }
                    ],
                    "conferenceId": f"zoom-meeting-{i}",
                    "signature": f"signature_{i}"
                },
                "reminders": {
                    "useDefault": False,
                    "overrides": [
                        {"method": "email", "minutes": 1440},
                        {"method": "popup", "minutes": 30}
                    ]
                }
            }
        elif event_type == 2:
            # Event with attachments
            event = {
                "kind": "calendar#event",
                "etag": f"\"event_etag_{calendar_index}_{i}\"",
                "id": event_id,
                "status": "confirmed",
                "htmlLink": f"https://calendar.google.com/event?eid={event_id}",
                "created": (now - timedelta(days=30)).isoformat() + "Z",
                "updated": (now - timedelta(days=1)).isoformat() + "Z",
                "summary": f"Document Review {i} - Calendar {calendar_index}",
                "description": f"Review session {i} with attached documents for functional testing",
                "location": f"Office Building {i}",
                "creator": {
                    "email": "test@example.com",
                    "displayName": "Test User"
                },
                "organizer": {
                    "email": "test@example.com",
                    "displayName": "Test User"
                },
                "start": {
                    "dateTime": event_start.isoformat() + "Z",
                    "timeZone": "America/Los_Angeles"
                },
                "end": {
                    "dateTime": event_end.isoformat() + "Z",
                    "timeZone": "America/Los_Angeles"
                },
                "attachments": [
                    {
                        "fileId": f"file_id_{i}_1",
                        "title": f"Document {i}_1.pdf",
                        "mimeType": "application/pdf",
                        "fileUrl": f"https://drive.google.com/file/d/file_id_{i}_1/view"
                    },
                    {
                        "fileId": f"file_id_{i}_2",
                        "title": f"Presentation {i}_2.pptx",
                        "mimeType": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        "fileUrl": f"https://drive.google.com/file/d/file_id_{i}_2/view"
                    }
                ]
            }
        else:
            # Simple event
            event = {
                "kind": "calendar#event",
                "etag": f"\"event_etag_{calendar_index}_{i}\"",
                "id": event_id,
                "status": "confirmed",
                "htmlLink": f"https://calendar.google.com/event?eid={event_id}",
                "created": (now - timedelta(days=30)).isoformat() + "Z",
                "updated": (now - timedelta(days=1)).isoformat() + "Z",
                "summary": f"Simple Event {i} - Calendar {calendar_index}",
                "description": f"This is a simple test event {i} for functional testing",
                "creator": {
                    "email": "test@example.com",
                    "displayName": "Test User"
                },
                "organizer": {
                    "email": "test@example.com",
                    "displayName": "Test User"
                },
                "start": {
                    "dateTime": event_start.isoformat() + "Z",
                    "timeZone": "America/Los_Angeles"
                },
                "end": {
                    "dateTime": event_end.isoformat() + "Z",
                    "timeZone": "America/Los_Angeles"
                }
            }

        events.append(event)

    return {
        "kind": "calendar#events",
        "etag": f"\"events_etag_{calendar_index}\"",
        "summary": f"Calendar {calendar_index}",
        "description": f"Test calendar {calendar_index} events",
        "updated": now.isoformat() + "Z",
        "timeZone": "America/Los_Angeles",
        "accessRole": "owner",
        "defaultReminders": [
            {"method": "popup", "minutes": 30}
        ],
        "items": events
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10340, debug=True)