#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import urllib.parse
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.sources.google import (
    GoogleServiceAccountClient,
    load_service_account_json,
    remove_universe_domain,
    validate_service_account_json,
)


# Default timeout for Google Calendar API calls (in seconds)
DEFAULT_TIMEOUT = 60

# Calendar API scopes
CALENDAR_READONLY_SCOPE = "https://www.googleapis.com/auth/calendar.readonly"


class GoogleCalendarClient(GoogleServiceAccountClient):
    """A Google Calendar client to handle API calls to the Google Calendar API."""

    def __init__(self, json_credentials, subject=None):
        """Initialize the GoogleCalendarClient.

        Args:
            json_credentials (dict): Service account credentials JSON.
            subject (str, optional): Email of the user to impersonate. Defaults to None.
        """
        remove_universe_domain(json_credentials)
        if subject:
            json_credentials["subject"] = subject

        super().__init__(
            json_credentials=json_credentials,
            api="calendar",
            api_version="v3",
            scopes=[CALENDAR_READONLY_SCOPE],
            api_timeout=DEFAULT_TIMEOUT,
        )

    async def ping(self):
        """Verify connectivity to Google Calendar API."""
        return await self.api_call(
            resource="calendarList", method="list", maxResults=1
        )

    async def list_calendar_list(self):
        """Fetch all calendar list entries from Google Calendar.

        Yields:
            dict: Calendar list entry.
        """
        async for page in self.api_call_paged(
            resource="calendarList",
            method="list",
            maxResults=100,
        ):
            yield page

    async def get_calendar(self, calendar_id):
        """Get a specific calendar by ID.

        Args:
            calendar_id (str): The calendar ID.

        Returns:
            dict: Calendar resource.
        """
        return await self.api_call(
            resource="calendars", method="get", calendarId=calendar_id
        )

    async def list_events(self, calendar_id):
        """Fetch all events from a specific calendar.

        Args:
            calendar_id (str): The calendar ID.

        Yields:
            dict: Events page.
        """
        async for page in self.api_call_paged(
            resource="events",
            method="list",
            calendarId=calendar_id,
            maxResults=100,
        ):
            yield page

    async def get_free_busy(self, calendar_ids, time_min, time_max):
        """Get free/busy information for a list of calendars.

        Args:
            calendar_ids (list): List of calendar IDs.
            time_min (str): Start time in ISO format.
            time_max (str): End time in ISO format.

        Returns:
            dict: Free/busy information.
        """
        items = [{"id": calendar_id} for calendar_id in calendar_ids]
        request_body = {
            "timeMin": time_min,
            "timeMax": time_max,
            "items": items,
        }

        return await self.api_call(
            resource="freebusy", method="query", body=request_body
        )


class GoogleCalendarDataSource(BaseDataSource):
    """Google Calendar connector for Elastic Enterprise Search.
    
    This connector fetches data from a user's Google Calendar (read-only mode):
      - CalendarList entries (the user's list of calendars)
      - Each underlying Calendar resource
      - Events belonging to each Calendar
      - Free/Busy data for each Calendar
    
    Reference:
        https://developers.google.com/calendar/api/v3/reference
    """

    name = "Google Calendar"
    service_type = "google_calendar"

    def __init__(self, configuration):
        """Initialize the GoogleCalendarDataSource.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self._calendar_client = None
        self.include_freebusy = self.configuration.get("include_freebusy", False)

    @classmethod
    def get_default_configuration(cls):
        """Returns a dict with a default configuration for the connector."""
        return {
            "service_account_credentials": {
                "display": "textarea",
                "label": "Google Calendar service account JSON",
                "order": 1,
                "required": True,
                "sensitive": True,
                "type": "str",
            },
            "subject": {
                "display": "text",
                "label": "User email to impersonate",
                "order": 2,
                "required": True,
                "type": "str",
            },
            "include_freebusy": {
                "display": "toggle",
                "label": "Include Free/Busy Data",
                "order": 3,
                "type": "bool",
                "value": False,
            },
        }

    def _set_internal_logger(self):
        """Set the logger for internal components."""
        if self._calendar_client:
            self._calendar_client.set_logger(self._logger)

    @property
    def _service_account_credentials(self):
        """Load and return the service account credentials.

        Returns:
            dict: The loaded service account credentials.
        """
        service_account_credentials = self.configuration["service_account_credentials"]
        return load_service_account_json(
            service_account_credentials, "Google Calendar"
        )

    def calendar_client(self):
        """Get or create a Google Calendar client.

        Returns:
            GoogleCalendarClient: An initialized Google Calendar client.
        """
        if not self._calendar_client:
            self._calendar_client = GoogleCalendarClient(
                json_credentials=self._service_account_credentials,
                subject=self.configuration["subject"],
            )
            self._calendar_client.set_logger(self._logger)
        return self._calendar_client

    async def validate_config(self):
        """Validate the configuration.

        Raises:
            ConfigurableFieldValueError: If the configuration is invalid.
        """
        await super().validate_config()
        validate_service_account_json(
            self.configuration["service_account_credentials"], "Google Calendar"
        )

        # Verify connectivity to Google Calendar API
        try:
            await self.ping()
        except Exception as e:
            msg = f"Google Calendar authentication failed. Please check your service account credentials and subject email. Error: {str(e)}"
            raise ConfigurableFieldValueError(msg) from e

    async def ping(self):
        """Verify connectivity to Google Calendar API."""
        await self.calendar_client().ping()

    async def get_docs(self, filtering=None):
        """Yields documents from Google Calendar API.
        
        Each document is a tuple with:
        - a mapping with the data to index
        - an optional mapping with attachment data
        """
        client = self.calendar_client()
        
        # 1) Get the user's calendarList
        calendar_list_entries = []
        async for cal_list_doc in self._generate_calendar_list_docs(client):
            yield cal_list_doc, None
            calendar_list_entries.append(cal_list_doc)
        
        # 2) For each calendar in the user's calendarList, yield its Calendar resource
        for cal_list_doc in calendar_list_entries:
            async for calendar_doc in self._generate_calendar_docs(
                client, cal_list_doc["calendar_id"]
            ):
                yield calendar_doc, None
        
        # 3) For each calendar, yield event documents
        for cal_list_doc in calendar_list_entries:
            async for event_doc in self._generate_event_docs(client, cal_list_doc):
                yield event_doc, None
        
        # 4) (Optionally) yield free/busy data for each calendar
        if self.include_freebusy:
            calendar_ids = [cal["calendar_id"] for cal in calendar_list_entries]
            if calendar_ids:
                async for freebusy_doc in self._generate_freebusy_docs(
                    client, calendar_ids
                ):
                    yield freebusy_doc, None

    async def _generate_calendar_list_docs(self, client):
        """Yield documents for each calendar in the user's CalendarList.
        
        Args:
            client (GoogleCalendarClient): The Google Calendar client.
            
        Yields:
            dict: Calendar list entry document.
        """
        async for page in client.list_calendar_list():
            items = page.get("items", [])
            for cal in items:
                doc = {
                    "_id": cal["id"],
                    "type": "calendar_list",
                    "calendar_id": cal["id"],
                    "summary": cal.get("summary"),
                    "summary_override": cal.get("summaryOverride"),
                    "color_id": cal.get("colorId"),
                    "background_color": cal.get("backgroundColor"),
                    "foreground_color": cal.get("foregroundColor"),
                    "hidden": cal.get("hidden", False),
                    "selected": cal.get("selected", False),
                    "access_role": cal.get("accessRole"),
                    "primary": cal.get("primary", False),
                    "deleted": cal.get("deleted", False),
                }
                yield doc

    async def _generate_calendar_docs(self, client, calendar_id):
        """Yield a document for the specified calendar_id.
        
        Args:
            client (GoogleCalendarClient): The Google Calendar client.
            calendar_id (str): The calendar ID.
            
        Yields:
            dict: Calendar document.
        """
        try:
            data = await client.get_calendar(calendar_id)
            doc = {
                "_id": data["id"],
                "type": "calendar",
                "calendar_id": data["id"],
                "summary": data.get("summary"),
                "description": data.get("description"),
                "location": data.get("location"),
                "time_zone": data.get("timeZone"),
            }
            yield doc
        except Exception as e:
            self._logger.warning(f"Error fetching calendar {calendar_id}: {str(e)}")

    async def _generate_event_docs(self, client, cal_list_doc):
        """Yield documents for all events in the given calendar.
        
        Args:
            client (GoogleCalendarClient): The Google Calendar client.
            cal_list_doc (dict): Calendar list entry document.
            
        Yields:
            dict: Event document.
        """
        calendar_id = cal_list_doc["calendar_id"]
        
        # Create calendar reference for events
        calendar_ref = {
            "id": calendar_id,
            "name": cal_list_doc.get("summary_override") or cal_list_doc.get("summary") or "",
            "type": "calendar",
        }
        
        try:
            async for page in client.list_events(calendar_id):
                for event in page.get("items", []):
                    event_id = event["id"]
                    # Extract date/time fields
                    start_info = event.get("start", {})
                    end_info = event.get("end", {})
                    start_datetime = start_info.get("dateTime")
                    start_date = start_info.get("date")
                    end_datetime = end_info.get("dateTime")
                    end_date = end_info.get("date")
                    created_at_str = event.get("created")
                    updated_at_str = event.get("updated")
                    
                    # Convert created/updated to datetime if present
                    created_at = (
                        datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                        if created_at_str
                        else None
                    )
                    updated_at = (
                        datetime.fromisoformat(updated_at_str.replace("Z", "+00:00"))
                        if updated_at_str
                        else None
                    )
                    
                    doc = {
                        "_id": event_id,
                        "type": "event",
                        "event_id": event_id,
                        "calendar_id": calendar_id,
                        "calendar": calendar_ref,
                        "status": event.get("status"),
                        "html_link": event.get("htmlLink"),
                        "created_at": created_at.isoformat() if created_at else None,
                        "updated_at": updated_at.isoformat() if updated_at else None,
                        "summary": event.get("summary"),
                        "description": event.get("description"),
                        "location": event.get("location"),
                        "color_id": event.get("colorId"),
                        "start_datetime": start_datetime,
                        "start_date": start_date,
                        "end_datetime": end_datetime,
                        "end_date": end_date,
                        "recurrence": event.get("recurrence"),
                        "recurring_event_id": event.get("recurringEventId"),
                        "organizer": event.get("organizer"),
                        "creator": event.get("creator"),
                        "attendees": event.get("attendees"),
                        "transparency": event.get("transparency"),
                        "visibility": event.get("visibility"),
                        "conference_data": event.get("conferenceData"),
                        "event_type": event.get("eventType"),
                    }
                    yield doc
        except Exception as e:
            self._logger.warning(f"Error fetching events for calendar {calendar_id}: {str(e)}")

    async def _generate_freebusy_docs(self, client, calendar_ids):
        """Yield documents for free/busy data for the next 7 days for each calendar.
        
        Args:
            client (GoogleCalendarClient): The Google Calendar client.
            calendar_ids (list): List of calendar IDs.
            
        Yields:
            dict: Free/busy document.
        """
        now = datetime.utcnow()
        in_7_days = now + timedelta(days=7)
        time_min = now.isoformat() + "Z"
        time_max = in_7_days.isoformat() + "Z"
        
        try:
            data = await client.get_free_busy(calendar_ids, time_min, time_max)
            calendars = data.get("calendars", {})
            
            for calendar_id, busy_info in calendars.items():
                busy_ranges = busy_info.get("busy", [])
                
                doc = {
                    "_id": f"{calendar_id}_freebusy",
                    "type": "freebusy",
                    "calendar_id": calendar_id,
                    "busy": busy_ranges,
                    "time_min": time_min,
                    "time_max": time_max,
                }
                yield doc
        except Exception as e:
            self._logger.warning(f"Error fetching free/busy data: {str(e)}")

    async def close(self):
        """Close any resources."""
        pass