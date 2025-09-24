#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime, timedelta

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
        return await self.api_call(resource="calendarList", method="list", maxResults=1)

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

    async def list_events(self, calendar_id, time_min=None, time_max=None):
        """Fetch all events from a specific calendar within the specified time range.

        Args:
            calendar_id (str): The calendar ID.
            time_min (str): Start time in ISO format. Optional.
            time_max (str): End time in ISO format. Optional.

        Yields:
            dict: Events page.
        """
        params = {
            "calendarId": calendar_id,
            "maxResults": 100,
        }
        if time_min:
            params["timeMin"] = time_min
        if time_max:
            params["timeMax"] = time_max

        async for page in self.api_call_paged(
            resource="events", method="list", **params
        ):
            yield page


class GoogleCalendarDataSource(BaseDataSource):
    """Google Calendar connector for Elastic Enterprise Search.

    This connector fetches data from a user's Google Calendar (read-only mode):
      - CalendarList entries (the user's list of calendars)
      - Each underlying Calendar resource
      - Events belonging to each Calendar

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
            "days_back": {
                "display": "numeric",
                "label": "Days back to fetch events",
                "order": 3,
                "type": "int",
                "value": 30,
            },
            "days_forward": {
                "display": "numeric",
                "label": "Days forward to fetch events",
                "order": 4,
                "type": "int",
                "value": 30,
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
        return load_service_account_json(service_account_credentials, "Google Calendar")

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

        # Calculate time range based on configuration
        now = datetime.utcnow()
        days_back = self.configuration.get("days_back", 30)
        days_forward = self.configuration.get("days_forward", 30)

        time_min = (now - timedelta(days=days_back)).isoformat() + "Z"
        time_max = (now + timedelta(days=days_forward)).isoformat() + "Z"

        # Create calendar reference for events
        calendar_ref = {
            "id": calendar_id,
            "name": cal_list_doc.get("summary_override")
            or cal_list_doc.get("summary")
            or "",
            "type": "calendar",
        }

        try:
            async for page in client.list_events(calendar_id, time_min, time_max):
                for event in page.get("items", []):
                    event_id = event["id"]
                    # Extract date/time fields
                    start_info = event.get("start", {})
                    end_info = event.get("end", {})
                    start_datetime = start_info.get("dateTime")
                    start_date = start_info.get("date")
                    end_datetime = end_info.get("dateTime")
                    end_date = end_info.get("date")

                    # Extract attendee names and emails
                    attendees_info = []
                    if event.get("attendees"):
                        for attendee in event.get("attendees", []):
                            attendee_info = {}
                            if attendee.get("displayName"):
                                attendee_info["name"] = attendee.get("displayName")
                            if attendee.get("email"):
                                attendee_info["email"] = attendee.get("email")
                            if attendee_info:
                                attendees_info.append(attendee_info)

                    # Extract meeting/zoom links from conferenceData or location
                    meeting_link = None
                    if event.get("conferenceData"):
                        entry_points = event.get("conferenceData", {}).get(
                            "entryPoints", []
                        )
                        for entry_point in entry_points:
                            if entry_point.get("uri"):
                                meeting_link = entry_point.get("uri")
                                break

                    # Extract attachments
                    attachments_info = []
                    if event.get("attachments"):
                        for attachment in event.get("attachments", []):
                            attachment_info = {}
                            if attachment.get("title"):
                                attachment_info["title"] = attachment.get("title")
                            if attachment.get("fileUrl"):
                                attachment_info["url"] = attachment.get("fileUrl")
                            if attachment.get("mimeType"):
                                attachment_info["mime_type"] = attachment.get(
                                    "mimeType"
                                )
                            if attachment_info:
                                attachments_info.append(attachment_info)

                    doc = {
                        "_id": event_id,
                        "type": "event",
                        "event_id": event_id,
                        "calendar_id": calendar_id,
                        "calendar": calendar_ref,
                        "summary": event.get("summary"),
                        "description": event.get("description"),
                        "location": event.get("location"),
                        "meeting_link": meeting_link,
                        "start_datetime": start_datetime,
                        "start_date": start_date,
                        "end_datetime": end_datetime,
                        "end_date": end_date,
                        "attendees": attendees_info,
                        "attachments": attachments_info,
                    }
                    yield doc
        except Exception as e:
            self._logger.warning(
                f"Error fetching events for calendar {calendar_id}: {str(e)}"
            )

    async def close(self):
        """Close any resources."""
        pass
