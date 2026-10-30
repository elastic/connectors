#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from connectors.sources.outlook_cloud.constants import CALENDAR_OBJECT
from connectors.sources.outlook_cloud.mail_eml import _smtp_address
from connectors.sources.outlook_cloud.utils import (
    graph_datetime_to_iso_utc,
    graph_event_type_to_legacy,
    graph_importance_to_legacy,
    graph_recurrence_to_legacy,
)
from connectors.utils import html_to_text


class OutlookCloudDocFormatter:
    """Format Microsoft Graph message JSON to Elasticsearch documents."""

    @staticmethod
    def _document_sender(message):
        sender = _smtp_address(message.get("sender"))
        if sender:
            return sender
        return _smtp_address(message.get("from"))

    def mail_doc_formatter(self, message, mail_type):
        document = {
            "_id": message["id"],
            "_timestamp": graph_datetime_to_iso_utc(
                message.get("lastModifiedDateTime")
            ),
            "title": message.get("subject"),
            "type": mail_type["constant"],
            "sender": self._document_sender(message),
            "to_recipients": _recipient_list(message.get("toRecipients")),
            "cc_recipients": _recipient_list(message.get("ccRecipients")),
            "bcc_recipients": _recipient_list(message.get("bccRecipients")),
            "importance": graph_importance_to_legacy(message.get("importance")),
            "categories": list(message.get("categories") or []),
            "message": _message_text(message.get("body")),
        }
        folder_name = mail_type.get("folder_name")
        if folder_name is not None:
            document["folder_name"] = folder_name
        return document

    def calendar_doc_formatter(self, event, birthday=False):
        """Format a Graph event. `birthday` events only carry the day they fall on."""
        document = {
            "_id": event["id"],
            "_timestamp": graph_datetime_to_iso_utc(event.get("lastModifiedDateTime")),
            "type": CALENDAR_OBJECT,
            "title": event.get("subject"),
            "meeting_type": _meeting_type(event),
            "organizer": _smtp_address(event.get("organizer")),
        }
        start = _event_time(event.get("start"))
        if birthday:
            document["date"] = start.split("T", 1)[0] if start else None
        else:
            document.update(
                {
                    "attendees": _required_attendees(event.get("attendees")),
                    "start_date": start,
                    "end_date": _event_time(event.get("end")),
                    "location": (event.get("location") or {}).get("displayName"),
                    "content": _message_text(event.get("body")),
                }
            )
        return document


def _meeting_type(event):
    """`Single`, `Recurring <pattern text>`, or the legacy event type when there is no pattern."""
    event_type = graph_event_type_to_legacy(event.get("type"))
    if event_type == "Single":
        return event_type
    pattern = graph_recurrence_to_legacy((event.get("recurrence") or {}).get("pattern"))
    return f"Recurring {pattern}" if pattern else event_type


def _event_time(date_time_zone):
    """A Graph `dateTimeTimeZone`, already in UTC through the Prefer header, as ``YYYY-MM-DDTHH:MM:SSZ``."""
    return graph_datetime_to_iso_utc((date_time_zone or {}).get("dateTime"))


def _required_attendees(attendees):
    if not isinstance(attendees, list):
        return []
    return [
        address
        for address in (
            _smtp_address(attendee)
            for attendee in attendees
            if isinstance(attendee, dict) and attendee.get("type") == "required"
        )
        if address
    ]


def _recipient_list(recipients):
    if not isinstance(recipients, list):
        return []
    return [
        address
        for address in (_smtp_address(recipient) for recipient in recipients)
        if address
    ]


def _message_text(body):
    if not isinstance(body, dict):
        return None
    content = body.get("content")
    if not isinstance(content, str):
        return None
    if body.get("contentType") == "text":
        return content
    return html_to_text(html=content)
