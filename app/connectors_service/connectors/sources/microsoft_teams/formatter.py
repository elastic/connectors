from calendar import month_name
from datetime import datetime

from connectors.sources.microsoft_teams.client import UserEndpointName, TeamEndpointName


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
                "sender": item["from"]["user"].get("displayName")
                if item.get("from") and item["from"].get("user")
                else "",
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
                "sender": message["from"]["user"].get("displayName")
                if message.get("from") and message["from"].get("user")
                else "",
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


USER_MEETING_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
