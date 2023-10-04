import io
import os

from flask import Flask, request
from flask_limiter import HEADERS, Limiter
from flask_limiter.util import get_remote_address

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

app = Flask(__name__)


THROTTLING = os.environ.get("THROTTLING", False)
PRE_REQUEST_SLEEP = float(os.environ.get("PRE_REQUEST_SLEEP", "0.05"))

if THROTTLING:
    limiter = Limiter(
        get_remote_address,
        app=app,
        storage_uri="memory://",
        application_limits=[
            "6000 per minute",
            "6000000 per day",
        ],  # Microsoft 50k+ licences limits
        retry_after="delta-seconds",
        headers_enabled=True,
        header_name_mapping={
            HEADERS.LIMIT: "RateLimit-Limit",
            HEADERS.RESET: "RateLimit-Reset",
            HEADERS.REMAINING: "RateLimit-Remaining",
        },
    )

    limiter.init_app(app)

SIZES = {
    "small": 100000,
    "medium": 500000,
    "large": 1000000,
}

DOC_ID_SIZE = 36


def adjust_document_id_size(doc_id):
    """
    This methods make sure that all the documemts ids are min 36 bytes
    """

    bytesize = len(doc_id)

    if bytesize >= DOC_ID_SIZE:
        return doc_id

    addition = "".join(["0" for _ in range(DOC_ID_SIZE - bytesize - 1)])
    return f"{doc_id}-{addition}"


DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

match DATA_SIZE:
    case "small":
        MESSAGES = 25
        EVENTS = 50
        CHANNEL = 3
        FILES = 10
        CHANNEL_MESSAGE = 50
    case "medium":
        MESSAGES = 50
        EVENTS = 50
        CHANNEL = 3
        FILES = 50
        CHANNEL_MESSAGE = 500
    case "large":
        MESSAGES = 250
        EVENTS = 150
        CHANNEL = 5
        FILES = 150
        CHANNEL_MESSAGE = 1000
    case _:
        raise Exception(
            f"Unknown DATA_SIZE: {DATA_SIZE}. Expecting 'small', 'medium' or 'large'"
        )

MESSAGES_TO_DELETE = 10
EVENTS_TO_DELETE = 1

ROOT = os.environ.get("OVERRIDE_URL", "http://127.0.0.1:10971")


def get_num_docs():
    # I tried to do the maths, but it's not possible without diving too deep into the connector
    # Therefore, doing naive way - just ran connector and took the number from the test
    expected_count = 0
    match DATA_SIZE:
        case "small":
            expected_count = 508
        case "medium":
            expected_count = 4613
        case "large":
            expected_count = 15435

    print(expected_count)


class MicrosoftTeamsAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.app.route("/me", methods=["GET"])(self.get_myself)

        self.app.route("/chats", methods=["GET"])(self.get_user_chats)
        self.app.route("/chats/<string:chat_id>/messages", methods=["GET"])(
            self.get_user_chat_messages
        )
        self.app.route("/chats/<string:chat_id>/tabs", methods=["GET"])(
            self.get_user_chat_tabs
        )

        self.app.route("/users", methods=["GET"])(self.get_users)
        self.app.route("/users/<string:user_id>/events", methods=["GET"])(
            self.get_events
        )

        self.app.route("/teams", methods=["GET"])(self.get_teams)
        self.app.route("/teams/<string:team_id>/channels", methods=["GET"])(
            self.get_channels
        )
        self.app.route(
            "/teams/<string:team_id>/channels/<string:channel_id>/tabs", methods=["GET"]
        )(self.get_channel_tabs)
        self.app.route(
            "/teams/<string:team_id>/channels/<string:channel_id>/messages",
            methods=["GET"],
        )(self.get_channel_messages)
        self.app.route(
            "/teams/<string:team_id>/channels/<string:channel_id>/filesFolder",
            methods=["GET"],
        )(self.get_teams_filefolder)
        self.app.route(
            "/drives/<string:drive_id>/items/<string:item_id>/children",
            methods=["GET"],
        )(self.get_teams_file)

        self.app.route("/sites/list.txt", methods=["GET"])(self.download_file)

    def get_myself(self):
        return {
            "displayName": "Alex Wilber",
            "givenName": "Alex",
            "mail": "Alex@3hr2.onmicrosoft.com",
            "userPrincipalName": "Alex@3hr2.onmicrosoft.com",
            "id": adjust_document_id_size("me-1"),
        }

    def get_user_chats(self):
        return {
            "value": [
                {
                    "id": adjust_document_id_size("1"),
                    "topic": None,
                    "createdDateTime": "2023-07-21T21:24:18.338Z",
                    "lastUpdatedDateTime": "2023-07-21T21:24:18.338Z",
                    "chatType": "oneOnOne",
                    "webUrl": "https://teams.microsoft.com/l/chat/1",
                    "members": [
                        {
                            "@odata.type": "#microsoft.graph.aadUserConversationMember",
                            "displayName": "Cervantes, Andres",
                            "userId": "123abc",
                            "email": "ACervantes@mlock.com",
                        },
                    ],
                }
            ]
        }

    def get_user_chat_messages(self, chat_id):
        global MESSAGES
        message_data = []
        top = int(request.args.get("$top"))
        for message in range(MESSAGES):
            message_data.append(
                {
                    "id": adjust_document_id_size(f"user-chat-{message}-{MESSAGES}"),
                    "messageType": "onetoone",
                    "createdDateTime": "2023-07-21T21:24:18.726Z",
                    "lastModifiedDateTime": "2023-07-21T21:24:18.726Z",
                    "deletedDateTime": None,
                    "subject": None,
                    "summary": None,
                    "webUrl": None,
                    "from": None,
                    "body": {"contentType": "html", "content": "<h1>dummy data</h1>"},
                    "attachments": [],
                    "eventDetail": None,
                }
            )
        response = {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#Chats)",
            "value": message_data,
        }

        if len(message_data) == top:
            response["@odata.nextLink"] = f"{ROOT}/chats/{chat_id}/messages?$top=50"
            MESSAGES -= MESSAGES_TO_DELETE  # performs deletion and pagination
        return response

    def get_user_chat_tabs(self, chat_id):
        return {
            "value": [
                {
                    "id": adjust_document_id_size("tab-1"),
                    "displayName": "Notes",
                    "configuration": {"websiteUrl": "https://onenote.com"},
                }
            ]
        }

    def get_users(self):
        return {
            "value": [
                {
                    "mail": "AdeleV@3hmnr2.onmicrosoft.com",
                    "id": adjust_document_id_size("user-1"),
                }
            ]
        }

    def get_events(self, user_id):
        global EVENTS
        event_data = []
        top = int(request.args.get("$top"))
        for event in range(EVENTS):
            event_data.append(
                {
                    "id": adjust_document_id_size(f"event-{user_id}"),
                    "createdDateTime": "2023-08-10T08:22:14.5296951Z",
                    "lastModifiedDateTime": "2023-08-10T08:25:29.3693436Z",
                    "categories": [],
                    "originalStartTimeZone": "India Standard Time",
                    "originalEndTimeZone": "India Standard Time",
                    "reminderMinutesBeforeStart": 15,
                    "isReminderOn": True,
                    "subject": "new meet",
                    "bodyPreview": "Body prebiew dummy",
                    "importance": "normal",
                    "isAllDay": False,
                    "isCancelled": False,
                    "showAs": "busy",
                    "webLink": f"https://outlook.office365.com/calendar/item/{event}",
                    "body": {"contentType": "html", "content": "<html>dummy</html>"},
                    "start": {
                        "dateTime": "2023-08-10T08:00:00.0000000",
                        "timeZone": "UTC",
                    },
                    "end": {
                        "dateTime": "2023-08-10T08:30:00.0000000",
                        "timeZone": "UTC",
                    },
                    "locations": [
                        {
                            "displayName": "Microsoft Teams Meeting",
                        }
                    ],
                    "recurrence": None,
                    "attendees": [],
                    "organizer": {
                        "emailAddress": {
                            "name": "Dummy",
                            "address": "dummy@mnr.onmicrosoft.com",
                        }
                    },
                    "onlineMeeting": {"joinUrl": "https://teams.microsoft.com/meet"},
                }
            )
        response = {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#events)",
            "value": event_data,
        }

        if len(event_data) == top:
            response["@odata.nextLink"] = f"{ROOT}/users/{user_id}/events?$top=50"
            EVENTS -= EVENTS_TO_DELETE
        return response

    def get_teams(self):
        return {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#teams)",
            "value": [
                {
                    "id": adjust_document_id_size("team-1"),
                    "createdDateTime": None,
                    "displayName": "team1",
                    "description": "team1",
                    "webUrl": None,
                    "summary": None,
                },
                {
                    "id": adjust_document_id_size("team-2"),
                    "createdDateTime": None,
                    "displayName": "team2",
                    "description": "team2",
                    "webUrl": None,
                    "summary": None,
                },
                {
                    "id": adjust_document_id_size("team-3"),
                    "createdDateTime": None,
                    "displayName": "team3",
                    "description": "team3",
                    "webUrl": None,
                    "summary": None,
                },
            ],
        }

    def get_channels(self, team_id):
        channel_list = []
        for channel in range(CHANNEL):
            channel_list.append(
                {
                    "id": adjust_document_id_size(f"channel-{channel}-{team_id}"),
                    "createdDateTime": "2023-08-16T04:46:53.056Z",
                    "displayName": f"General-{channel}",
                    "description": "channel",
                    "webUrl": f"https://teams.microsoft.com/l/channel/{channel}/{team_id}",
                }
            )
        return {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#teams)",
            "value": channel_list,
        }

    def get_channel_messages(self, team_id, channel_id):
        message_list = []
        for message in range(CHANNEL_MESSAGE):
            message_list.append(
                {
                    "id": adjust_document_id_size(
                        f"message-{team_id}-{channel_id}-{message}"
                    ),
                    "messageType": "message",
                    "createdDateTime": "2023-08-16T04:47:55.794Z",
                    "lastModifiedDateTime": "2023-08-16T04:47:55.794Z",
                    "deletedDateTime": None,
                    "subject": "",
                    "summary": None,
                    "webUrl": f"https://teams.microsoft.com/l/message/{team_id}/{channel_id}/{message}",
                    "policyViolation": None,
                    "eventDetail": None,
                    "from": {
                        "user": {
                            "displayName": "Dummy",
                        }
                    },
                    "body": {
                        "contentType": "html",
                        "content": "<div>I added a tab at the top of this channel. Check it out!</div>",
                    },
                    "attachments": [],
                }
            )
        return {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#message)",
            "value": message_list,
        }

    def get_channel_tabs(self, team_id, channel_id):
        return {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#tabs)",
            "value": [
                {
                    "id": adjust_document_id_size(f"tabs-{team_id}-{channel_id}"),
                    "displayName": "Notes",
                    "webUrl": f"https://teams.microsoft.com/l/entity/tab/{team_id}/{channel_id}",
                    "configuration": {"websiteUrl": "https://onenote.com"},
                }
            ],
        }

    def get_teams_filefolder(self, team_id, channel_id):
        return {
            "id": "filfolder-1",
            "createdDateTime": "0001-01-01T00:00:00Z",
            "lastModifiedDateTime": "2023-09-21T10:23:48Z",
            "name": "root",
            "size": 351660,
            "parentReference": {
                "driveId": "driveid-123",
                "driveType": "documentLibrary",
            },
        }

    def get_teams_file(self, drive_id, item_id):
        files_list = []
        for file_data in range(FILES):
            files_list.append(
                {
                    "@microsoft.graph.downloadUrl": f"{ROOT}/sites/list.txt",
                    "createdDateTime": "2023-08-16T04:47:26Z",
                    "id": adjust_document_id_size(
                        f"file-{file_data}-{drive_id}-{item_id}"
                    ),
                    "lastModifiedDateTime": "2023-08-16T04:47:29Z",
                    "name": "list.txt",
                    "size": 45441,
                    "webUrl": f"{ROOT}/sites/list.html",
                    "file": {
                        "mimeType": "text/plain",
                    },
                }
            )
        return {"value": files_list}

    def download_file(self):
        return io.BytesIO(bytes(fake_provider.get_html(), encoding="utf-8"))


if __name__ == "__main__":
    MicrosoftTeamsAPI().app.run(host="0.0.0.0", port=10971)
