#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
import io
import os

from flask import Flask, request
from flask_limiter import HEADERS, Limiter
from flask_limiter.util import get_remote_address

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

THROTTLING = os.environ.get("THROTTLING", False)

DOC_ID_SIZE = 36

DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

match DATA_SIZE:
    case "small":
        TEAMS = 2
        MEMBERS = 3
        CHANNELS = 2
        CHANNEL_MESSAGES = 10
        REPLIES = 1
        FILES = 3
        CHATS = 3
        CHAT_MESSAGES = 10
    case "medium":
        TEAMS = 3
        MEMBERS = 5
        CHANNELS = 3
        CHANNEL_MESSAGES = 50
        REPLIES = 2
        FILES = 10
        CHATS = 10
        CHAT_MESSAGES = 50
    case "large":
        TEAMS = 5
        MEMBERS = 10
        CHANNELS = 5
        CHANNEL_MESSAGES = 100
        REPLIES = 3
        FILES = 20
        CHATS = 20
        CHAT_MESSAGES = 100
    case _:
        msg = f"Unknown DATA_SIZE: {DATA_SIZE}. Expecting 'small', 'medium' or 'large'"
        raise Exception(msg)


def adjust_document_id_size(doc_id):
    """Ensure document ids are at least DOC_ID_SIZE bytes."""
    bytesize = len(doc_id)
    if bytesize >= DOC_ID_SIZE:
        return doc_id
    addition = "".join(["0" for _ in range(DOC_ID_SIZE - bytesize - 1)])
    return f"{doc_id}-{addition}"


def expected_docs_count():
    teams = TEAMS
    team_members = TEAMS * MEMBERS
    channels = TEAMS * CHANNELS
    channel_messages = TEAMS * CHANNELS * CHANNEL_MESSAGES
    channel_replies = TEAMS * CHANNELS * CHANNEL_MESSAGES * REPLIES
    channel_attachments = TEAMS * CHANNELS * FILES
    chats = CHATS
    chat_messages = CHATS * CHAT_MESSAGES
    # every chat message carries a single reference attachment resolving to one file
    chat_attachments = CHATS * CHAT_MESSAGES
    return (
        teams
        + team_members
        + channels
        + channel_messages
        + channel_replies
        + channel_attachments
        + chats
        + chat_messages
        + chat_attachments
    )


def get_num_docs():
    print(expected_docs_count())


def _message(message_id, with_replies=False):
    replies = []
    if with_replies:
        for reply in range(REPLIES):
            replies.append(_message(f"{message_id}-reply-{reply}", with_replies=False))
    return {
        "id": adjust_document_id_size(message_id),
        "messageType": "message",
        "createdDateTime": "2023-08-16T04:47:55.794Z",
        "lastModifiedDateTime": "2023-08-16T04:47:55.794Z",
        "deletedDateTime": None,
        "subject": None,
        "summary": None,
        "webUrl": f"https://teams.microsoft.com/l/message/{message_id}",
        "eventDetail": None,
        "from": {"user": {"id": "sender-1", "displayName": "Dummy"}},
        "body": {"contentType": "html", "content": fake_provider.get_html()},
        "attachments": [],
        "replies": replies,
    }


def _file(file_id, drive_id):
    return {
        "id": adjust_document_id_size(file_id),
        "name": "list.txt",
        "webUrl": f"{ROOT}/download/{file_id}",
        "size": 45441,
        "createdDateTime": "2023-08-16T04:47:26Z",
        "lastModifiedDateTime": "2023-08-16T04:47:29Z",
        "file": {"mimeType": "text/plain"},
        "parentReference": {"driveId": drive_id, "driveType": "documentLibrary"},
    }


ROOT = os.environ.get("OVERRIDE_URL", "http://127.0.0.1:10971")


class MicrosoftTeamsAPI:
    def __init__(self):
        self.app = Flask(__name__)

        if THROTTLING:
            limiter = Limiter(
                get_remote_address,
                app=self.app,
                storage_uri="memory://",
                application_limits=["6000 per minute", "6000000 per day"],
                retry_after="delta-seconds",
                headers_enabled=True,
                header_name_mapping={
                    HEADERS.LIMIT: "RateLimit-Limit",
                    HEADERS.RESET: "RateLimit-Reset",
                    HEADERS.REMAINING: "RateLimit-Remaining",
                },
            )
            limiter.init_app(self.app)

        self.app.route("/<string:tenant_id>/oauth2/v2.0/token", methods=["POST"])(
            self.get_token
        )

        self.app.route("/teams", methods=["GET"])(self.get_teams)
        self.app.route("/teams/<string:team_id>/members", methods=["GET"])(
            self.get_team_members
        )
        self.app.route("/teams/<string:team_id>/channels", methods=["GET"])(
            self.get_channels
        )
        self.app.route(
            "/teams/<string:team_id>/channels/<string:channel_id>/messages",
            methods=["GET"],
        )(self.get_channel_messages)
        self.app.route(
            "/teams/<string:team_id>/channels/<string:channel_id>/filesFolder",
            methods=["GET"],
        )(self.get_channel_files_folder)

        self.app.route("/chats", methods=["GET"])(self.get_chats)
        self.app.route("/chats/<string:chat_id>/messages", methods=["GET"])(
            self.get_chat_messages
        )

        self.app.route("/users/<string:user_id>/drive", methods=["GET"])(
            self.get_user_drive
        )
        self.app.route(
            "/drives/<string:drive_id>/items/<string:item_id>/children",
            methods=["GET"],
        )(self.get_drive_children)
        self.app.route(
            "/drives/<string:drive_id>/items/<string:item_id>/content",
            methods=["GET"],
        )(self.download_file)

    def get_token(self, tenant_id):
        return {
            "access_token": "fake-access-token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }

    def get_teams(self):
        teams = []
        for team in range(TEAMS):
            teams.append(
                {
                    "id": adjust_document_id_size(f"team-{team}"),
                    "displayName": f"team{team}",
                    "description": f"team {team} description",
                    "createdDateTime": "2023-08-16T04:46:53.056Z",
                    "webUrl": f"https://teams.microsoft.com/l/team/{team}",
                }
            )
        return {"value": teams}

    def get_team_members(self, team_id):
        members = []
        for member in range(MEMBERS):
            members.append(
                {
                    "id": f"membership-{member}-{team_id}",
                    "displayName": f"Member {member}",
                    "userId": f"user-{member}",
                    "email": f"member{member}@example.onmicrosoft.com",
                    "roles": ["owner"] if member == 0 else [],
                }
            )
        return {"value": members}

    def get_channels(self, team_id):
        channels = []
        for channel in range(CHANNELS):
            channels.append(
                {
                    "id": adjust_document_id_size(f"channel-{channel}-{team_id}"),
                    "displayName": f"General-{channel}",
                    "description": "channel",
                    "webUrl": f"https://teams.microsoft.com/l/channel/{channel}/{team_id}",
                    "createdDateTime": "2023-08-16T04:46:53.056Z",
                }
            )
        return {"value": channels}

    def get_channel_messages(self, team_id, channel_id):
        messages = []
        for message in range(CHANNEL_MESSAGES):
            messages.append(
                _message(f"message-{team_id}-{channel_id}-{message}", with_replies=True)
            )
        return {"value": messages}

    def get_channel_files_folder(self, team_id, channel_id):
        drive_id = f"cdrive-{team_id}-{channel_id}"
        return {
            "id": f"cfolder-{team_id}-{channel_id}",
            "name": "root",
            "parentReference": {"driveId": drive_id, "driveType": "documentLibrary"},
        }

    def get_chats(self):
        chats = []
        for chat in range(CHATS):
            chats.append(
                {
                    "id": adjust_document_id_size(f"chat-{chat}"),
                    "topic": f"Chat {chat}",
                    "chatType": "group",
                    "webUrl": f"https://teams.microsoft.com/l/chat/{chat}",
                    "createdDateTime": "2023-07-21T21:24:18.338Z",
                    "lastUpdatedDateTime": "2023-07-21T21:24:18.338Z",
                    "members": [
                        {
                            "id": f"cm-{member}-{chat}",
                            "displayName": f"Chat member {member}",
                            "userId": f"user-{member}",
                            "email": f"member{member}@example.onmicrosoft.com",
                        }
                        for member in range(MEMBERS)
                    ],
                }
            )
        return {"value": chats}

    def get_chat_messages(self, chat_id):
        messages = []
        for message in range(CHAT_MESSAGES):
            doc = _message(f"chat-message-{chat_id}-{message}", with_replies=False)
            doc["attachments"] = [
                {
                    "id": f"att-{chat_id}-{message}",
                    "name": "chat.txt",
                    "contentType": "reference",
                }
            ]
            messages.append(doc)
        return {"value": messages}

    def get_user_drive(self, user_id):
        return {"id": f"udrive-{user_id}"}

    def get_drive_children(self, drive_id, item_id):
        if item_id == "root":
            # OneDrive "Microsoft Teams Chat Files" folder lookup
            return {
                "value": [
                    {
                        "id": f"chatfolder-{drive_id}",
                        "name": "Microsoft Teams Chat Files",
                        "folder": {"childCount": 1},
                    }
                ]
            }

        if request.args.get("$filter"):
            # Chat attachment resolution by name
            return {"value": [_file(f"chatfile-{drive_id}-{item_id}", drive_id)]}

        # Channel drive children
        files = [_file(f"file-{i}-{drive_id}", drive_id) for i in range(FILES)]
        return {"value": files}

    def download_file(self, drive_id, item_id):
        return io.BytesIO(bytes(fake_provider.get_html(), encoding="utf-8"))


if __name__ == "__main__":
    MicrosoftTeamsAPI().app.run(host="0.0.0.0", port=10971)
