#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to responsible to generate Zoom documents using the Flask framework."""

import os
import random
import string
from datetime import datetime, timedelta

from faker import Faker
from flask import Flask, request

override_url = os.environ.get("OVERRIDE_URL", "http://127.0.0.1:10971")
fake = Faker()

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
_SIZES = {"small": 500000, "medium": 1000000, "large": 3000000}
FILE_SIZE = _SIZES[DATA_SIZE]
DOC_COUNT = 50


class ZoomAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.total_user = 10

        self.app.route("/oauth/token", methods=["POST"])(self.get_access_token)
        self.app.route("/users", methods=["GET"])(self.get_users)
        self.app.route("/users/<string:user>/meetings", methods=["GET"])(
            self.get_meetings
        )
        self.app.route("/users/<string:user>/recordings", methods=["GET"])(
            self.get_recordings
        )
        self.app.route("/chat/users/<string:user>/channels", methods=["GET"])(
            self.get_channels
        )
        self.app.route("/chat/users/<string:user>/messages", methods=["GET"])(
            self.get_messages
        )
        self.app.route("/download", methods=["GET"])(self.get_content)

    def get_access_token(self):
        return {"access_token": "123456789", "expires_in": 3599}

    def get_users(self):
        args = request.args
        if args.get("next_page_token") == "page1":
            self.total_user = 8  # to eliminate documents belonging to 2 users in preparation for the next sync.
            res = {
                "next_page_token": None,
                "users": [],
            }
        else:
            res = {
                "next_page_token": "page1",
                "users": [
                    {"id": f"user-{user_id}", "type": "user", "name": f"user-{user_id}"}
                    for user_id in range(self.total_user)
                ],
            }
        return res

    def get_meetings(self, user):
        meeting_type = request.args.get("type")
        return {
            "next_page_token": None,
            "meetings": [
                {
                    "id": f"{user}:{meeting_type}-meeting-{meeting_id}",
                    "type": meeting_type,
                    "title": fake.text(max_nb_chars=10),
                    "user": user,
                    "created_at": "2023-03-09T00:00:00Z",
                }
                for meeting_id in range(100)
            ],
        }

    def get_recordings(self, user):
        def _format_recording_date(date):
            return date.strftime("%Y-%m-%d")

        def _format_date(date):
            return date.strftime("%Y-%m-%dT%H:%M:%SZ")

        args = request.args
        current_date = datetime.utcnow()
        if args.get("to") == _format_recording_date(date=current_date) and args.get(
            "from"
        ) == _format_recording_date(date=current_date + timedelta(days=-30)):
            res = {
                "next_page_token": None,
                "meetings": [
                    {
                        "id": f"{user}:phase1-recording-{recording_id}",
                        "type": "recording",
                        "title": fake.text(max_nb_chars=10),
                        "user": user,
                        "created_at": _format_date(current_date + timedelta(days=-15)),
                    }
                    for recording_id in range(DOC_COUNT)
                ],
            }
        elif args.get("to") == _format_recording_date(
            date=current_date + timedelta(days=-60)
        ) and args.get("from") == _format_recording_date(
            date=current_date + timedelta(days=-90)
        ):
            res = {
                "next_page_token": None,
                "meetings": [
                    {
                        "id": f"{user}:phase2-recording-{recording_id}",
                        "type": "recording",
                        "title": fake.text(max_nb_chars=10),
                        "user": user,
                        "created_at": _format_date(current_date + timedelta(days=-45)),
                    }
                    for recording_id in range(DOC_COUNT)
                ],
            }
        else:
            res = {}
        return res

    def get_channels(self, user):
        return {
            "next_page_token": None,
            "channels": [
                {
                    "id": f"{user}:channel-{channel_id}",
                    "type": "channel",
                    "title": fake.text(max_nb_chars=10),
                    "user": user,
                    "date_time": "2023-03-09T00:00:00Z",
                }
                for channel_id in range(DOC_COUNT)
            ],
        }

    def get_messages(self, user):
        message_type = request.args.get("search_type")
        if message_type == "message":
            res = {
                "next_page_token": None,
                "messages": [
                    {
                        "id": f"{user}:{message_type}-{message_id}",
                        "type": message_type,
                        "message": fake.text(max_nb_chars=20),
                        "user": user,
                        "date_time": "2023-03-09T00:00:00Z",
                    }
                    for message_id in range(DOC_COUNT)
                ],
            }
        elif message_type == "file":
            res = {
                "next_page_token": None,
                "messages": [
                    {
                        "file_id": f"{user}:{message_type}-{message_id}",
                        "type": message_type,
                        "date_time": "2023-03-09T00:00:00Z",
                        "file_size": FILE_SIZE,
                        "file_name": f"{fake.word()}.txt",
                        "download_url": f"{override_url}/download?{fake.uuid4()}",
                    }
                    for message_id in range(DOC_COUNT)
                ],
            }
        else:
            res = {}
        return res

    def get_content(self):
        return "".join([random.choice(string.ascii_letters) for _ in range(FILE_SIZE)])


if __name__ == "__main__":
    ZoomAPI().app.run(host="0.0.0.0", port=10971)
