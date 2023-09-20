#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to handle api calls received from connector."""
import io
import os
import random
import string

from faker import Faker
from flask import Flask, make_response, request

app = Flask(__name__)


DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()
_SIZES = {"small": 500000, "medium": 1000000, "large": 3000000}
FILE_SIZE = _SIZES[DATA_SIZE]
LARGE_DATA = "".join([random.choice(string.ascii_letters) for _ in range(FILE_SIZE)])
fake = Faker()


def _create_data(size):
    return "".join([random.choice(string.ascii_letters) for _ in range(size)])


class BoxAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.first_sync = True
        self.file_count = 5000
        self.app.route("/oauth2/token", methods=["POST"])(self.get_token)
        self.app.route("/2.0/users/me", methods=["GET"])(self.get_user)
        self.app.route(
            "/2.0/folders/<string:folder_id>/items",
            methods=["GET"],
        )(self.get_folder_items)
        self.app.route("/2.0/files/<file_id>/content", methods=["GET"])(
            self.get_content
        )

    def get_token(self):
        fake_res = {
            "access_token": "FAKE-ACCESS-TOKEN",
            "refresh_token": "FAKE-REFRESH-TOKEN",
            "expires_in": 3600,
        }
        response = make_response(fake_res)
        response.headers["status_code"] = 200
        return response

    def get_user(self):
        if self.first_sync:
            self.first_sync = False
        else:
            self.file_count = 3500
        return {"username": "demo_user"}

    def get_entries(self, offset, limit):
        if offset >= self.file_count:
            return []
        entries = [
            {
                "type": "file",
                "id": f"file_number_{file_count}",
                "etag": "0",
                "name": f"{fake.name()}.txt",
                "modified_at": "2023-08-04T03:17:55-07:00",
                "size": FILE_SIZE,
            }
            for file_count in range(offset, min(offset + limit, self.file_count))
        ]
        return entries

    def get_folder_items(self, folder_id):
        offset = int(request.args.get("offset"))
        limit = int(request.args.get("limit"))
        entries = self.get_entries(offset, limit)
        response = {
            "total_count": self.file_count,
            "entries": entries,
            "offset": offset,
            "limit": limit,
            "order": [
                {"by": "type", "direction": "ASC"},
                {"by": "name", "direction": "ASC"},
            ],
        }
        return response

    def get_content(self, file_id):
        return io.BytesIO(bytes(LARGE_DATA, encoding="utf-8"))


if __name__ == "__main__":
    BoxAPI().app.run(host="0.0.0.0", port=9092)
