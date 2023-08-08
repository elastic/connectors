#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to handle api calls received from connector."""

import io
import os
import time
from random import choices
from faker import Faker
from functools import cached_property
import string

from flask import Flask, request

# TODO: make used generally
class FakeProvider:
    def __init__(self, seed=None):
        self.seed = seed
        self.fake = Faker()
        if seed:
            self.fake.seed_instance(seed)

    @cached_property
    def _cached_random_str(self):
        return self.fake.pystr(min_chars=100 * 1024, max_chars=100 * 1024 + 1)

    def small_text(self):
        # Up to 1KB of text
        return self.generate_text(1 * 1024)

    def medium_text(self):
        # Up to 1MB of text
        return self.generate_text(1024 * 1024)

    def large_text(self):
        # Up to 4MB of text
        return self.generate_text(4 * 1024 * 1024)

    def extra_large_text(self):
        return self.generate_text(20 * 1024 * 1024)

    def small_html(self):
        # Around 100KB
        return self.generate_html(1)

    def medium_html(self):
        # Around 1MB
        return self.generate_html(1 * 10)

    def large_html(self):
        # Around 8MB
        return self.generate_html(8 * 10)

    def extra_large_html(self):
        # Around 25MB
        return self.generate_html(25 * 10)

    def generate_text(self, max_size):
        return self.fake.text(max_nb_chars=max_size)

    def generate_html(self, images_of_100kb):
        img = self._cached_random_str  # 100kb
        text = self.small_text()

        images = []
        for _ in range(images_of_100kb):
            images.append(f"<img src='{img}'/>")

        return f"<html><head></head><body><div>{text}</div><div>{'<br/>'.join(images)}</div></body></html>"

fake_provider = FakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium")

match DATA_SIZE:
    case "small":
        DOCS_COUNT = 250
    case "medium":
        DOCS_COUNT = 1000
    case "large":
        DOCS_COUNT = 5000

population = [fake_provider.small_html(), fake_provider.medium_html(), fake_provider.large_html(), fake_provider.extra_large_html()]
weights = [0.58, 0.3, 0.1, 0.02]

def get_file():
    return choices(population, weights)[0]


app = Flask(__name__)


PRE_REQUEST_SLEEP = float(os.environ.get("PRE_REQUEST_SLEEP", "0.05"))

@app.before_request
def before_request():
    time.sleep(PRE_REQUEST_SLEEP)


@app.route("/drive/v3/about", methods=["GET"])
def about_get():
    return {"kind": "drive#about"}


@app.route("/drive/v3/drives", methods=["GET"])
def drives_list():
    return {
        "nextPageToken": "dummyToken",
        "kind": "drive#driveList",
        "drives": [
            {"id": "id1", "name": "Drive1 [Internal]", "kind": "drive#drive"},
            {"id": "id2", "name": "Drive2 [Internal]", "kind": "drive#drive"},
        ],
    }


@app.route("/drive/v3/files", methods=["GET"])
def files_list():
    files_list = [
        {
            "kind": "drive#file",
            "mimeType": "text/plain",
            "id": fake_provider.fake.sha1(),
            "name": f"file_name_{id}",
            "fileExtension": "txt",
            "size": 12345,
            "modifiedTime": 1687860674,
            "parents": [],
        }
        for id in range(DOCS_COUNT)
    ]
    return {"nextPageToken": None, "files": files_list}


@app.route("/drive/v3/files/<string:file_id>", methods=["GET"])
def files_get(file_id):
    req_params = request.args.to_dict()

    # response includes the file contents in the response body
    if req_params.get("alt", None) == "media":
        return io.BytesIO(bytes(get_file(), encoding="utf-8"))
    # response includes file metadata
    else:
        return {
            "kind": "drive#file",
            "id": "file_0",
            "name": "file_name_0",
            "mimeType": "text/plain",
        }


@app.route("/token", methods=["POST"])
def post_auth_token():
    """Function to load"""
    return {
        "access_token": "XXXXXXStBkRnGyZ2mUYOLgls7QVBxOg82XhBCFo8UIT5gM",
        "token_type": "Bearer",
        "expires_in": 3600,
        "refresh_token": "XXXXXX3SEBX7F2cfrHcqJEa3KoAHYeXES6nmho",
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10339)
