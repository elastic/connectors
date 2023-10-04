#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to handle api calls received from connector."""

import os
import time

from flask import Flask, request

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

match DATA_SIZE:
    case "small":
        DOCS_COUNT = 250
    case "medium":
        DOCS_COUNT = 1000
    case "large":
        DOCS_COUNT = 5000


app = Flask(__name__)

PRE_REQUEST_SLEEP = float(os.environ.get("PRE_REQUEST_SLEEP", "0.1"))


def get_num_docs():
    print(DOCS_COUNT)


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
            "name": f"file_name_{id_}",
            "fileExtension": "txt",
            "size": 12345,
            "modifiedTime": 1687860674,
            "parents": [],
        }
        for id_ in range(DOCS_COUNT)
    ]
    return {"nextPageToken": "dummyToken", "files": files_list}


@app.route("/drive/v3/files/<string:file_id>", methods=["GET"])
def files_get(file_id):
    req_params = request.args.to_dict()

    # response includes the file contents in the response body
    if req_params.get("alt", None) == "media":
        return fake_provider.get_html()

    # response includes file metadata
    return {
        "kind": "drive#file",
        "id": "file_0",
        "name": "file_name_0",
        "mimeType": "text/html",
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
