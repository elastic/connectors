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

from flask import Flask, request

DOCS_COUNT = {"small": 750, "medium": 1500, "large": 3000}

DATA_SIZE = os.environ.get("DATA_SIZE")


def generate_random_string(length):
    """Function that generates random string with fixed lenght.

    Args:
        length (int): Length of generated string

    Returns:
        str: Random string
    """
    return "".join([random.choice(string.ascii_letters) for _ in range(length)])


def generate_document_data():
    """Function to generate random data content.

    Returns:
        io.BytesIO: Dummy attachment content
    """
    # 1KB text file
    file_content = generate_random_string(1000)
    return io.BytesIO(bytes(file_content, encoding="utf-8"))


app = Flask(__name__)


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
            "id": generate_random_string(length=16),
            "name": f"file_name_{id}",
            "fileExtension": "txt",
            "size": 12345,
            "modifiedTime": 1687860674,
            "parents": [],
        }
        for id in range(DOCS_COUNT.get(DATA_SIZE, "small"))
    ]
    return {"nextPageToken": "dummyToken", "files": files_list}


@app.route("/drive/v3/files/<string:file_id>", methods=["GET"])
def files_get(file_id):
    req_params = request.args.to_dict()

    # response includes the file contents in the response body
    if req_params.get("alt", None) == "media":
        return generate_document_data()
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
