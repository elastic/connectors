#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to responsible to generate confluence documents using the Flask framework.
"""
import io
import os
import random
import string

from flask import Flask, request

app = Flask(__name__)
space_start_at, space_page_limit, total_spaces = 0, 100, 4000
DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
_SIZES = {"small": 1000000, "medium": 2000000, "large": 6000000}
FILE_SIZE = _SIZES[DATA_SIZE]
LARGE_DATA = "".join([random.choice(string.ascii_letters) for _ in range(FILE_SIZE)])


@app.route("/rest/api/space", methods=["GET"])
def get_spaces():
    """Function to handle get spaces calls with pagination

    Returns:
        spaces (dictionary): dictionary of spaces.
    """
    args = request.args
    global space_start_at, space_page_limit, total_spaces
    spaces = {
        "results": [],
        "start": 0,
        "limit": 1,
        "size": total_spaces,
        "_links": {"next": None},
    }
    if args["limit"] == "1":
        spaces["results"].append(
            {
                "id": "space 0",
                "name": "Demo Space 0",
                "_links": {
                    "webui": "/spaces/space0",
                },
            }
        )
    else:
        for space_count in range(space_start_at, space_page_limit):
            spaces["results"].append(
                {
                    "id": f"space {space_count}",
                    "name": f"Demo Space {space_count}",
                    "_links": {
                        "webui": f"/spaces/space{space_count}",
                    },
                }
            )
        spaces["start"] = space_start_at
        spaces["limit"] = 100
        space_start_at = space_page_limit
        if space_page_limit < spaces["size"]:
            spaces["_links"]["next"] = "/rest/api/space?limit=100"
        elif spaces["_links"]["next"] is None:
            space_start_at = 0
            space_page_limit = 0
            total_spaces -= 100  # Deleting 100 spaces for the second sync
        elif space_page_limit >= spaces["size"]:
            spaces["_links"]["next"] = None
        space_page_limit = space_page_limit + spaces["limit"]
    return spaces


@app.route("/rest/api/content", methods=["GET"])
def get_content():
    """Function to handle get content calls

    Returns:
        content (dictionary): dictionary of pages/blogposts.
    """
    args = request.args
    content = {
        "results": [],
        "start": 0,
        "limit": 100,
        "size": 100,
        "_links": {"next": None},
    }
    document_type = args.get("type", "page")
    for content_count in range(100):
        content["results"].append(
            {
                "id": f"{document_type}_{content_count}",
                "title": f"ES-scrum_{content_count}",
                "type": document_type,
                "history": {"lastUpdated": {"when": "2023-01-24T04:07:19.672Z"}},
                "children": {"attachment": {"size": 5}},
                "body": {"storage": {"value": f"This is a test {document_type}"}},
                "space": {"name": "Demo Space 0"},
                "_links": {
                    "webui": f"/spaces/space0/{document_type}/{document_type}_{content_count}/ES-scrum_{content_count}",
                },
            }
        )
    return content


@app.route("/rest/api/content/<string:id>/child/attachment", methods=["GET"])
def get_attachments(id):
    """Function to handle get attachments calls

    Args:
        id (string): id of a content.

    Returns:
        attachments (dictionary): dictionary of attachments.
    """
    attachments = {
        "results": [],
        "start": 0,
        "limit": 100,
        "size": 5,
        "_links": {"next": None},
    }

    for attachment_count in range(1, 6):
        attachment = {
            "id": f"attachment_{id}_{attachment_count}",
            "title": f"attachment_{id}_{attachment_count}.py",
            "type": "attachment",
            "version": {"when": "2023-01-03T09:24:50.633Z"},
            "extensions": {"fileSize": FILE_SIZE},
            "_links": {
                "download": f"/download/attachments/{id}/attachment_{id}_{attachment_count}.py",
                "webui": f"/pages/viewpageattachments.action?pageId={id}&preview=attachment_{id}_{attachment_count}.py",
            },
        }
        attachments["results"].append(attachment)
    return attachments


@app.route(
    "/download/attachments/<string:content_id>/<string:attachment_id>", methods=["GET"]
)
def download(content_id, attachment_id):
    """Function to handle download calls for attachments

    Args:
        content_id (string): id of a content.
        attachment_id (string): id of a attachment.

    Returns:
        data_reader (io.BytesIO): object of io.BytesIO.
    """
    data_reader = io.BytesIO(bytes(LARGE_DATA, encoding="utf-8"))
    return data_reader


if __name__ == "__main__":
    app.run(host="0.0.0.0")
