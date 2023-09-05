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

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
_SIZES = {"small": 1000000, "medium": 2000000, "large": 6000000}
FILE_SIZE = _SIZES[DATA_SIZE]
LARGE_DATA = "".join([random.choice(string.ascii_letters) for _ in range(FILE_SIZE)])


class ConfluenceAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.space_start_at = 0
        self.space_page_limit = 100
        self.total_spaces = 4000
        self.total_content = 50
        self.attachment_start_at = 1
        self.attachment_end_at = 6

        self.app.route("/rest/api/space", methods=["GET"])(self.get_spaces)
        self.app.route("/rest/api/content/search", methods=["GET"])(self.get_content)
        self.app.route(
            "/rest/api/content/<string:content_id>/child/attachment", methods=["GET"]
        )(self.get_attachments)
        self.app.route(
            "/download/attachments/<string:content_id>/<string:attachment_id>",
            methods=["GET"],
        )(self.download)

    def get_spaces(self):
        """Function to handle get spaces calls with pagination

        Returns:
            spaces (dictionary): dictionary of spaces.
        """
        args = request.args
        spaces = {
            "results": [],
            "start": 0,
            "limit": 1,
            "size": self.total_spaces,
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
            for space_count in range(self.space_start_at, self.space_page_limit):
                spaces["results"].append(
                    {
                        "id": f"space {space_count}",
                        "key": f"space{space_count}",
                        "name": f"Demo Space {space_count}",
                        "_links": {
                            "webui": f"/spaces/space{space_count}",
                        },
                    }
                )
            spaces["start"] = self.space_start_at
            spaces["limit"] = 100
            self.space_start_at = self.space_page_limit
            if self.space_page_limit < spaces["size"]:
                spaces["_links"]["next"] = "/rest/api/space?limit=100"
            elif spaces["_links"]["next"] is None:
                self.space_start_at = 0
                self.space_page_limit = 0
                self.total_spaces -= 100  # Deleting 100 spaces for the second sync
            elif self.space_page_limit >= spaces["size"]:
                spaces["_links"]["next"] = None
            self.space_page_limit = self.space_page_limit + spaces["limit"]
        return spaces

    def get_content(self):
        """Function to handle get content calls

        Returns:
            content (dictionary): dictionary of pages/blogposts.
        """
        args = request.args
        content = {
            "results": [],
            "start": 0,
            "limit": 50,
            "size": 50,
            "_links": {"next": None},
        }
        confluence_query = args.get("cql")
        document_type = confluence_query.split("type=")[1]
        for content_count in range(self.total_content):
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

    def get_attachments(self, content_id):
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

        for attachment_count in range(self.attachment_start_at, self.attachment_end_at):
            attachment = {
                "id": f"attachment_{content_id}_{attachment_count}",
                "title": f"attachment_{content_id}_{attachment_count}.py",
                "type": "attachment",
                "version": {"when": "2023-01-03T09:24:50.633Z"},
                "extensions": {"fileSize": FILE_SIZE},
                "_links": {
                    "download": f"/download/attachments/{content_id}/attachment_{content_id}_{attachment_count}.py",
                    "webui": f"/pages/viewpageattachments.action?pageId={content_id}&preview=attachment_{content_id}_{attachment_count}.py",
                },
            }
            attachments["results"].append(attachment)
        return attachments

    def download(self, content_id, attachment_id):
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
    ConfluenceAPI().app.run(host="0.0.0.0", port="9696")
