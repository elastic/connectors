#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
"""Module to responsible to generate confluence documents using the Flask framework.
"""
import io
import os
import time

from flask import Flask, request

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

match DATA_SIZE:
    case "small":
        SPACE_COUNT = 100
        SPACE_OBJECT_COUNT = 100
        ATTACHMENT_COUNT = 3
    case "medium":
        SPACE_COUNT = 100
        SPACE_OBJECT_COUNT = 200
        ATTACHMENT_COUNT = 5
    case "large":
        SPACE_COUNT = 100
        SPACE_OBJECT_COUNT = 250
        ATTACHMENT_COUNT = 7
    case _:
        msg = f"Unknown DATA_SIZE: {DATA_SIZE}. Expecting 'small', 'medium' or 'large'"
        raise Exception(msg)


def get_num_docs():
    # 2 is multiplier cause SPACE_OBJECTs will be delivered twice:
    # Test returns SPACE_OBJECT_COUNT objects for each type of content
    # There are 2 types of content:
    # - blogpost
    # - page
    print(SPACE_COUNT + SPACE_OBJECT_COUNT * ATTACHMENT_COUNT * 2)


class ConfluenceAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.space_start_at = 0
        self.space_page_limit = 100
        self.total_spaces = SPACE_COUNT
        self.total_content = SPACE_OBJECT_COUNT
        self.attachment_start_at = 1
        self.attachment_end_at = self.attachment_start_at + ATTACHMENT_COUNT - 1
        self.attachments = {}

        self.app.route("/rest/api/space", methods=["GET"])(self.get_spaces)
        self.app.route("/rest/api/content/<string:label_id>/label", methods=["GET"])(
            self.get_label
        )
        self.app.route("/rest/api/content/search", methods=["GET"])(self.get_content)
        self.app.route(
            "/rest/api/content/<string:content_id>/child/attachment", methods=["GET"]
        )(self.get_attachments)
        self.app.route(
            "/download/attachments/<string:content_id>/<string:attachment_id>",
            methods=["GET"],
        )(self.download)

        @self.app.before_request
        def before_request():
            time.sleep(0.05)

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

    def get_label(self, label_id):
        return {
            "results": [
                {
                    "prefix": "global",
                    "name": "label-xyz",
                    "id": f"{label_id}",
                    "label": "label-xyz",
                }
            ],
            "start": 0,
            "limit": 5,
            "size": 1,
        }

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
                    "children": {"attachment": {"size": ATTACHMENT_COUNT}},
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
            "size": ATTACHMENT_COUNT,
            "_links": {"next": None},
        }

        for attachment_count in range(self.attachment_start_at, self.attachment_end_at):
            attachment_name = f"attachment_{content_id}_{attachment_count}.html"
            attachment_file = fake_provider.get_html()
            self.attachments[attachment_name] = attachment_file
            attachment = {
                "id": f"attachment_{content_id}_{attachment_count}",
                "title": attachment_name,
                "type": "attachment",
                "version": {"when": "2023-01-03T09:24:50.633Z"},
                "extensions": {"fileSize": len(attachment_file.encode("utf-8"))},
                "_links": {
                    "download": f"/download/attachments/{content_id}/attachment_{content_id}_{attachment_count}.html",
                    "webui": f"/pages/viewpageattachments.action?pageId={content_id}&preview=attachment_{content_id}_{attachment_count}.html",
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
        data_reader = io.BytesIO(
            bytes(self.attachments[attachment_id], encoding="utf-8")
        )
        return data_reader


if __name__ == "__main__":
    ConfluenceAPI().app.run(host="0.0.0.0", port="9696")
