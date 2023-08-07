#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to responsible to generate confluence documents using the Flask framework.
"""
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

population = [fake_provider.small_html(), fake_provider.medium_html(), fake_provider.large_html(), fake_provider.extra_large_html()]
weights = [0.58, 0.3, 0.1, 0.02]

def get_file():
    return choices(population, weights)[0]

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
        self.app.route("/rest/api/content/search", methods=["GET"])(self.get_content)
        self.app.route(
            "/rest/api/content/<string:id>/child/attachment", methods=["GET"]
        )(self.get_attachments)
        self.app.route(
            "/download/attachments/<string:content_id>/<string:attachment_id>",
            methods=["GET"],
        )(self.download)

        PRE_REQUEST_SLEEP = float(os.environ.get("PRE_REQUEST_SLEEP", "0.05"))

        @self.app.before_request
        def before_request():
            time.sleep(PRE_REQUEST_SLEEP)

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
                    "children": {"attachment": {"size": ATTACHMENT_COUNT}},
                    "body": {"storage": {"value": f"This is a test {document_type}"}},
                    "space": {"name": "Demo Space 0"},
                    "_links": {
                        "webui": f"/spaces/space0/{document_type}/{document_type}_{content_count}/ES-scrum_{content_count}",
                    },
                }
            )
        return content

    def get_attachments(self, id):
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
            file = get_file()
            attachment_id = f"attachment_{id}_{attachment_count}"
            self.attachments[attachment_id] = file
            attachment = {
                "id": attachment_id,
                "title": f"attachment_{id}_{attachment_count}.html",
                "type": "attachment",
                "version": {"when": "2023-01-03T09:24:50.633Z"},
                "extensions": {"fileSize": len(file.encode("utf-8"))},
                "_links": {
                    "download": f"/download/attachments/{id}/attachment_{id}_{attachment_count}",
                    "webui": f"/pages/viewpageattachments.action?pageId={id}&preview=attachment_{id}_{attachment_count}",
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
        data_reader = io.BytesIO(bytes(self.attachments[attachment_id], encoding="utf-8"))
        return data_reader


if __name__ == "__main__":
    ConfluenceAPI().app.run(host="0.0.0.0", port="9696")
