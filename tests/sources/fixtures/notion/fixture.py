#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
"""Module to handle api calls received from connector."""
import os

from flask import Flask, request

from tests.commons import WeightedFakeProvider

app = Flask(__name__)


DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()
_SIZES = {"small": 5, "medium": 10, "large": 15}
NUMBER_OF_DATABASES_PAGES = _SIZES[DATA_SIZE]

fake_provider = WeightedFakeProvider()


class NotionAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.first_sync = True
        self.app.route("/v1/users/me", methods=["GET"])(self.get_owner)
        self.app.route("/v1/users", methods=["GET"])(self.get_users)
        self.app.route("/v1/search", methods=["POST"])(self.search_by_query)
        self.app.route("/v1/blocks/<string:block_id>/children", methods=["GET"])(
            self.get_block_children
        )

    def get_owner(self):
        return {
            "object": "user",
            "id": "user_id",
            "name": "Alex Wilber",
            "type": "bot",
            "bot": {
                "owner": {"type": "workspace", "workspace": True},
                "workspace_name": "Alex's Workspace",
            },
        }

    def get_users(self):
        users = {
            "object": "list",
            "results": [
                {
                    "object": "user",
                    "id": f"id_{user_id}",
                    "name": f"user_{user_id}",
                    "avatar_url": "https://test.avatar.com/a/#123",
                    "type": "person",
                    "person": {"email": f"user_{user_id}@test.com"},
                }
                for user_id in range(100)
            ],
            "next_cursor": None,
            "has_more": False,
            "type": "user",
            "user": {},
            "developer_survey": "https://notionup.typeform.com/to/bllBsoI4?utm_source=postman",
            "request_id": "2ba1075b-4b0d-4436-a3c4-ff873449ded4",
        }
        return users

    def get_block_children(self, block_id):
        has_start_cursor = request.args.get("start_cursor")
        if has_start_cursor:
            response = {
                "object": "list",
                "next_cursor": None,
                "has_more": False,
                "type": "block",
                "request_id": "fake-request-id",
            }
            start_range = 101
            end_range = 201
        else:
            response = {
                "object": "list",
                "next_cursor": "fake_next_cursor",
                "has_more": True,
                "type": "block",
                "request_id": "fake-request-id",
            }
            start_range = 1
            end_range = 101

        response["results"] = [
            {
                "object": "block",
                "id": f"{block_id}_{i}",
                "parent": {"id": block_id},
                "created_time": "2024-01-01T01:01:01.000Z",
                "last_edited_time": "2024-01-01T01:01:01.000Z",
                "has_children": False,
                "type": "to_do",
                "to_do": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": fake_provider.get_text()},
                            "plain_text": fake_provider.get_text(),
                        }
                    ]
                },
            }
            for i in range(start_range, end_range)
        ]
        return response

    def get_page_database(self):
        if self.first_sync:
            total_doc = NUMBER_OF_DATABASES_PAGES
            self.first_sync = False
        else:
            total_doc = NUMBER_OF_DATABASES_PAGES - 1
        databases_pages = [
            {
                "object": "database",
                "id": f"database_{database_id}",
                "cover": None,
                "icon": None,
                "created_time": "2024-01-31T08:14:00.000Z",
                "created_by": {
                    "object": "user",
                    "id": "user_2",
                },
                "last_edited_by": {
                    "object": "user",
                    "id": "user_2",
                },
                "last_edited_time": "2024-01-31T08:15:00.000Z",
                "title": [
                    {
                        "type": "text",
                        "text": {"content": fake_provider.get_text(), "link": None},
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default",
                        },
                        "plain_text": fake_provider.get_text(),
                        "href": None,
                    }
                ],
                "description": [],
                "is_inline": False,
                "properties": {
                    "Tags": {
                        "id": "id#1",
                        "name": "Tags",
                        "type": "multi_select",
                        "multi_select": {"options": []},
                    },
                    "Created": {
                        "id": "id#2",
                        "name": "Created",
                        "type": "created_time",
                        "created_time": {},
                    },
                    "Name": {
                        "id": "title",
                        "name": "Name",
                        "type": "title",
                        "title": {},
                    },
                },
                "parent": {"type": "workspace", "workspace": True},
                "url": "https://www.notion-test.so/abcd#123",
                "public_url": None,
                "archived": False,
            }
            for database_id in range(total_doc)
        ]

        pages = [
            {
                "object": "page",
                "id": f"page_{page_id}",
                "created_time": "2024-01-15T13:42:00.000Z",
                "last_edited_time": "2024-02-08T10:20:00.000Z",
                "created_by": {
                    "object": "user",
                    "id": "user_1",
                },
                "last_edited_by": {
                    "object": "user",
                    "id": "user_1",
                },
                "cover": None,
                "icon": None,
                "parent": {"type": "workspace", "workspace": True},
                "archived": False,
                "properties": {
                    "title": {
                        "id": "title",
                        "type": "title",
                        "title": [
                            {
                                "type": "text",
                                "text": {
                                    "content": fake_provider.get_text(),
                                    "link": None,
                                },
                                "annotations": {
                                    "bold": False,
                                    "italic": False,
                                    "strikethrough": False,
                                    "underline": False,
                                    "code": False,
                                    "color": "default",
                                },
                                "plain_text": fake_provider.get_text(),
                                "href": None,
                            }
                        ],
                    }
                },
                "url": "https://www.notion-test.so/abcd#1234",
                "public_url": None,
            }
            for page_id in range(total_doc)
        ]

        databases_pages.extend(pages)
        return databases_pages

    def search_by_query(self):
        return {
            "object": "list",
            "results": self.get_page_database(),
            "next_cursor": None,
            "has_more": False,
            "type": "page_or_database",
            "page_or_database": {},
            "developer_survey": "https://notionup.typeform.com/to/bllBsoI4?utm_source=postman",
            "request_id": "15d3cb08-e7bd-4ae7-ad43-df41c5b767e8",
        }


if __name__ == "__main__":
    NotionAPI().app.run(host="0.0.0.0", port=9096)
