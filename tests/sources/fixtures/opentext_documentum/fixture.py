#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
"""Opentext Documentum module responsible to generate repositories, cabinets and files/folders using flask.
"""

import io
import os
import random

from flask import Flask, request

from connectors.utils import iso_utc
from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium")

match DATA_SIZE:
    case "small":
        REPOSITORIES = 10
        CABINETS_PER_REPOSITORY = 20
        FOLDERS = 20
        FILES = 2
    case "medium":
        REPOSITORIES = 50
        CABINETS_PER_REPOSITORY = 50
        FOLDERS = 50
        FILES = 2
    case "large":
        REPOSITORIES = 100
        CABINETS_PER_REPOSITORY = 100
        FOLDERS = 100
        FILES = 2

NUM_OF_REPOSITORIES_TO_DELETE = 5


class UniqueID:
    def __init__(self, n):
        self.numbers = list(range(n))
        random.shuffle(self.numbers)
        self.index = 0

    def get_next_number(self):
        if self.index >= len(self.numbers):
            msg = "No more unique numbers available."
            raise Exception(msg)
        number = self.numbers[self.index]
        self.index += 1
        return number


class DocumentumAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.first_sync = True

        self.app.route("/dctm-rest/repositories", methods=["GET"])(
            self.get_repositories
        )
        self.app.route("/dctm-rest/repositories/<repository_name>", methods=["GET"])(
            self.get_repository_by_name
        )
        self.app.route(
            "/dctm-rest/repositories/<string:repository_name>/cabinets", methods=["GET"]
        )(self.get_cabinets)
        self.app.route(
            "/dctm-rest/repositories/<string:repository_name>/folders", methods=["GET"]
        )(self.get_folders)
        self.app.route(
            "/dctm-rest/repositories/<string:repository_name>/folders/<folder_id>/folders",
            methods=["GET"],
        )(self.get_folders_recursively)
        self.app.route(
            "/dctm-rest/repositories/<string:repository_name>/folders/<folder_id>/documents",
            methods=["GET"],
        )(self.get_files)
        self.app.route(
            "/dctm-rest/repositories/<string:repository_name>/nodes/<string:file_id>/content",
            methods=["GET"],
        )(self.download_file)

    def get_repositories(self):
        args = request.args
        items_per_page = int(args.get("items-per-page", 100))
        page = int(args.get("page", 0))

        if self.first_sync:
            total = REPOSITORIES
        else:
            total = REPOSITORIES - NUM_OF_REPOSITORIES_TO_DELETE
        unique_id = UniqueID(REPOSITORIES)

        response = {
            "id": "http://127.0.0.1:2099/repositories",
            "title": "Repositories",
            "author": [{"name": "Alex Wilber"}],
            "updated": "2018-09-28T14:41:29.686+00:00",
            "page": page,
            "items-per-page": items_per_page,
            "total": total,
            "links": [{"rel": "self", "href": "http://127.0.0.1:2099/repositories"}],
            "entries": [],
        }

        if page * items_per_page < total:
            docs_per_page = total if total < items_per_page else items_per_page
            for _ in range(1, docs_per_page + 1):
                _id = unique_id.get_next_number()
                response["entries"].append(
                    {
                        "id": _id,
                        "title": f"Repo_{_id}",
                        "summary": f"Repository Summary for Repo_{_id}",
                        "updated": iso_utc(),
                        "published": "2018-09-28T14:41:29.738+00:00",
                        "links": [
                            {
                                "rel": "edit",
                                "href": f"http://127.0.0.1:2099/repositories/{_id}",
                            }
                        ],
                        "content": {
                            "type": "application/vnd.emc.documentum+json",
                            "src": f"http://127.0.0.1:2099/repositories/{_id}",
                        },
                    }
                )
            return response
        return response

    def get_repository_by_name(self, repository_name):
        unique_id = UniqueID(REPOSITORIES)
        _id = unique_id.get_next_number()

        return {
            "id": _id,
            "title": repository_name,
            "summary": f"Repository Summary for {repository_name}",
            "updated": iso_utc(),
            "published": "2018-09-28T14:41:29.738+00:00",
            "links": [
                {
                    "rel": "edit",
                    "href": f"http://127.0.0.1:2099/repositories/{_id}",
                }
            ],
            "content": {
                "type": "application/vnd.emc.documentum+json",
                "src": f"http://127.0.0.1:2099/repositories/{_id}",
            },
        }

    def get_cabinets(self, repository_name):
        args = request.args
        items_per_page = int(args.get("items-per-page", 100))
        page = int(args.get("page", 0))

        total_cabinets = CABINETS_PER_REPOSITORY
        unique_id = UniqueID(REPOSITORIES)

        response = {
            "id": f"http://127.0.0.1:2099/repositories/{repository_name}/cabinets",
            "title": "Cabinets",
            "author": [{"name": "Alex Wilber"}],
            "updated": "2018-09-28T14:41:29.686+00:00",
            "page": page,
            "items-per-page": items_per_page,
            "total": total_cabinets,
            "links": [
                {
                    "rel": "self",
                    "href": f"http://127.0.0.1:2099/repositories/{repository_name}/cabinets",
                }
            ],
            "entries": [],
        }

        if page * items_per_page < total_cabinets:
            docs_per_page = (
                total_cabinets if total_cabinets < items_per_page else items_per_page
            )
            for _ in range(1, docs_per_page + 1):
                _id = unique_id.get_next_number()
                response["entries"].append(
                    {
                        "id": _id,
                        "title": f"Cabinet_{_id}",
                        "type": "cabinet",
                        "updated": iso_utc(),
                        "definition": "Human Resources Cabinet",
                        "properties": [
                            {
                                "name": f"Cabinet_{_id}",
                                "description": "Cabinet containing HR documents",
                            }
                        ],
                    }
                )
            return response
        return response

    def get_folders(self, repository_name):
        args = request.args
        items_per_page = int(args.get("items-per-page", 100))
        page = int(args.get("page", 0))

        total = FOLDERS
        unique_id = UniqueID(REPOSITORIES)

        response = {
            "id": "http://127.0.0.1:2099/folders",
            "title": "Folders",
            "author": [{"name": "Alex Wilber"}],
            "updated": "2018-09-28T14:41:29.686+00:00",
            "page": page,
            "items-per-page": items_per_page,
            "total": total,
            "links": [{"rel": "self", "href": "http://127.0.0.1:2099/folders"}],
            "entries": [],
        }

        if page * items_per_page < total:
            docs_per_page = total if total < items_per_page else items_per_page
            for _ in range(1, docs_per_page + 1):
                _id = unique_id.get_next_number()
                response["entries"].append(
                    {
                        "id": _id,
                        "title": f"folder_{_id}",
                        "updated": iso_utc(),
                        "created": "2018-09-28T14:41:29.686+00:00",
                        "parent_id": None,
                    }
                )
            return response
        return response

    def get_folders_recursively(self, repository_name, folder_id):
        args = request.args
        items_per_page = int(args.get("items-per-page", 100))
        page = int(args.get("page", 0))

        total = 1  # All folders contains 1 sub-folder
        unique_id = UniqueID(REPOSITORIES)

        response = {
            "id": f"http://127.0.0.1:2099/folders/{folder_id}/folders",
            "title": "Folders",
            "author": [{"name": "Alex Wilber"}],
            "updated": "2018-09-28T14:41:29.686+00:00",
            "page": page,
            "items-per-page": items_per_page,
            "total": total,
            "links": [
                {
                    "rel": "self",
                    "href": f"http://127.0.0.1:2099/folders/{folder_id}/folders",
                }
            ],
            "entries": [],
        }

        if page * items_per_page < total:
            docs_per_page = total if total < items_per_page else items_per_page
            for _ in range(1, docs_per_page + 1):
                _id = unique_id.get_next_number()
                response["entries"].append(
                    {
                        "id": _id,
                        "title": f"folder_{_id}",
                        "updated": iso_utc(),
                        "created": "2018-09-28T14:41:29.686+00:00",
                        "parent_id": folder_id,
                    }
                )
            return response
        return response

    def get_files(self, repository_name, folder_id):
        args = request.args
        items_per_page = int(args.get("items-per-page", 100))
        page = int(args.get("page", 0))

        total = FILES
        unique_id = UniqueID(REPOSITORIES)

        response = {
            "id": f"http://127.0.0.1:2099/folders/{folder_id}/documents",
            "title": "Folders",
            "author": [{"name": "Alex Wilber"}],
            "updated": "2018-09-28T14:41:29.686+00:00",
            "page": page,
            "items-per-page": items_per_page,
            "total": total,
            "links": [
                {
                    "rel": "self",
                    "href": f"http://127.0.0.1:2099/folders/{folder_id}/documents",
                }
            ],
            "entries": [],
        }

        if page * items_per_page < total:
            docs_per_page = total if total < items_per_page else items_per_page
            for _ in range(1, docs_per_page + 1):
                _id = unique_id.get_next_number()
                response["entries"].append(
                    {
                        "id": _id,
                        "title": f"document_{_id}.txt",
                        "size": 256,
                        "updated": iso_utc(),
                        "created": "2018-09-28T14:41:29.686+00:00",
                        "parent_id": folder_id,
                        "content": {
                            "src": f"http://127.0.0.1:2099/dctm-rest/repositories/{repository_name}/nodes/document_{_id}/content"
                        },
                    }
                )
            return response
        return response

    def download_file(self, repository_name, file_id):
        return io.BytesIO(bytes(fake_provider.get_html(), encoding="utf-8"))


if __name__ == "__main__":
    DocumentumAPI().app.run(host="0.0.0.0", port=2099)
