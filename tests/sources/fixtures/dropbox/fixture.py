#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Dropbox module responsible to generate files/folders using flask.
"""

import io
import json
import os
import random
import string

from random import choices
from faker import Faker
from functools import cached_property
from flask import Flask, jsonify, make_response, request

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

# TODO: change number of files based on DATA_SIZE
match DATA_SIZE:
    case "small":
        pass
    case "medium":
        pass
    case "large":
        pass

population = [fake_provider.small_html(), fake_provider.medium_html(), fake_provider.large_html(), fake_provider.extra_large_html()]
weights = [0.58, 0.3, 0.1, 0.02]

def get_file():
    return choices(population, weights)[0]

class DropboxAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.first_sync = True

        self.app.route("/oauth2/token", methods=["POST"])(self.get_dropbox_token)
        self.app.route("/2/users/get_current_account", methods=["POST"])(
            self.get_current_account
        )
        self.app.route("/2/files/list_folder", methods=["POST"])(self.files_list_folder)
        self.app.route("/2/files/list_folder/continue", methods=["POST"])(
            self.files_list_folder_continue
        )
        self.app.route("/2/sharing/list_received_files", methods=["POST"])(
            self.get_received_files
        )
        self.app.route("/2/sharing/list_received_files/continue", methods=["POST"])(
            self.get_received_files_continue
        )
        self.app.route("/2/sharing/get_shared_link_metadata", methods=["POST"])(
            self.get_shared_file_metadata
        )
        self.app.route("/2/sharing/get_shared_link_file", methods=["POST"])(
            self.download_shared_file
        )
        self.app.route("/2/files/download", methods=["POST"])(self.download_file)
        self.app.route("/2/files/export", methods=["POST"])(self.download_paper_file)

    def get_dropbox_token(self):
        res = {"access_token": "fake-access-token", "expires_in": 3699}
        response = make_response(res)
        response.headers["status_code"] = 200
        return response

    def get_current_account(self):
        return {
            "account_id": "dbid:1122aabb2AogwKjDAG8RkrnCee-8Zex-e94",
            "name": {
                "given_name": "Alex",
                "surname": "Wilber",
                "familiar_name": "Alex",
                "display_name": "Alex Wilber",
                "abbreviated_name": "AW",
            },
            "email": "Alex.Wilber@abc.com",
            "email_verified": True,
            "disabled": False,
            "country": "IN",
            "locale": "en",
            "referral_link": "https://www.dropbox.com/referrals/abcdeOUWL6rU9BLNtn0MtF0OmmJUnMrkHtcw?src=app9-806211",
            "is_paired": False,
            "account_type": {".tag": "basic"},
            "root_info": {
                ".tag": "user",
                "root_namespace_id": "8936575200",
                "home_namespace_id": "8936575200",
            },
        }

    def files_list_folder(self):
        response = {"entries": [], "cursor": "fake-cursor", "has_more": True}
        if self.first_sync:
            end_files_folders = 1201
            self.first_sync = False
        else:
            end_files_folders = 1001
        for entry in range(1, end_files_folders):
            folder_entry = {
                ".tag": "folder",
                "name": f"test_folder{entry}",
                "path_lower": f"/test_folder{entry}",
                "path_display": f"/test_folder{entry}",
                "id": f"id:folder{entry}",
                "shared_folder_id": "1234567890",
                "sharing_info": {
                    "read_only": False,
                    "shared_folder_id": "1234567890",
                    "traverse_only": False,
                    "no_access": False,
                },
            }
            response["entries"].append(folder_entry)

            file_entry = {
                ".tag": "file",
                "name": f"test_file{entry}.txt",
                "path_lower": f"/test_folder{entry}/test_file{entry}.html",
                "path_display": f"/test_folder{entry}/test_file{entry}.html",
                "id": f"id:file{entry}",
                "client_modified": "2023-01-01T01:01:01Z",
                "server_modified": "2023-01-01T01:01:01Z",
                "rev": "015b860f6ddc7a40000000214a950e0",
                "size": 240,
                "is_downloadable": True,
                "content_hash": "f40c1228343d7e2a632281c986dbb7af3491b9b63ddfd0eb10fee2c913f6cfa7",
            }
            response["entries"].append(file_entry)
        return jsonify(response)

    def files_list_folder_continue(self):
        response = {"entries": [], "cursor": "fake-cursor", "has_more": False}
        for entry in range(1201, 2401):
            folder_entry = {
                ".tag": "folder",
                "name": f"test_folder{entry}",
                "path_lower": f"/test_folder{entry}",
                "path_display": f"/test_folder{entry}",
                "id": f"id:folder{entry}",
                "shared_folder_id": "1234567890",
                "sharing_info": {
                    "read_only": False,
                    "shared_folder_id": "1234567890",
                    "traverse_only": False,
                    "no_access": False,
                },
            }
            response["entries"].append(folder_entry)

            file_entry = {
                ".tag": "file",
                "name": f"test_file{entry}.txt",
                "path_lower": f"/test_folder{entry}/test_file{entry}.html",
                "path_display": f"/test_folder{entry}/test_file{entry}.html",
                "id": f"id:file{entry}",
                "client_modified": "2023-01-01T01:01:01Z",
                "server_modified": "2023-01-01T01:01:01Z",
                "rev": "015b860f6ddc7a40000000214a950e0",
                "size": 240,
                "is_downloadable": True,
                "content_hash": "f40c1228343d7e2a632281c986dbb7af3491b9b63ddfd0eb10fee2c913f6cfa7",
            }
            response["entries"].append(file_entry)
        return jsonify(response)

    def get_received_files(self):
        response = {"entries": [], "cursor": "fake-cursor"}
        for entry in range(1, 101):
            response["entries"].append(
                {
                    "access_type": {".tag": "viewer"},
                    "id": f"id:shared_file{entry}",
                    "name": f"shared-file{entry}.txt",
                    "policy": {
                        "acl_update_policy": {".tag": "editors"},
                        "shared_link_policy": {".tag": "anyone"},
                        "viewer_info_policy": {".tag": "enabled"},
                    },
                    "preview_url": f"https://www.dropbox.com/scl/fi/{entry}/shared-file{entry}.html",
                    "time_invited": "2023-01-01T01:01:01Z",
                }
            )
        return jsonify(response)

    def get_received_files_continue(self):
        response = {"entries": [], "cursor": None}
        for entry in range(101, 201):
            response["entries"].append(
                {
                    "access_type": {".tag": "viewer"},
                    "id": f"id:shared_file{entry}",
                    "name": f"shared-file{entry}.txt",
                    "policy": {
                        "acl_update_policy": {".tag": "editors"},
                        "shared_link_policy": {".tag": "anyone"},
                        "viewer_info_policy": {".tag": "enabled"},
                    },
                    "preview_url": f"https://www.dropbox.com/scl/fi/{entry}/shared-file{entry}.html",
                    "time_invited": "2023-01-01T01:01:01Z",
                }
            )
        return jsonify(response)

    def get_shared_file_metadata(self):
        data = request.get_data().decode("utf-8")
        url = json.loads(data)["url"]
        res = {
            ".tag": "file",
            "url": url,
            "id": f"id:{url}",
            "name": url.split("/")[-1],
            "preview_type": "text",
            "client_modified": "2023-01-01T01:01:01Z",
            "server_modified": "2023-01-01T01:01:01Z",
            "size": 512,
            "link_permissions": {
                "resolved_visibility": {".tag": "public"},
                "requested_visibility": {".tag": "public"},
                "can_revoke": True,
                "visibility_policies": [
                    {
                        "policy": {".tag": "public"},
                        "resolved_policy": {".tag": "public"},
                        "allowed": True,
                    },
                    {
                        "policy": {".tag": "team_only"},
                        "resolved_policy": {".tag": "team_only"},
                        "allowed": False,
                        "disallowed_reason": {".tag": "user_not_on_team"},
                    },
                    {
                        "policy": {".tag": "password"},
                        "resolved_policy": {".tag": "password"},
                        "allowed": False,
                        "disallowed_reason": {".tag": "user_account_type"},
                    },
                ],
                "can_set_expiry": False,
                "can_remove_expiry": True,
                "allow_download": True,
                "can_allow_download": True,
                "can_disallow_download": False,
                "allow_comments": True,
                "team_restricts_comments": False,
                "audience_options": [
                    {"audience": {".tag": "public"}, "allowed": True},
                    {
                        "audience": {".tag": "team"},
                        "allowed": False,
                        "disallowed_reason": {".tag": "user_not_on_team"},
                    },
                    {
                        "audience": {".tag": "password"},
                        "allowed": False,
                        "disallowed_reason": {".tag": "user_account_type"},
                    },
                    {"audience": {".tag": "no_one"}, "allowed": True},
                ],
                "can_set_password": False,
                "can_remove_password": False,
            },
            "rev": "015fbe2ba5a15460000000214a950e0",
        }
        return jsonify(res)

    def download_file(self):
        return io.BytesIO(bytes(get_file(), encoding="utf-8"))

    def download_paper_file(self):
        return io.BytesIO(bytes(get_file(), encoding="utf-8"))

    def download_shared_file(self):
        return io.BytesIO(bytes(get_file(), encoding="utf-8"))


if __name__ == "__main__":
    DropboxAPI().app.run(host="0.0.0.0", port=8085)
