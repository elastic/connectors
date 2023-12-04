#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
"""Module to handle api calls received from connector."""

import io
import os
import time

from flask import Flask, make_response, request
from flask_limiter import HEADERS, Limiter
from flask_limiter.util import get_remote_address

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

app = Flask(__name__)


DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()

match DATA_SIZE:
    case "small":
        TOTAL_USERS = 50
        FILE_COUNT_PER_USER = 50
    case "medium":
        TOTAL_USERS = 70
        FILE_COUNT_PER_USER = 100
    case "large":
        TOTAL_USERS = 90
        FILE_COUNT_PER_USER = 150


THROTTLING = os.environ.get("THROTTLING", False)
PRE_REQUEST_SLEEP = float(os.environ.get("PRE_REQUEST_SLEEP", "0.05"))

if THROTTLING:
    limiter = Limiter(
        get_remote_address,
        app=app,
        storage_uri="memory://",
        application_limits=[
            "6000 per minute",
            "6000000 per day",
        ],  # Microsoft 50k+ licences limits
        retry_after="delta-seconds",
        headers_enabled=True,
        header_name_mapping={
            HEADERS.LIMIT: "RateLimit-Limit",
            HEADERS.RESET: "RateLimit-Reset",
            HEADERS.REMAINING: "RateLimit-Remaining",
        },
    )

    limiter.init_app(app)


TOKEN_EXPIRATION_TIMEOUT = 3699  # seconds
fake = fake_provider.fake
DRIVE_ID = fake.uuid4()
ROOT = os.environ.get("ROOT_HOST_URL", "http://127.0.0.1:10972")


class DataGenerator:
    """
    This class is used to generate fake data for OneDrive source.
    """

    def __init__(self):
        self.users = []
        self.files_per_user = {}
        self.files_by_id = {}

    def generate(self):
        # Generate users in Azure AD
        for user_id in range(1, TOTAL_USERS + 1):
            user = {
                "id": str(user_id),
                "displayName": fake.name(),
                "mail": f"user-{user_id}@onmicrosoft.com",
            }
            self.users.append(user)
            self.files_per_user[user["id"]] = []

            # Generate files and folders in the OneDrive for each user

            for file_id in range(1, FILE_COUNT_PER_USER + 1):
                item_id = f"{user_id}-{file_id}"
                item = {
                    "id": item_id,
                }

                if file_id % 11 == 0:  # Every 11th is a folder
                    item["folder"] = {"childCount": 0}
                    item["name"] = fake.word()
                    item["size"] = 0
                else:
                    file_content = fake_provider.get_html()
                    item["size"] = len(file_content.encode("UTF-8"))
                    item["file"] = {
                        "mimeType": "text/plain",
                    }
                    item["name"] = fake.file_name(extension="html")
                    self.files_by_id[item_id] = file_content

                self.files_per_user[user["id"]].append(item)

    def get_users(self, skip=0, take=100):
        results = []

        for user in self.users[skip:][:take]:
            results.append(user)

        return results

    def get_drive_items(self, user_id, skip=0, take=100):
        results = []

        for file in self.files_per_user[user_id][skip:][:take]:
            item = {
                "id": file["id"],
                "name": file["name"],
                "size": file["size"],
                "webUrl": f"{ROOT}/personal/Documents/{file['name']}",
                "createdDateTime": "2023-05-01T10:00:40Z",
                "lastModifiedDateTime": "2023-05-01T10:00:40Z",
                "parentReference": {
                    "driveType": "documentLibrary",
                    "driveId": fake.uuid4(),
                },
                "fileSystemInfo": {
                    "createdDateTime": "2023-05-01T10:00:40Z",
                    "lastModifiedDateTime": "2023-05-01T10:00:40Z",
                },
            }

            if file.get("folder"):
                item["folder"] = file["folder"]
            else:
                item["file"] = file["file"]
                item[
                    "@microsoft.graph.downloadUrl"
                ] = f"{ROOT}/users/{user_id}/drive/items/{file['id']}/content"

            results.append(item)

        return results

    def get_drive_item_content(self, user_id, item_id):
        files_list = self.files_per_user[user_id]

        for file in files_list:
            if file.get("id") == item_id:
                return self.files_by_id[item_id]


class OneDriveAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.first_sync = True
        self.data_generator = DataGenerator()

        self.data_generator.generate()

        self.app.route("/<string:tenant_id>/oauth2/v2.0/token", methods=["POST"])(
            self.get_access_token
        )
        self.app.route("/$batch", methods=["POST"])(self.batched_uris)
        self.app.route("/users", methods=["GET"])(self.get_users)
        self.app.route("/drives", methods=["GET"])(self.get_drive)
        self.app.route("/users/<string:user_id>/drive/root/delta", methods=["GET"])(
            self.get_root_drive_delta
        )
        self.app.route(
            "/users/<string:user_id>/drive/items/<string:item_id>/content",
            methods=["GET"],
        )(self.download_content)

        self.app.before_request(self.before_request)

    def before_request(self):
        time.sleep(PRE_REQUEST_SLEEP)

    def get_access_token(self, tenant_id):
        res = {
            "access_token": f"fake-access-token-for-{tenant_id}",
            "expires_in": TOKEN_EXPIRATION_TIMEOUT,
        }
        response = make_response(res)
        response.headers["status_code"] = 200
        return response

    def batched_uris(self):
        payload = request.get_json()
        response = []
        for rest_request in payload["requests"]:
            request_id = rest_request["id"]
            response.append(
                {"id": request_id, "body": self.get_root_drive_delta(request_id)}
            )
        return {"responses": response}

    def get_users(self):
        skip = int(request.args.get("$skip", 0))
        take = int(request.args.get("$take", 100))
        users = self.data_generator.get_users(skip, take)
        response = {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#users",
            "value": users,
        }

        if len(users) == take:
            response["@odata.nextLink"] = f"{ROOT}/users?$skip={skip+take}&$take={take}"

        return response

    def get_drive(self):
        return {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#drives",
            "value": {
                "createdDateTime": "2022-05-29T05:06:27Z",
                "description": str(fake.paragraph),
                "id": DRIVE_ID,
                "lastModifiedDateTime": "2022-05-29T05:06:27Z",
                "name": "Documents",
                "webUrl": f"{ROOT}/Shared%20Documents",
                "driveType": "documentLibrary",
                "createdBy": {"user": {"displayName": "System Account"}},
            },
        }

    def get_root_drive_delta(self, user_id):
        skip = int(request.args.get("$skip", 0))
        take = int(request.args.get("$take", 100))

        drive_items = self.data_generator.get_drive_items(user_id, skip, take)
        response = {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#Collection(driveItem)",
            "value": drive_items,
        }

        if len(drive_items) == take:
            response[
                "@odata.nextLink"
            ] = f"{ROOT}/users/{user_id}/drive/root/delta?$skip={skip+take}&$take={take}"

        return response

    def download_content(self, user_id, item_id):
        content = self.data_generator.get_drive_item_content(user_id, item_id)

        return io.BytesIO(bytes(content, encoding="utf-8"))


if __name__ == "__main__":
    OneDriveAPI().app.run(host="0.0.0.0", port=10972)
