#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64
import os
import random
import string

from flask import Flask

from connectors.utils import iso_utc


def generate_random_base64_string(length):
    """Function that generates random base64 string with fixed length.

    Args:
        length (int): Length of generated string

    Returns:
        str: Random string
    """
    return base64.b64encode(
        ("".join([random.choice(string.ascii_letters) for _ in range(length)])).encode(
            "UTF-8"
        )
    )


DOCS_COUNT = {"small": 750, "medium": 1500, "large": 3000}

DATA_SIZE = os.environ.get("DATA_SIZE")

MESSAGE_ONE_ID = "1"
MESSAGE_TWO_ID = "2"

USER_TO_MESSAGE_IDS = {"1": [MESSAGE_ONE_ID], "2": [MESSAGE_TWO_ID]}

MESSAGE_IDS_TO_MESSAGE_CONTENT = {
    MESSAGE_ONE_ID: {
        "id": MESSAGE_ONE_ID,
        "raw": generate_random_base64_string(length=100),
        "internalDate": iso_utc(),
    },
    MESSAGE_TWO_ID: {
        "id": MESSAGE_TWO_ID,
        "raw": generate_random_base64_string(length=100),
        "internalDate": iso_utc(),
    },
}


app = Flask(__name__)


@app.route("/gmail/v1/users/<string:user_id>/profile", methods=["GET"])
def user_profile(user_id):
    return {
        "emailAddress": "some email",
        "messagesTotal": 10,
        "threadsTotal": 2,
        "historyId": "some id",
    }


@app.route("/gmail/v1/users/<string:user_id>/messages", methods=["GET"])
def messages(user_id):
    return USER_TO_MESSAGE_IDS[user_id]


@app.route(
    "/gmail/v1/users/<string:user_id>/messages/<string:message_id>", methods=["GET"]
)
def message(_, message_id):
    return MESSAGE_IDS_TO_MESSAGE_CONTENT[message_id]


@app.route("/admin/directory/v1/users", methods=["GET"])
def users_list():
    return [{"primaryEmail": "user1@test.com"}, {"primaryEmail": "user2@test.com"}]


@app.route("/token", methods=["POST"])
def post_auth_token():
    return {
        "access_token": "XXXXXXStBkRnGyZ2mUYOLgls7QVBxOg82XhBCFo8UIT5gM",
        "token_type": "Bearer",
        "expires_in": 3600,
        "refresh_token": "XXXXXX3SEBX7F2cfrHcqJEa3KoAHYeXES6nmho",
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10339)
