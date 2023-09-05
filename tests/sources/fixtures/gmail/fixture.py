#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module imitating the Google Directory and the GMail API"""
import base64
import os
import random
import string

from faker import Faker
from flask import Flask


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

fake = Faker()


def distribute_list_uniformly_across_keys(keys, input_list):
    result_dict = {key: [] for key in keys}
    num_keys = len(keys)

    for index, element in enumerate(input_list):
        # Select the key in a round-robin fashion
        key = keys[index % num_keys]
        result_dict[key].append(element)

    return result_dict


USER_IDS = ["1", "2", "3"]
USER_TO_MESSAGE_IDS = distribute_list_uniformly_across_keys(
    USER_IDS, [message_id for message_id in range(DOCS_COUNT.get(DATA_SIZE, "small"))]
)

SAMPLE_MESSAGE = {
    "id": "some id",
    "raw": generate_random_base64_string(length=100),
    "internalDate": None,
}


app = Flask(__name__)


@app.route("/gmail/v1/users/<string:user_id>/profile", methods=["GET"])
def user_profile(_user_id):
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
def message(_user_id, _message_id):
    return SAMPLE_MESSAGE


@app.route("/admin/directory/v1/users", methods=["GET"])
def users_list():
    return [{"primaryEmail": fake.email()} for _ in USER_IDS]


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
