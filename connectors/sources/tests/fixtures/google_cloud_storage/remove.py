#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Google Cloud Storage module responsible to remove blobs from Google Cloud Storage Mockserver.
"""
import json

NUMBER_OF_BLOBS_TO_BE_DELETED = 10


def remove_blobs():
    """Method for removing 10 random blobs from Google Cloud Storage Mockserver"""
    try:
        with open(
            r"mocked_http_responses.json",
            encoding="utf-8",
        ) as blobs_file:
            global UPDATED_DATA
            UPDATED_DATA = json.load(blobs_file)
            UPDATED_DATA[3]["httpResponse"]["body"]["items"] = UPDATED_DATA[3][
                "httpResponse"
            ]["body"]["items"][
                : len(UPDATED_DATA[3]["httpResponse"]["body"]["items"])
                - NUMBER_OF_BLOBS_TO_BE_DELETED
            ]
    except Exception as error:
        print(
            f"Error occurred while removing blobs from from Google Cloud Storage Mockserver. Error: {error}"
        )
        raise


def update_blobs():
    """Method for updating blobs data in Google Cloud Storage Mockserver"""
    try:
        with open(
            r"mocked_http_responses.json",
            "w",
            encoding="utf-8",
        ) as blobs_file:
            json.dump(UPDATED_DATA, blobs_file, indent=4)
        print(
            f"Removed total {NUMBER_OF_BLOBS_TO_BE_DELETED} number of blobs from the Google Cloud Storage Mockserver."
        )
    except Exception as error:
        print(
            f"Error occurred while updating blobs data in Google Cloud Storage Mockserver. Error: {error}"
        )
        raise


if __name__ == "__main__":
    remove_blobs()
    update_blobs()
