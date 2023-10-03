#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Google Cloud Storage module responsible to generate blob(s) on the fake Google Cloud Storage server.
"""
import os
import random
import string

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from tests.commons import WeightedFakeProvider

client_connection = None
HERE = os.path.dirname(__file__)
HOSTS = "/etc/hosts"

fake_provider = WeightedFakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

match DATA_SIZE:
    case "small":
        FIRST_BUCKET_FILE_COUNT = 500
        SECOND_BUCKET_FILE_COUNT = 1500
    case "medium":
        FIRST_BUCKET_FILE_COUNT = 1000
        SECOND_BUCKET_FILE_COUNT = 2500
    case "large":
        FIRST_BUCKET_FILE_COUNT = 3000
        SECOND_BUCKET_FILE_COUNT = 7500

NUMBER_OF_BLOBS_TO_BE_DELETED = 10


class PrerequisiteException(Exception):
    """This class is used to generate the custom error exception when prerequisites are not satisfied."""

    def __init__(self, errors):
        super().__init__(
            f"Error while running e2e test for the Google Cloud Storage connector. \nReason: {errors}"
        )
        self.errors = errors


def get_num_docs():
    print(FIRST_BUCKET_FILE_COUNT + SECOND_BUCKET_FILE_COUNT - NUMBER_OF_BLOBS_TO_BE_DELETED)


def verify():
    "Method to verify if prerequisites are satisfied for e2e or not"
    storage_emulator_host = os.getenv(key="STORAGE_EMULATOR_HOST", default=None)
    if storage_emulator_host != "http://localhost:4443":
        raise PrerequisiteException(
            "Environment variable STORAGE_EMULATOR_HOST is not set properly.\n"
            "Solution: Please set the environment variable STORAGE_EMULATOR_HOST value with the below command on your terminal:\n"
            "export STORAGE_EMULATOR_HOST=http://localhost:4443 \n"
        )


def create_connection():
    """Method for creating connection to the fake Google Cloud Storage server"""
    try:
        global client_connection
        client_connection = storage.Client(
            credentials=AnonymousCredentials(),
            project="dummy_project_id",
        )
    except Exception as error:
        print(
            f"Error occurred while creating connection to the fake Google Cloud Storage server. Error: {error}"
        )
        raise


def generate_files(bucket_name, number_of_files):
    """Method for generating files on the fake Google Cloud Storage server"""
    client_connection.create_bucket(bucket_name)
    bucket = client_connection.bucket(bucket_name)

    for number in range(number_of_files):
        blob = bucket.blob(f"sample_file{number}.html")
        blob.upload_from_string(fake_provider.get_html())

    print(
        f"Loaded {number_of_files} files on the fake Google Cloud Storage server into bucket {bucket_name}...."
    )


def load():
    create_connection()
    print("Started loading files on the fake Google Cloud Storage server....")
    if FIRST_BUCKET_FILE_COUNT:
        generate_files("first-bucket", FIRST_BUCKET_FILE_COUNT)
    if SECOND_BUCKET_FILE_COUNT:
        generate_files("second-bucket", SECOND_BUCKET_FILE_COUNT)
    print(
        f"Loaded 2 buckets with {FIRST_BUCKET_FILE_COUNT} files and {SECOND_BUCKET_FILE_COUNT} files on the fake Google Cloud Storage server."
    )


def remove():
    """Method for removing random blobs from the fake Google Cloud Storage server"""
    create_connection()
    print(
        "Started removing random blobs from the fake Google Cloud Storage server...."
    )

    bucket = client_connection.bucket("first-bucket")
    for number in range(NUMBER_OF_BLOBS_TO_BE_DELETED):
        blob = bucket.blob(f"sample_file{number}.html")
        blob.delete()

    print(
        f"Removed total {NUMBER_OF_BLOBS_TO_BE_DELETED} random blobs from the fake Google Cloud Storage server...."
    )


def setup():
    verify()
