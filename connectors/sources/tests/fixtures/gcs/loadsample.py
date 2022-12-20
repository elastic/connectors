#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Google Cloud Storage module responsible to generate blob(s) on the fake gcs server.
"""
import random
import string

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

NUMBER_OF_SMALL_FILES = 9500
NUMBER_OF_LARGE_FILES = 500
client_connection = None


def create_connection():
    """Method for creating connection to the fake gcs server"""
    try:
        global client_connection
        client_connection = storage.Client(
            credentials=AnonymousCredentials(),
            project="dummy_project_id",
        )
    except Exception as error:
        print(
            f"Error occurred while creating connection to the fake gcs server. Error: {error}"
        )
        raise


def create_buckets():
    """Method for generating buckets on the fake gcs server"""
    try:
        print("Started loading 2 buckets on the fake gcs server....")

        client_connection.create_bucket("sample-small-files-bucket")
        client_connection.create_bucket("sample-large-files-bucket")

        print("Loaded total 2 buckets on the fake gcs server....")
    except Exception as error:
        print(
            f"Error occurred while creating buckets on the fake gcs server. Error: {error}"
        )


def generate_small_files(start_number_of_small_files, end_number_of_small_files):
    """Method for generating small files on the fake gcs server"""
    try:
        print("Started loading small files on the fake gcs server....")
        bucket = client_connection.bucket("sample-small-files-bucket")

        for number in range(start_number_of_small_files, end_number_of_small_files):
            blob = bucket.blob(f"sample_small_file{number}.random")
            blob.upload_from_string("")

        print(
            f"Loaded {end_number_of_small_files-start_number_of_small_files} small files on the fake gcs server...."
        )
    except Exception as error:
        print(
            f"Error occurred while generating small files on the fake gcs server. Error: {error}"
        )
        raise


def generate_large_files(start_number_of_large_files, end_number_of_large_files):
    """Method for generating large files on the fake gcs server"""
    try:
        size_of_file = 2097152  # 2 Mb of text
        large_data = "".join(
            [random.choice(string.ascii_letters) for i in range(size_of_file)]
        )

        print("Started loading large files on the fake gcs server....")

        bucket = client_connection.bucket("sample-large-files-bucket")

        for number in range(start_number_of_large_files, end_number_of_large_files):
            blob = bucket.blob(f"sample_large_file{number}.txt")
            blob.upload_from_string(large_data)

        print(
            f"Loaded {end_number_of_large_files-start_number_of_large_files} large files on the fake gcs server...."
        )
    except Exception as error:
        print(
            f"Error occurred while generating large files on the fake gcs server. Error: {error}"
        )
        raise


if __name__ == "__main__":
    create_connection()
    create_buckets()
    if NUMBER_OF_LARGE_FILES:
        generate_large_files(0, NUMBER_OF_LARGE_FILES)
    if NUMBER_OF_SMALL_FILES:
        generate_small_files(0, NUMBER_OF_SMALL_FILES)
    print(
        f"Loaded 2 buckets with {NUMBER_OF_LARGE_FILES} large files and {NUMBER_OF_SMALL_FILES} small files on the fake gcs server."
    )
