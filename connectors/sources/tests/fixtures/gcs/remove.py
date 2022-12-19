#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Google Cloud Storage module responsible to remove blobs from the fake gcs server.
"""
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

NUMBER_OF_BLOBS_TO_BE_DELETED = 10
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


def remove_blobs():
    """Method for removing random blobs from the fake gcs server"""
    try:
        print("Started removing random blobs from the fake gcs server....")

        bucket = client_connection.bucket("sample-small-files-bucket")
        for number in range(0, NUMBER_OF_BLOBS_TO_BE_DELETED):
            blob = bucket.blob(f"sample_small_file{number}.random")
            blob.delete()

        print(
            f"Removed total {NUMBER_OF_BLOBS_TO_BE_DELETED} random blobs from the fake gcs server...."
        )
    except Exception as error:
        print(
            f"Error occurred while removing blobs from the fake gcs server. Error: {error}"
        )
        raise


if __name__ == "__main__":
    create_connection()
    remove_blobs()
