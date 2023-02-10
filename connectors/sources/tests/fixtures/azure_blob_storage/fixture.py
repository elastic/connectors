#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import random
import string

from azure.storage.blob import BlobServiceClient

CONTAINER = 2
LARGE_CONTAINER = 3
SMALL_CONTAINER = 7
BLOB_COUNT = 1000
CONNECTION_STRING = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"


def random_text(k=1024 * 20):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


BIG_TEXT = random_text()


def load():
    """Method for generating 10k document for azurite emulator"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            CONNECTION_STRING
        )

        for container_id in range(0, SMALL_CONTAINER):
            container_client = blob_service_client.get_container_client(
                f"containersmall{container_id}"
            )
            container_client.create_container()

            for blob_id in range(0, BLOB_COUNT):
                blob_client = container_client.get_blob_client(f"file{blob_id}.txt")
                blob_client.upload_blob(
                    f"Testing blob{blob_id} document for container{container_id}",
                    blob_type="BlockBlob",
                )

        for container_id in range(0, LARGE_CONTAINER):
            container_client = blob_service_client.get_container_client(
                f"containerlarge{container_id}"
            )
            container_client.create_container()

            for blob_id in range(0, BLOB_COUNT):
                blob_client = container_client.get_blob_client(f"file{blob_id}.txt")
                blob_client.upload_blob(
                    BIG_TEXT,
                    blob_type="BlockBlob",
                )
    except Exception as exception:
        print(f"Exception: {exception}")


def remove():
    """Method for removing 2k document for azurite emulator"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            CONNECTION_STRING
        )

        for container_id in range(0, CONTAINER):
            container_client = blob_service_client.get_container_client(
                f"containersmall{container_id}"
            )
            container_client.delete_container()
    except Exception as exception:
        print(f"Exception: {exception}")
