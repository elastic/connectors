#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import random
import string

from azure.storage.blob import BlobServiceClient
from tests.commons import WeightedFakeProvider

CONTAINER = 2
LARGE_CONTAINER = 3
SMALL_CONTAINER = 7
BLOB_COUNT = 1000
CONNECTION_STRING = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"



fake_provider = WeightedFakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium")

CONTAINERS_TO_DELETE = 1

match DATA_SIZE:
    case "small":
        CONTAINER_COUNT = 4
        BLOB_COUNT = 50
    case "medium":
        CONTAINER_COUNT = 6
        BLOB_COUNT = 200
    case "large":
        CONTAINER_COUNT = 10
        BLOB_COUNT = 1000

def get_num_docs():
    print((CONTAINER_COUNT - CONTAINERS_TO_DELETE) * BLOB_COUNT)

def load():
    """Method for generating documents for azurite emulator"""
    blob_service_client = BlobServiceClient.from_connection_string(
        CONNECTION_STRING
    )

    for container_id in range(0, CONTAINER_COUNT):
        container_client = blob_service_client.get_container_client(
            f"container{container_id}"
        )
        container_client.create_container()

        for blob_id in range(0, BLOB_COUNT):
            blob_client = container_client.get_blob_client(f"file{blob_id}.html")
            blob_client.upload_blob(
                fake_provider.get_html(),
                blob_type="BlockBlob",
            )


def remove():
    """Method for removing document for azurite emulator"""
    blob_service_client = BlobServiceClient.from_connection_string(
        CONNECTION_STRING
    )

    for container_id in range(0, CONTAINER):
        container_client = blob_service_client.get_container_client(
            f"containersmall{container_id}"
        )
        container_client.delete_container()
