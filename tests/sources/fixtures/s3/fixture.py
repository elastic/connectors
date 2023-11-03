#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
import os

import boto3

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

BUCKET_NAME = "ent-search-ingest-dev"
REGION_NAME = "us-west-2"
AWS_ENDPOINT_URL = "http://127.0.0.1"
AWS_PORT = int(os.environ.get("AWS_PORT", "5001"))
DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
AWS_SECRET_KEY = "dummy_secret_key"
AWS_ACCESS_KEY_ID = "dummy_access_key"

if DATA_SIZE == "small":
    FOLDER_COUNT = 50
    FILE_COUNT = 250
    OBJECT_TO_DELETE_COUNT = 5
elif DATA_SIZE == "medium":
    FOLDER_COUNT = 150
    FILE_COUNT = 1000
    OBJECT_TO_DELETE_COUNT = 10
else:
    FOLDER_COUNT = 300
    FILE_COUNT = 3000
    OBJECT_TO_DELETE_COUNT = 15

fake_provider = WeightedFakeProvider()


def get_num_docs():
    print(FOLDER_COUNT + FILE_COUNT - OBJECT_TO_DELETE_COUNT)


def setup():
    os.environ["AWS_ENDPOINT_URL"] = AWS_ENDPOINT_URL
    os.environ["AWS_PORT"] = str(AWS_PORT)


def load():
    """Method for generating 10k document for aws s3 emulator"""
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"{AWS_ENDPOINT_URL}:{AWS_PORT}",
        region_name=REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_KEY,
    )
    s3_client.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": REGION_NAME,
        },
    )
    print("Creating objects on the aws-moto server")
    # add folders to the bucket
    for object_id in range(FOLDER_COUNT):
        if object_id % 100 == 0:
            print(f"Inserted [{object_id}/{FOLDER_COUNT}] folders")
        s3_client.put_object(
            Key=f"{BUCKET_NAME}/{object_id}/",
            Bucket=BUCKET_NAME,
            StorageClass="STANDARD",
        )
    # add small text files to the bucket
    for object_id in range(FILE_COUNT):
        if object_id % 100 == 0:
            print(f"Inserted [{object_id}/{FILE_COUNT}] objects")
        s3_client.put_object(
            Key=f"{BUCKET_NAME}/small_file_{object_id}.html",
            Bucket=BUCKET_NAME,
            Body=fake_provider.get_html(),
            StorageClass="STANDARD",
        )


def remove():
    """Method for removing 15 random document from aws s3 emulator"""
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"{AWS_ENDPOINT_URL}:{AWS_PORT}",
        region_name=REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_KEY,
    )
    print("Removing data from aws-moto server.")
    for object_id in range(OBJECT_TO_DELETE_COUNT):
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=f"{BUCKET_NAME}/{object_id}/")
