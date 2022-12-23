import os
import random
import string
import sys

import boto3

BUCKET_NAME = "ent-search-ingest-dev"
REGION_NAME = "us-west-2"
FOLDER_COUNT = 4000
SMALL_TEXT_COUNT = 5000
BIG_TEXT_COUNT = 1000
ENDPOINT_URL = "http://127.0.0.1"
OBJECT_COUNT = 15
PORT = int(os.environ.get("PORT", "5001"))


def random_text(k=0):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


BIG_TEXT = random_text(k=1024 * 20)

creds = """\
[default]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
"""


def setup():
    aws_config =  os.path.expanduser(os.path.join("~", ".aws"))
    creds = os.path.join(aws_config, "credentials")

    if not os.path.exists(creds):
        os.makedirs(aws_config, exist_ok=True)
        with open(creds, "w") as f:
            f.write(creds)


def load():
    """Method for generating 10k document for aws s3 emulator"""
    try:
        s3_client = boto3.client(
            "s3", endpoint_url=f"{ENDPOINT_URL}:{PORT}", region_name=REGION_NAME
        )
        s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={
                "LocationConstraint": REGION_NAME,
            },
        )
        print("Creating objects on the aws-moto server")
        # add folders to the bucket
        for object_id in range(0, FOLDER_COUNT):
            s3_client.put_object(
                Key=f"{BUCKET_NAME}/{object_id}/",
                Bucket=BUCKET_NAME,
                StorageClass="STANDARD",
            )
        # add small text files to the bucket
        for object_id in range(0, SMALL_TEXT_COUNT):
            s3_client.put_object(
                Key=f"{BUCKET_NAME}/small_file_{object_id}.txt",
                Bucket=BUCKET_NAME,
                Body=f"Testing object{object_id} document for bucket: {BUCKET_NAME}",
                StorageClass="STANDARD",
            )
        # add big text files to the bucket
        for object_id in range(0, BIG_TEXT_COUNT):
            s3_client.put_object(
                Key=f"{BUCKET_NAME}/big_file_{object_id}.txt",
                Bucket=BUCKET_NAME,
                Body=BIG_TEXT,
                StorageClass="STANDARD",
            )
    except Exception:
        raise


def remove():
    """Method for removing 15 random document from aws s3 emulator"""
    try:
        s3_client = boto3.client(
            "s3", endpoint_url=f"{ENDPOINT_URL}:{PORT}", region_name=REGION_NAME
        )
        print("Removing data from aws-moto server.")
        for object_id in range(0, OBJECT_COUNT):
            s3_client.delete_object(
                Bucket=BUCKET_NAME, Key=f"{BUCKET_NAME}/{object_id}/"
            )
    except Exception:
        raise
