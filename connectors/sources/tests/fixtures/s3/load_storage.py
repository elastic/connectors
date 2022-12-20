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


def random_text(k=0):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


BIG_TEXT = random_text(k=1024 * 20)


def main():
    """Method for generating 10k document for aws s3 emulator"""
    try:
        try:
            PORT = sys.argv[1]
        except IndexError:
            PORT = 5000
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


if __name__ == "__main__":
    main()
