import os
import random
import string

BUCKET_NAME = "ent-search-ingest-dev"
REGION_NAME = "us-west-2"
AWS_ENDPOINT_URL = "http://127.0.0.1"
AWS_PORT = int(os.environ.get("AWS_PORT", "5001"))
DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
AWS_SECRET_KEY = "dummy_secret_key"
AWS_ACCESS_KEY_ID = "dummy_access_key"

if DATA_SIZE == "small":
    FOLDER_COUNT = 400
    SMALL_TEXT_COUNT = 500
    BIG_TEXT_COUNT = 100
    OBJECT_COUNT = 5
elif DATA_SIZE == "medium":
    FOLDER_COUNT = 2000
    SMALL_TEXT_COUNT = 2500
    BIG_TEXT_COUNT = 500
    OBJECT_COUNT = 10
else:
    FOLDER_COUNT = 4000
    SMALL_TEXT_COUNT = 5000
    BIG_TEXT_COUNT = 1000
    OBJECT_COUNT = 15


def random_text(k=0):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


BIG_TEXT = random_text(k=1024 * 20)


def setup():
    os.environ["AWS_ENDPOINT_URL"] = AWS_ENDPOINT_URL
    os.environ["AWS_PORT"] = str(AWS_PORT)


def load():
    """Method for generating 10k document for aws s3 emulator"""
    import boto3

    try:
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
    import boto3

    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=f"{AWS_ENDPOINT_URL}:{AWS_PORT}",
            region_name=REGION_NAME,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_KEY,
        )
        print("Removing data from aws-moto server.")
        for object_id in range(0, OBJECT_COUNT):
            s3_client.delete_object(
                Bucket=BUCKET_NAME, Key=f"{BUCKET_NAME}/{object_id}/"
            )
    except Exception:
        raise
