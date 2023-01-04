import sys

import boto3

BUCKET_NAME = "ent-search-ingest-dev"
REGION_NAME = "us-west-2"
OBJECT_COUNT = 15
ENDPOINT_URL = "http://127.0.0.1"


def main():
    """Method for removing 15 random document from aws s3 emulator"""
    try:
        try:
            PORT = sys.argv[1]
        except IndexError:
            PORT = 5000
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


if __name__ == "__main__":
    main()
