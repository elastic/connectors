import boto3
import sys

BUCKET_NAME = "ent-search-ingest-dev"
OBJECT_COUNT = 15000
ENDPOINT_URL = "http://127.0.0.1"


def main():
    """Method for generating 15k document for aws s3 emulator"""
    try:
        try:
            PORT = sys.argv[1]
        except IndexError:
            PORT = 5000
        s3_client = boto3.client("s3", endpoint_url=f"{ENDPOINT_URL}:{PORT}")
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print("Creating objects on the aws-moto server")
        for object_id in range(0, OBJECT_COUNT):
            s3_client.put_object(
                Key=f"{BUCKET_NAME}/{object_id}",
                Bucket=BUCKET_NAME,
                Body=f"Testing object{object_id} document for bucket: {BUCKET_NAME}",
                StorageClass="STANDARD",
            )
    except Exception as exception:
        print(f"Exception: {exception}")


if __name__ == "__main__":
    main()
