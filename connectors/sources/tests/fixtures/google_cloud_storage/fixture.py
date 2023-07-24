#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Google Cloud Storage module responsible to generate blob(s) on the fake Google Cloud Storage server.
"""
import os
import random
import shutil
import ssl
import string

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

client_connection = None
NUMBER_OF_BLOBS_TO_BE_DELETED = 10
HERE = os.path.dirname(__file__)
HOSTS = "/etc/hosts"

DATA_SIZE = os.environ.get("DATA_SIZE", "medium")

match DATA_SIZE:
    case "extra_small":
        NUMBER_OF_SMALL_FILES = 50
        NUMBER_OF_LARGE_FILES = 5
    case "small":
        NUMBER_OF_SMALL_FILES = 1000
        NUMBER_OF_LARGE_FILES = 10
    case "medium":
        NUMBER_OF_SMALL_FILES = 5000
        NUMBER_OF_LARGE_FILES = 50
    case "large":
        NUMBER_OF_SMALL_FILES = 9500
        NUMBER_OF_LARGE_FILES = 500


class PrerequisiteException(Exception):
    """This class is used to generate the custom error exception when prerequisites are not satisfied."""

    def __init__(self, errors):
        super().__init__(
            f"Error while running e2e test for the Google Cloud Storage connector. \nReason: {errors}"
        )
        self.errors = errors


def get_num_docs():
    print(NUMBER_OF_LARGE_FILES + NUMBER_OF_SMALL_FILES - NUMBER_OF_BLOBS_TO_BE_DELETED)


def verify():
    "Method to verify if prerequisites are satisfied for e2e or not"
    storage_emulator_host = os.getenv(key="STORAGE_EMULATOR_HOST", default=None)
    if storage_emulator_host != "http://localhost:4443":
        raise PrerequisiteException(
            "Environment variable STORAGE_EMULATOR_HOST is not set properly.\n"
            "Solution: Please set the environment variable STORAGE_EMULATOR_HOST value with the below command on your terminal:\n"
            "export STORAGE_EMULATOR_HOST=http://localhost:4443 \n"
        )


def create_connection():
    """Method for creating connection to the fake Google Cloud Storage server"""
    try:
        global client_connection
        client_connection = storage.Client(
            credentials=AnonymousCredentials(),
            project="dummy_project_id",
        )
    except Exception as error:
        print(
            f"Error occurred while creating connection to the fake Google Cloud Storage server. Error: {error}"
        )
        raise


def create_buckets():
    """Method for generating buckets on the fake Google Cloud Storage server"""
    try:
        print("Started loading 2 buckets on the fake Google Cloud Storage server....")

        client_connection.create_bucket("sample-small-files-bucket")
        client_connection.create_bucket("sample-large-files-bucket")

        print("Loaded total 2 buckets on the fake Google Cloud Storage server....")
    except Exception as error:
        print(
            f"Error occurred while creating buckets on the fake Google Cloud Storage server. Error: {error}"
        )


def generate_small_files(start_number_of_small_files, end_number_of_small_files):
    """Method for generating small files on the fake Google Cloud Storage server"""
    try:
        print("Started loading small files on the fake Google Cloud Storage server....")
        bucket = client_connection.bucket("sample-small-files-bucket")

        for number in range(start_number_of_small_files, end_number_of_small_files):
            blob = bucket.blob(f"sample_small_file{number}.random")
            blob.upload_from_string("")

        print(
            f"Loaded {end_number_of_small_files-start_number_of_small_files} small files on the fake Google Cloud Storage server...."
        )
    except Exception as error:
        print(
            f"Error occurred while generating small files on the fake Google Cloud Storage server. Error: {error}"
        )
        raise


def generate_large_files(start_number_of_large_files, end_number_of_large_files):
    """Method for generating large files on the fake Google Cloud Storage server"""
    try:
        size_of_file = 2097152  # 2 Mb of text
        large_data = "".join(
            [random.choice(string.ascii_letters) for i in range(size_of_file)]
        )

        print("Started loading large files on the fake Google Cloud Storage server....")

        bucket = client_connection.bucket("sample-large-files-bucket")

        for number in range(start_number_of_large_files, end_number_of_large_files):
            blob = bucket.blob(f"sample_large_file{number}.txt")
            blob.upload_from_string(large_data)

        print(
            f"Loaded {end_number_of_large_files-start_number_of_large_files} large files on the fake Google Cloud Storage server...."
        )
    except Exception as error:
        print(
            f"Error occurred while generating large files on the fake Google Cloud Storage server. Error: {error}"
        )
        raise


def load():
    create_connection()
    create_buckets()
    if NUMBER_OF_LARGE_FILES:
        generate_large_files(0, NUMBER_OF_LARGE_FILES)
    if NUMBER_OF_SMALL_FILES:
        generate_small_files(0, NUMBER_OF_SMALL_FILES)
    print(
        f"Loaded 2 buckets with {NUMBER_OF_LARGE_FILES} large files and {NUMBER_OF_SMALL_FILES} small files on the fake Google Cloud Storage server."
    )


def remove():
    """Method for removing random blobs from the fake Google Cloud Storage server"""
    create_connection()
    try:
        print(
            "Started removing random blobs from the fake Google Cloud Storage server...."
        )

        bucket = client_connection.bucket("sample-small-files-bucket")
        for number in range(0, NUMBER_OF_BLOBS_TO_BE_DELETED):
            blob = bucket.blob(f"sample_small_file{number}.random")
            blob.delete()

        print(
            f"Removed total {NUMBER_OF_BLOBS_TO_BE_DELETED} random blobs from the fake Google Cloud Storage server...."
        )
    except Exception as error:
        print(
            f"Error occurred while removing blobs from the fake Google Cloud Storage server. Error: {error}"
        )
        raise


def target_ssl_file():
    ssl_paths = ssl.get_default_verify_paths()
    return ssl_paths.cafile is not None and ssl_paths.cafile or ssl_paths.openssl_cafile


def setup():
    verify()

    if os.access(HOSTS, os.W_OK):
        with open(HOSTS) as f:
            hosts = f.read()

        if "127.0.0.1   storage.googleapis.com" not in hosts:
            saved_hosts = os.path.join(HERE, "hosts")
            shutil.copy(HOSTS, saved_hosts)

            with open(HOSTS, "a") as f:
                f.write("\n127.0.0.1   storage.googleapis.com\n")

    with open(os.path.join(HERE, "gcs_dummy_cert.pem")) as f:
        dummy_cert = f.read()

    ssl_file = target_ssl_file()
    if os.path.exists(ssl_file) and os.access(ssl_file, os.W_OK):
        saved_ssl = os.path.join(HERE, "ssl")
        shutil.copy(ssl_file, saved_ssl)

        with open(ssl_file, "a+") as f:
            f.write(f"/n{dummy_cert}/n")


def teardown():
    if os.access(HOSTS, os.W_OK):
        saved_hosts = os.path.join(HERE, "hosts")
        if os.path.exists(saved_hosts):
            shutil.copy(saved_hosts, HOSTS)
    saved_ssl = os.path.join(HERE, "ssl")
    if os.path.exists(saved_ssl):
        shutil.copy(saved_ssl, target_ssl_file())
