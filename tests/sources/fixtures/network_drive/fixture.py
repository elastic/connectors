#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
"""Network Drive module responsible to generate file/folder(s) on Network Drive server.
"""
import os

import smbclient

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

SERVER = "127.0.0.1"
NUMBER_OF_SMALL_FILES = 10000
NUMBER_OF_LARGE_FILES = 2000
NUMBER_OF_FILES_TO_BE_DELETED = 10
USERNAME = "admin"
PASSWORD = "abc@123"

DATA_SIZE = os.environ.get("DATA_SIZE", "medium")

match DATA_SIZE:
    case "small":
        FILE_COUNT = 1000
    case "medium":
        FILE_COUNT = 5000
    case "large":
        FILE_COUNT = 25000


def generate_folder():
    """Method for generating folder on Network Drive server"""
    try:
        print("Started loading folder on network drive server....")

        smbclient.register_session(server=SERVER, username=USERNAME, password=PASSWORD)
        smbclient.mkdir(rf"\\{SERVER}/Folder1/Large-Data-Folder")

        print("Loaded Large-Data-Folder folder on network drive server....")
    except Exception as error:
        print(
            f"Error occurred while generating folder on Network Drive server. Error: {error}"
        )


def generate_files():
    """Method for generating files on Network Drive server"""
    try:
        smbclient.register_session(server=SERVER, username=USERNAME, password=PASSWORD)

        print("Started loading files on network drive server....")

        for number in range(FILE_COUNT):
            with smbclient.open_file(
                rf"\\{SERVER}/Folder1/file{number}.html",
                mode="w",
            ) as fd:
                fd.write(fake_provider.get_html())

        print(f"Loaded {FILE_COUNT} files on network drive server....")
    except Exception as error:
        print(
            f"Error occurred while generating files on Network Drive server. Error: {error}"
        )
        raise


def load():
    generate_folder()
    generate_files()


def remove():
    """Method for deleting 10 random files from Network Drive server"""
    try:
        smbclient.register_session(server=SERVER, username=USERNAME, password=PASSWORD)

        print("Started deleting files from network drive server....")

        for number in range(0, NUMBER_OF_FILES_TO_BE_DELETED):
            smbclient.remove(rf"\\{SERVER}/Folder1/file{number}.html")
            smbclient.remove(rf"\\{SERVER}/Folder1/.deleted/file{number}.html")

        print(
            f"Deleted {NUMBER_OF_FILES_TO_BE_DELETED} files from network drive server...."
        )
    except Exception as error:
        print(
            f"Error occurred while deleting files from Network Drive server. Error: {error}"
        )
        raise
