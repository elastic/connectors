#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Network Drive module responsible to generate file/folder(s) on Network Drive server.
"""
import os
import random
from random import choices
import string
from tests.commons import FakeProvider

import smbclient

SERVER = "127.0.0.1"
NUMBER_OF_SMALL_FILES = 10000
NUMBER_OF_LARGE_FILES = 2000
NUMBER_OF_FILES_TO_BE_DELETED = 10
USERNAME = "admin"
PASSWORD = "abc@123"

fake_provider = FakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium")

match DATA_SIZE:
    case "small":
        FILE_COUNT = 1000
    case "medium":
        FILE_COUNT = 5000
    case "large":
        FILE_COUNT = 25000

population = [fake_provider.small_html(), fake_provider.medium_html(), fake_provider.large_html(), fake_provider.extra_large_html()]
weights = [0.58, 0.3, 0.1, 0.02]

def get_file():
    return choices(population, weights)[0]

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
                rf"\\{SERVER}/Folder1/file{number}.txt",
                mode="w",
            ) as fd:
                fd.write(get_file())

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
            smbclient.remove(rf"\\{SERVER}/Folder1/file{number}.txt")
            smbclient.remove(rf"\\{SERVER}/Folder1/.deleted/file{number}.txt")

        print(
            f"Deleted {NUMBER_OF_FILES_TO_BE_DELETED} files from network drive server...."
        )
    except Exception as error:
        print(
            f"Error occurred while deleting files from Network Drive server. Error: {error}"
        )
        raise
