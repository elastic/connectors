#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Network Drive module responsible to generate file/folder(s) on Network Drive server.
"""
import random
import string

import smbclient

SERVER = "127.0.0.1"
NUMBER_OF_SMALL_FILES = 10000
NUMBER_OF_LARGE_FILES = 2000
NUMBER_OF_FILES_TO_BE_DELETED = 10
USERNAME = "admin"
PASSWORD = "abc@123"


def generate_small_files():
    """Method for generating small files on Network Drive server"""
    try:
        smbclient.register_session(server=SERVER, username=USERNAME, password=PASSWORD)

        print("Started loading small files on network drive server....")

        for number in range(0, NUMBER_OF_SMALL_FILES):
            with smbclient.open_file(
                rf"\\{SERVER}/Folder1/file{number}.txt", mode="w"
            ) as fd:
                fd.write(f"File {number} dummy content....")

        print(f"Loaded {NUMBER_OF_SMALL_FILES} small files on network drive server....")
    except Exception as error:
        print(
            f"Error occurred while generating small files on Network Drive server. Error: {error}"
        )
        raise


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


def generate_large_files():
    """Method for generating large files on Network Drive server"""
    try:
        size_of_file = 1024**2  # 1 Mb of text
        large_data = "".join(
            [random.choice(string.ascii_letters) for i in range(size_of_file)]
        )

        smbclient.register_session(server=SERVER, username=USERNAME, password=PASSWORD)

        print("Started loading large files on network drive server....")

        for number in range(0, NUMBER_OF_LARGE_FILES):
            with smbclient.open_file(
                rf"\\{SERVER}/Folder1/Large-Data-Folder/large_size_file{number}.txt",
                mode="w",
            ) as fd:
                fd.write(large_data)

        print(f"Loaded {NUMBER_OF_LARGE_FILES} large files on network drive server....")
    except Exception as error:
        print(
            f"Error occurred while generating large files on Network Drive server. Error: {error}"
        )
        raise


def load():
    if NUMBER_OF_SMALL_FILES:
        generate_small_files()
    generate_folder()
    if NUMBER_OF_LARGE_FILES:
        generate_large_files()
    print(
        f"Loaded {NUMBER_OF_LARGE_FILES} large files, {NUMBER_OF_SMALL_FILES} small files, and 1 folder on network drive server."
    )


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
