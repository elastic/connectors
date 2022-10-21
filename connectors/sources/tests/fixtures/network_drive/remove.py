#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Network Drive module responsible to delete files from Network Drive server.
"""
import smbclient

SERVER = "127.0.0.1"
NUMBER_OF_FILES_TO_BE_DELETED = 10
USERNAME = "admin"
PASSWORD = "abc@123"


def main():
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


if __name__ == "__main__":
    main()
