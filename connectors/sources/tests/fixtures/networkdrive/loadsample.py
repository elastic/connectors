#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Network Drive module responsible to generate 15k files on Network Drive server.
"""
import smbclient

SERVER = "127.0.0.1"
NUMBER_OF_FILES = 15000
USERNAME = "admin"
PASSWORD = "abc@123"


def main():
    """Method for generating 15k files on Network Drive server"""
    try:
        smbclient.register_session(server=SERVER, username=USERNAME, password=PASSWORD)

        print("Started loading files on network drive server....")

        for number in range(0, NUMBER_OF_FILES):
            with smbclient.open_file(
                rf"\\{SERVER}/Folder1/file{number}.txt", mode="w"
            ) as fd:
                fd.write(f"File {number} dummy content....")

        print(f"Loaded {NUMBER_OF_FILES} files on network drive server....")
    except Exception as error:
        print(
            f"Error occurred while generating files on Network Drive server. Error: {error}"
        )
        raise


if __name__ == "__main__":
    main()
