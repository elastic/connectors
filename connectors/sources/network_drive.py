#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Network Drive source module responsible to fetch documents from Network Drive.
"""
import asyncio
import os
from functools import partial
from io import BytesIO

import smbclient
from smbprotocol.exceptions import SMBException, SMBOSError

from connectors.source import BaseDataSource
from connectors.utils import TIKA_SUPPORTED_FILETYPES, get_base64_value, iso_utc

MAX_CHUNK_SIZE = 65536
DEFAULT_FILE_SIZE_LIMIT = 10485760


class NASDataSource(BaseDataSource):
    """Network Drive"""

    name = "Network Drive"
    service_type = "network_drive"

    def __init__(self, configuration):
        """Set up the connection to the Network Drive

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.username = self.configuration["username"]
        self.password = self.configuration["password"]
        self.server_ip = self.configuration["server_ip"]
        self.port = self.configuration["server_port"]
        self.drive_path = self.configuration["drive_path"]

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Network Drive.

        Returns:
            dictionary: Default configuration.
        """
        return {
            "username": {
                "label": "Username",
                "order": 1,
                "type": "str",
                "value": "admin",
            },
            "password": {
                "label": "Password",
                "order": 2,
                "sensitive": True,
                "type": "str",
                "value": "abc@123",
            },
            "server_ip": {
                "label": "SMB IP",
                "order": 3,
                "type": "str",
                "value": "127.0.0.1",
            },
            "server_port": {
                "display": "numeric",
                "label": "SMB port",
                "order": 4,
                "type": "int",
                "value": 445,
            },
            "drive_path": {
                "label": "SMB path",
                "order": 5,
                "type": "str",
                "value": "Folder1",
            },
        }

    def create_connection(self):
        """Creates an SMB session to the shared drive."""
        smbclient.register_session(
            server=self.server_ip,
            username=self.username,
            password=self.password,
            port=self.port,
        )

    async def ping(self):
        """Verify the connection with Network Drive"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(executor=None, func=self.create_connection)
        self._logger.info("Successfully connected to the Network Drive")

    async def close(self):
        """Close all the open smb sessions"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            executor=None,
            func=partial(
                smbclient.delete_session, server=self.server_ip, port=self.port
            ),
        )

    async def get_files(self, path):
        """Fetches the metadata of the files and folders present on given path

        Args:
            path (str): The path of a folder in the Network Drive
        """
        files = []
        loop = asyncio.get_running_loop()
        try:
            files = await loop.run_in_executor(None, smbclient.scandir, path)
        except (SMBOSError, SMBException) as exception:
            self._logger.exception(
                f"Error while scanning the path {path}. Error {exception}"
            )

        for file in files:
            file_details = file._dir_info.fields
            yield {
                "path": file.path,
                "size": file_details["allocation_size"].get_value(),
                "_id": file_details["file_id"].get_value(),
                "created_at": iso_utc(file_details["creation_time"].get_value()),
                "_timestamp": iso_utc(file_details["change_time"].get_value()),
                "type": "folder" if file.is_dir() else "file",
                "title": file.name,
            }

    def fetch_file_content(self, path):
        """Fetches the file content from the given drive path

        Args:
            path (str): The file path of the file on the Network Drive
        """
        try:
            with smbclient.open_file(
                path=path, encoding="utf-8", errors="ignore", mode="rb"
            ) as file:
                file_content, chunk = BytesIO(), True
                while chunk:
                    chunk = file.read(MAX_CHUNK_SIZE) or b""
                    file_content.write(chunk)
                file_content.seek(0)
                return file_content
        except SMBOSError as error:
            self._logger.error(
                f"Cannot read the contents of file on path:{path}. Error {error}"
            )

    async def get_content(self, file, timestamp=None, doit=None):
        """Get the content for a given file

        Args:
            file (dictionary): Formatted file document
            timestamp (timestamp, optional): Timestamp of file last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to None.

        Returns:
            dictionary: Content document with id, timestamp & text
        """
        if not (
            doit
            and (os.path.splitext(file["title"])[-1]).lower()
            in TIKA_SUPPORTED_FILETYPES
            and file["size"]
        ):
            return

        if int(file["size"]) > DEFAULT_FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {file['size']} of {file['title']} bytes is larger than {DEFAULT_FILE_SIZE_LIMIT} bytes. Discarding the file content"
            )
            return

        loop = asyncio.get_running_loop()
        content = await loop.run_in_executor(
            executor=None, func=partial(self.fetch_file_content, path=file["path"])
        )

        attachment = content.read()
        content.close()
        return {
            "_id": file["id"],
            "_timestamp": file["_timestamp"],
            "_attachment": get_base64_value(content=attachment),
        }

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch files and folders in async manner.
        Yields:
            dictionary: Dictionary containing the Network Drive files and folders as documents
        """
        directory_details = smbclient.walk(top=rf"\\{self.server_ip}/{self.drive_path}")

        for path, _, _ in directory_details:
            async for file in self.get_files(path=path):
                if file["type"] == "folder":
                    yield file, None
                else:
                    yield file, partial(self.get_content, file)
