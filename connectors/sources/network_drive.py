#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Network Drive source module responsible to fetch documents from Network Drive.
"""
import asyncio
import smbclient

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import iso_utc


class NASDataSource(BaseDataSource):
    """Class to fetch documents from Network Drive"""

    def __init__(self, connector):
        """Setup the connection to the Network Drive

        Args:
            connector (BYOConnector): Object of the BYOConnector class
        """
        super().__init__(connector)
        self.username = self.configuration["username"]
        self.password = self.configuration["password"]
        self.server_ip = self.configuration["server_ip"]
        self.port = self.configuration["server_port"]

        self._first_sync = self._dirty = True

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Network Drive.

        Returns:
            dictionary: Default configuration.
        """
        return {
            "username": {
                "value": "admin",
                "label": "SMB username",
                "type": "str",
            },
            "password": {
                "value": "abc@123",
                "label": "SMB password",
                "type": "str",
            },
            "server_ip": {
                "value": "1.2.3.4",
                "label": "SMB IP",
                "type": "str",
            },
            "server_port": {
                "value": 445,
                "label": "SMB port",
                "type": "int",
            },
            "drive_path": {
                "value": "Folder1",
                "label": "SMB shared folder/directory",
                "type": "str",
            },
        }

    def create_connection(self):
        """Creates an SMB session to the shared drive.

        Returns:
            Session object for the network drive
        """
        return smbclient.register_session(
            server=self.server_ip,
            username=self.username,
            password=self.password,
            port=self.port,
        )

    async def ping(self):
        """Verify the connection with Network Drive"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(executor=None, func=self.create_connection)
        logger.info("Successfully connected to the Network Drive")

    async def get_docs(self):
        """Executes the logic to fetch files in async manner.
        Yields:
            dictionary: dictionary containing meta-data of the files.
        """
        # TODO: Fetch documents from Network Drive in async manner.
        # yield dummy document since this is a chunk PR.
        yield {"_id": "123", "timestamp": iso_utc()}, None
        self._dirty = False
