#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Network Drive source module responsible to fetch documents from Network Drive.
"""
import asyncio
import os
from functools import cached_property, partial
from io import BytesIO

import fastjsonschema
import smbclient
from smbprotocol.exceptions import SMBException, SMBOSError
from wcmatch import glob

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.source import BaseDataSource
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    RetryStrategy,
    get_base64_value,
    iso_utc,
    retryable,
)

MAX_CHUNK_SIZE = 65536
DEFAULT_FILE_SIZE_LIMIT = 10485760
RETRIES = 3
RETRY_INTERVAL = 2


class NetworkDriveAdvancedRulesValidator(AdvancedRulesValidator):
    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "pattern": {"type": "string", "minLength": 1},
        },
        "required": ["pattern"],
        "additionalProperties": False,
    }

    SCHEMA_DEFINITION = {"type": "array", "items": RULES_OBJECT_SCHEMA_DEFINITION}
    SCHEMA = fastjsonschema.compile(definition=SCHEMA_DEFINITION)

    def __init__(self, source):
        self.source = source

    async def validate(self, advanced_rules):
        if len(advanced_rules) == 0:
            return SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            )

        return await self._remote_validation(advanced_rules)

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _remote_validation(self, advanced_rules):
        try:
            NetworkDriveAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        self.source.create_connection()
        self.source.advanced_rules = advanced_rules

        _, invalid_rules = self.source.find_matching_paths

        if len(invalid_rules) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Following patterns do not match any path'{', '.join(invalid_rules)}'",
            )

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


class NASDataSource(BaseDataSource):
    """Network Drive"""

    name = "Network Drive"
    service_type = "network_drive"
    advanced_rules_enabled = True

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
        self.session = None
        self.advanced_rules = []

    def advanced_rules_validators(self):
        return [NetworkDriveAdvancedRulesValidator(self)]

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
        self.session = smbclient.register_session(
            server=self.server_ip,
            username=self.username,
            password=self.password,
            port=self.port,
        )

    def get_directory_details(self):
        return smbclient.walk(top=rf"\\{self.server_ip}/{self.drive_path}")

    @cached_property
    def find_matching_paths(self):
        """
        Find matching paths based on advanced rules.

        Returns:
            matched_paths (set): Set of paths that match the advanced rules.
            invalid_rules (list): List of advanced rules that have no matching paths.
        """
        invalid_rules = []
        matched_paths = set()
        for rule in self.advanced_rules:
            rule_valid = False
            glob_pattern = rule["pattern"].replace("\\", "/")
            for path, _, _ in self.get_directory_details():
                normalized_path = path.split("/", 1)[1].replace("\\", "/")
                is_match = glob.globmatch(
                    normalized_path, glob_pattern, flags=glob.GLOBSTAR
                )

                if is_match:
                    rule_valid = True
                    matched_paths.add(path)
            if not rule_valid:
                invalid_rules.append(rule["pattern"])
        return matched_paths, invalid_rules

    async def ping(self):
        """Verify the connection with Network Drive"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(executor=None, func=self.create_connection)
        self._logger.info("Successfully connected to the Network Drive")

    async def close(self):
        """Close all the open smb sessions"""
        if self.session is None:
            return
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
            self._logger.exception(f"Error while scanning the path {path}. Error {exception}")

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

        if filtering and filtering.has_advanced_rules():
            self.advanced_rules = filtering.get_advanced_rules()
            matched_paths, _ = self.find_matching_paths
        else:
            matched_paths = (path for path, _, _ in self.get_directory_details())

        for path in matched_paths:
            async for file in self.get_files(path=path):
                if file["type"] == "folder":
                    yield file, None
                else:
                    yield file, partial(self.get_content, file)
