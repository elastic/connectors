#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Network Drive source module responsible to fetch documents from Network Drive.
"""
import asyncio
import csv
from functools import cached_property, partial

import fastjsonschema
import smbclient
import winrm
from requests.exceptions import ConnectionError
from smbprotocol.exceptions import SMBConnectionClosed, SMBException, SMBOSError
from smbprotocol.file_info import (
    InfoType,
)
from smbprotocol.open import (
    DirectoryAccessMask,
    FilePipePrinterAccessMask,
    SMB2QueryInfoRequest,
    SMB2QueryInfoResponse,
)
from smbprotocol.security_descriptor import (
    SMB2CreateSDBuffer,
)
from wcmatch import glob

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    RetryStrategy,
    iso_utc,
    retryable,
)

ACCESS_ALLOWED_TYPE = 0
ACCESS_DENIED_TYPE = 1
ACCESS_MASK_ALLOWED_WRITE_PERMISSION = 1048854
ACCESS_MASK_DENIED_WRITE_PERMISSION = 278
GET_USERS_COMMAND = "Get-LocalUser | Select Name, SID"
GET_GROUPS_COMMAND = "Get-LocalGroup | Select-Object Name, SID"
GET_GROUP_MEMBERS = 'Get-LocalGroupMember -Name "{name}" | Select-Object Name, SID'
SECURITY_INFO_DACL = 0x00000004

MAX_CHUNK_SIZE = 65536
RETRIES = 3
RETRY_INTERVAL = 2

WINDOWS = "windows"
LINUX = "linux"


def _prefix_user(user):
    return prefix_identity("user", user)


def _prefix_rid(rid):
    return prefix_identity("rid", rid)


class InvalidRulesError(Exception):
    pass


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

        await asyncio.to_thread(self.source.smb_connection.create_connection)

        _, invalid_rules = await self.source.find_matching_paths(advanced_rules)

        if len(invalid_rules) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Following patterns do not match any path'{', '.join(invalid_rules)}'",
            )

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


class SecurityInfo:
    def __init__(self, user, password, server):
        self.username = user
        self.server_ip = server
        self.password = password

    def get_descriptor(self, file_descriptor, info):
        """Get the Security Descriptor for the opened file."""
        query_request = SMB2QueryInfoRequest()
        query_request["info_type"] = InfoType.SMB2_0_INFO_SECURITY
        query_request["output_buffer_length"] = MAX_CHUNK_SIZE
        query_request["additional_information"] = info
        query_request["file_id"] = file_descriptor.file_id

        req = file_descriptor.connection.send(
            query_request,
            sid=file_descriptor.tree_connect.session.session_id,
            tid=file_descriptor.tree_connect.tree_connect_id,
        )
        response = file_descriptor.connection.receive(req)
        query_response = SMB2QueryInfoResponse()
        query_response.unpack(response["data"].get_value())

        security_descriptor = SMB2CreateSDBuffer()
        security_descriptor.unpack(query_response["buffer"].get_value())

        return security_descriptor

    @cached_property
    def session(self):
        return winrm.Session(
            self.server_ip,
            auth=(self.username, self.password),
            transport="ntlm",
            server_cert_validation="ignore",
        )

    def parse_output(self, raw_output):
        """
        Formats and extracts key-value pairs from raw output data.

        This method takes raw output data as input and processes it to extract
        key-value pairs. It handles cases where key-value pairs may be spread
        across multiple lines and returns a dictionary containing the formatted
        key-value pairs.

        Args:
            raw_output (str): The raw output data to be processed.

        Returns:
            dict: A dictionary containing key-value pairs extracted from the raw
                output data.

        Example:
            Given the following raw output:

            Header 1: Value 1
            Header 2: Value 2
            Key 1   Value1
            Key 2   Value2
            Key 3   Value3

            The method will return the following dictionary:

            {
                "Key 1": "Value1",
                "Key 2": "Value2",
                "Key 3": "Value3"
            }
        """
        formatted_result = {}
        output_lines = raw_output.std_out.decode().splitlines()

        #  Ignoring initial headers with fixed length of 2
        if len(output_lines) > 2:
            for line in output_lines[3:]:
                parts = line.rsplit(maxsplit=1)
                if len(parts) == 2:
                    key, value = parts
                    key = key.strip()
                    value = value.strip()
                    formatted_result[key] = value

        return formatted_result

    def fetch_users(self):
        users = self.session.run_ps(GET_USERS_COMMAND)
        return self.parse_output(users)

    def fetch_groups(self):
        groups = self.session.run_ps(GET_GROUPS_COMMAND)

        return self.parse_output(groups)

    def fetch_members(self, group_name):
        members = self.session.run_ps(GET_GROUP_MEMBERS.format(name=group_name))

        return self.parse_output(members)


class SMBSession:
    _connection = None

    def __init__(self, server_ip, username, password, port):
        self.server_ip = server_ip
        self.username = username
        self.password = password
        self.port = port
        self.session = None

    def create_connection(self):
        """Creates an SMB session to the shared drive."""
        self.session = smbclient.register_session(
            server=self.server_ip,
            username=self.username,
            password=self.password,
            port=self.port,
        )


class NASDataSource(BaseDataSource):
    """Network Drive"""

    name = "Network Drive"
    service_type = "network_drive"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

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
        self.drive_type = self.configuration["drive_type"]
        self.identity_mappings = self.configuration["identity_mappings"]
        self.security_info = SecurityInfo(self.username, self.password, self.server_ip)

    def advanced_rules_validators(self):
        return [NetworkDriveAdvancedRulesValidator(self)]

    @cached_property
    def smb_connection(self):
        return SMBSession(self.server_ip, self.username, self.password, self.port)

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
            },
            "password": {
                "label": "Password",
                "order": 2,
                "sensitive": True,
                "type": "str",
            },
            "server_ip": {
                "label": "SMB IP",
                "order": 3,
                "type": "str",
            },
            "server_port": {
                "display": "numeric",
                "label": "SMB port",
                "order": 4,
                "type": "int",
            },
            "drive_path": {
                "label": "SMB path",
                "order": 5,
                "type": "str",
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 6,
                "tooltip": "Document level security ensures identities and permissions set in your network drive are mirrored in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
            "drive_type": {
                "display": "dropdown",
                "label": "Drive type",
                "depends_on": [
                    {"field": "use_document_level_security", "value": True},
                ],
                "options": [
                    {"label": "Windows", "value": WINDOWS},
                    {"label": "Linux", "value": LINUX},
                ],
                "order": 7,
                "type": "str",
                "ui_restrictions": ["advanced"],
                "value": WINDOWS,
            },
            "identity_mappings": {
                "label": "Path of CSV file containing users and groups SID (For Linux Network Drive)",
                "depends_on": [
                    {"field": "use_document_level_security", "value": True},
                    {"field": "drive_type", "value": LINUX},
                ],
                "order": 8,
                "type": "str",
                "required": False,
                "ui_restrictions": ["advanced"],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 9,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }

    def format_document(self, file):
        file_details = file._dir_info.fields
        document = {
            "path": file.path,
            "size": file_details["end_of_file"].get_value(),
            "_id": file_details["file_id"].get_value(),
            "created_at": iso_utc(file_details["creation_time"].get_value()),
            "_timestamp": iso_utc(file_details["change_time"].get_value()),
            "type": "file" if file.is_file() else "folder",
            "title": file.name,
        }
        return document

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[SMBOSError, SMBException],
    )
    async def traverse_diretory(self, path):
        self._logger.debug(
            "Fetching the directory tree from remote server and content of directory on path"
        )
        directory_info = []
        try:
            directory_info = await asyncio.to_thread(
                partial(
                    smbclient.scandir,
                    path=path,
                    username=self.username,
                    password=self.password,
                    port=self.port,
                ),
            )
        except SMBConnectionClosed as exception:
            self._logger.exception(
                f"Connection got closed. Error {exception}. Registering new session"
            )
            await asyncio.to_thread(self.smb_connection.create_connection)
            raise
        except (SMBOSError, SMBException) as exception:
            self._logger.exception(
                f"Error while scanning the path {path}. Error {exception}"
            )

        for file in directory_info:
            yield self.format_document(file=file)
            if file.is_dir():
                async for sub_file_data in self.traverse_diretory(
                    path=file.path
                ):  # pyright: ignore
                    yield sub_file_data

    async def get_directory_details(self):
        self._logger.debug("Fetching the directory tree from remote server")
        paths = await asyncio.to_thread(
            partial(
                smbclient.walk,
                top=rf"\\{self.server_ip}/{self.drive_path}",
                port=self.port,
            ),
        )
        return list(paths)

    async def find_matching_paths(self, advanced_rules):
        """
        Find matching paths based on advanced rules.

        Args:
            advanced_rules (list): List of advanced rules configured

        Returns:
            matched_paths (set): Set of paths that match the advanced rules.
            invalid_rules (list): List of advanced rules that have no matching paths.
        """
        self._logger.debug(
            "Fetching the matched directory paths using the list of advanced rules configured"
        )
        invalid_rules = []
        matched_paths = set()
        for rule in advanced_rules:
            rule_valid = False
            glob_pattern = rule["pattern"].replace("\\", "/")
            paths = await self.get_directory_details()
            for path, _, _ in paths:
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

        await asyncio.to_thread(self.smb_connection.create_connection)
        await self.close()
        self._logger.info("Successfully connected to the Network Drive")

    async def close(self):
        """Close all the open smb sessions"""
        if self.smb_connection.session is None:
            return
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            executor=None,
            func=partial(
                smbclient.delete_session, server=self.server_ip, port=self.port
            ),
        )

    async def fetch_file_content(self, path):
        """Fetches the file content from the given drive path

        Args:
            path (str): The file path of the file on the Network Drive
        """
        self._logger.debug(f"Fetching the contents of file on path: {path}")
        try:
            with smbclient.open_file(
                path=path,
                encoding="utf-8",
                errors="ignore",
                mode="rb",
                username=self.username,
                password=self.password,
                port=self.port,
            ) as file:
                chunk = True
                while chunk:
                    chunk = file.read(MAX_CHUNK_SIZE) or b""
                    yield chunk
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
        if not (doit):
            return

        filename = file["title"]
        file_size = file["size"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            self._logger.warning(
                f"File size {file['size']} of {file['title']} bytes is larger than {self.framework_config.max_file_size} bytes. Discarding the file content"
            )
            return

        document = {
            "_id": file["id"],
            "_timestamp": file["_timestamp"],
        }

        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(self.fetch_file_content, path=file["path"]),
        )

    def list_file_permission(self, file_path, file_type, mode, access):
        try:
            with smbclient.open_file(
                file_path,
                mode=mode,
                buffering=0,
                file_type=file_type,
                desired_access=access,
                port=self.port,
            ) as file:
                descriptor = self.security_info.get_descriptor(
                    file_descriptor=file.fd, info=SECURITY_INFO_DACL
                )
                return descriptor.get_dacl()["aces"]
        except SMBOSError as error:
            self._logger.error(
                f"Cannot read the contents of file on path:{file_path}. Error {error}"
            )

    def _dls_enabled(self):
        if (
            self._features is None
            or not self._features.document_level_security_enabled()
        ):
            return False

        return self.configuration["use_document_level_security"]

    async def _decorate_with_access_control(
        self, document, file_path, file_type, groups_info
    ):
        if self._dls_enabled():
            allow_permissions, deny_permissions = await self.get_entity_permission(
                file_path=file_path, file_type=file_type, groups_info=groups_info
            )
            entity_permissions = list(set(allow_permissions) - set(deny_permissions))
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + entity_permissions)
            )
        return document

    async def _user_access_control_doc(self, user, sid, groups_info=None):
        rid = str(sid).split("-")[-1]
        prefixed_username = _prefix_user(user)
        rid_user = _prefix_rid(rid)
        rid_groups = []

        for group_sid in groups_info or []:
            rid = group_sid.split("-")[-1]
            rid_groups.append(_prefix_rid(rid))

        access_control = [rid_user, prefixed_username, *rid_groups]

        return {
            "_id": rid,
            "identity": {
                "username": prefixed_username,
                "user_id": rid_user,
            },
            "created_at": iso_utc(),
        } | es_access_control_query(access_control)

    def read_user_info_csv(self):
        with open(self.identity_mappings, encoding="utf-8") as file:
            user_info = []
            try:
                csv_reader = csv.reader(file, delimiter=";")
                for row in csv_reader:
                    user_info.append(
                        {
                            "name": row[0],
                            "user_sid": row[1],
                            "groups": row[2].split(",") if len(row[2]) > 0 else [],
                        }
                    )
            except csv.Error as e:
                self._logger.exception(
                    f"Error while reading user mapping file at the location: {self.identity_mappings}. Error: {e}"
                )
            return user_info

    async def fetch_groups_info(self):
        self._logger.info(
            f"Fetching all groups and members for drive at path '{self.drive_path}'"
        )
        groups_info = await asyncio.to_thread(self.security_info.fetch_groups)

        groups_members = {}
        for group_name, group_sid in groups_info.items():
            rid = group_sid.split("-")[-1]
            groups_members[rid] = await asyncio.to_thread(
                self.security_info.fetch_members, group_name
            )

        return groups_members

    async def get_access_control(self):
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        # This if block fetches users, groups via local csv file path
        if self.drive_type == LINUX:
            if self.identity_mappings:
                self._logger.info(
                    f"Fetching all groups and users from configured file path '{self.identity_mappings}'"
                )

                for user in self.read_user_info_csv():
                    yield await self._user_access_control_doc(
                        user=user["name"],
                        sid=user["user_sid"],
                        groups_info=user["groups"],
                    )
            else:
                msg = "CSV file path cannot be empty. Please provide a valid csv file path."
                raise ConfigurableFieldValueError(msg)
        else:
            try:
                self._logger.info(
                    f"Fetching all users for drive at path '{self.drive_path}'"
                )
                users_info = await asyncio.to_thread(self.security_info.fetch_users)

                for user, sid in users_info.items():
                    yield await self._user_access_control_doc(
                        user=user,
                        sid=sid,
                    )
            except ConnectionError as exception:
                msg = "Something went wrong"
                raise ConnectionError(msg) from exception

    async def get_entity_permission(self, file_path, file_type, groups_info):
        """Processes permissions for a network drive, focusing on key terms:

        - SID (Security Identifier): The unique identifier for a user or group, it undergoes revision.
        - RID (Relative Identifier): The last part of a SID, uniquely identifying a user or group within a domain.
        - ACE (Access Control Entry): An entry in `an Access Control List (ACL), defining permissions for a specific user or group. 0 is allowed ACE, 1 is denied ACE.
        - Mask: A bit field representing allowed or denied permissions within an ACE. Example: Deny write, Allow read, etc.
        """
        if not self._dls_enabled():
            return []

        allow_permissions = []
        deny_permissions = []
        if file_type == "file":
            list_permissions = await asyncio.to_thread(
                self.list_file_permission,
                file_path=file_path,
                file_type="file",
                mode="rb",
                access=FilePipePrinterAccessMask.READ_CONTROL,
            )
        else:
            list_permissions = await asyncio.to_thread(
                self.list_file_permission,
                file_path=file_path,
                file_type="dir",
                mode="br",
                access=DirectoryAccessMask.READ_CONTROL,
            )
        for permission in list_permissions or []:
            # Access mask indicates specific permission within an ACE, such as read in deny ACE.
            mask = permission["mask"].value

            # Determine the type of ACE (access control entry), i.e, allow or deny
            ace_type = permission["ace_type"].value

            # Extract RID from SID. RID uniquely identifying a user or group within a domain.
            rid = str(permission["sid"]).split("-")[-1]

            if groups_info.get(rid):
                # If the RID corresponds to a group, get the RIDs of all members of that group
                permissions = [
                    _prefix_rid(member_id.split("-")[-1])
                    for member_id in groups_info[rid].values()
                ]
            else:
                # Else the RID corresponds to a user, hence we use it directly.
                permissions = [_prefix_rid(rid)]
            if (
                ace_type == ACCESS_ALLOWED_TYPE
                or mask == ACCESS_MASK_DENIED_WRITE_PERMISSION
            ):
                allow_permissions.extend(permissions)

            if (
                ace_type == ACCESS_DENIED_TYPE
                and mask != ACCESS_MASK_DENIED_WRITE_PERMISSION
            ):
                deny_permissions.extend(permissions)

        return allow_permissions, deny_permissions

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch files and folders in async manner.
        Yields:
            dictionary: Dictionary containing the Network Drive files and folders as documents
        """
        await asyncio.to_thread(self.smb_connection.create_connection)
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            matched_paths, invalid_rules = await self.find_matching_paths(
                advanced_rules
            )
            if len(invalid_rules) > 0:
                msg = f"Following advanced rules are invalid: {invalid_rules}"
                raise InvalidRulesError(msg)

            for path in matched_paths:
                async for document in self.traverse_diretory(path=path):
                    yield document, partial(self.get_content, document) if document[
                        "type"
                    ] == "file" else None

        else:
            groups_info = {}
            if self.drive_type == WINDOWS and self._dls_enabled():
                groups_info = await self.fetch_groups_info()

            async for document in self.traverse_diretory(
                path=rf"\\{self.server_ip}/{self.drive_path}"
            ):
                yield await self._decorate_with_access_control(
                    document, document["path"], document["type"], groups_info
                ), partial(self.get_content, document) if document[
                    "type"
                ] == "file" else None
