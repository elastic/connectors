import asyncio
import csv
from collections import deque
from functools import partial

import requests.exceptions
from connectors_sdk.source import BaseDataSource, ConfigurableFieldValueError
from connectors_sdk.utils import iso_utc
from smbprotocol.exceptions import (
    SMBConnectionClosed,
    SMBException,
    SMBOSError,
)
from smbprotocol.open import (
    DirectoryAccessMask,
    FilePipePrinterAccessMask,
)
from wcmatch import glob

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity
)
from connectors.sources.network_drive.netdrive import *
from connectors.utils import (
    RetryStrategy,
    retryable,
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

        stack = deque()
        stack.append(path)

        while stack:
            current_path = stack.pop()

            directory_info = []
            try:
                directory_info = list(
                    await asyncio.to_thread(
                        partial(
                            smbclient.scandir,
                            path=current_path,
                            username=self.username,
                            password=self.password,
                            port=self.port,
                        ),
                    )
                )
                for file in directory_info:
                    yield self.format_document(file=file)
                    if file.is_dir():
                        stack.append(file.path)
            except SMBConnectionClosed as exception:
                self._logger.exception(
                    f"Connection got closed. Error {exception}. Registering new session"
                )
                await asyncio.to_thread(self.smb_connection.create_connection)
                raise
            except (SMBOSError, SMBException) as exception:
                self._logger.exception(
                    f"Error while scanning the path {current_path}. Error {exception}"
                )
                continue

    def is_match_with_previous_rules(
        self, file_path, indexed_rules, match_with_previous_rules
    ):
        # Check if the file is matched with any of the previous indexed rules
        for indexed_rule in indexed_rules:
            if not match_with_previous_rules:
                match_with_previous_rules = glob.globmatch(
                    file_path, indexed_rule, flags=glob.GLOBSTAR
                )
                if match_with_previous_rules:
                    break
        return match_with_previous_rules

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[SMBOSError, SMBException],
    )
    async def traverse_directory_for_syncrule(self, path, glob_pattern, indexed_rules):
        self._logger.debug(
            "Fetching the directory tree from remote server and content of directory on path"
        )
        stack = deque()
        stack.append(path)

        while stack:
            directory_info = []
            current_path = stack.pop()
            try:
                directory_info = await asyncio.to_thread(
                    partial(
                        smbclient.scandir,
                        path=current_path,
                        port=self.port,
                    ),
                )
                for file in directory_info:
                    match_with_previous_rules = False
                    if file.is_dir():
                        stack.append(file.path)
                    file_path = file.path.split("/", 1)[1].replace("\\", "/")
                    is_file_match = glob.globmatch(
                        file_path, glob_pattern, flags=glob.GLOBSTAR
                    )
                    match_with_previous_rules = self.is_match_with_previous_rules(
                        file_path, indexed_rules, match_with_previous_rules
                    )

                    if not match_with_previous_rules and is_file_match:
                        yield self.format_document(file=file)
            except SMBConnectionClosed as exception:
                self._logger.exception(
                    f"Connection got closed. Error {exception}. Registering new session"
                )
                await asyncio.to_thread(self.smb_connection.create_connection)
                raise
            except (SMBOSError, SMBException) as exception:
                self._logger.exception(
                    f"Error while scanning the path {current_path}. Error {exception}"
                )
                continue

    def get_base_path(self, pattern):
        wildcards = ["*", "?", "[", "{", "!", "^"]
        for i, char in enumerate(pattern):
            if char in wildcards:
                return rf"\\{self.server_ip}/{pattern[:i].rsplit('/', 1)[0]}/"
        return rf"\\{self.server_ip}/{pattern}"

    async def fetch_filtered_directory(self, advanced_rules):
        """
        Fetch file and folder based on advanced rules.

        Args:
            advanced_rules (list): List of advanced rules configured

        Returns:
            format_document: Formatted document based on advance rules
        """
        self._logger.debug(
            "Fetching the matched directory/files using the list of advanced rules configured"
        )
        unmatched_rules = set()
        indexed_rules = set()
        for rule in advanced_rules:
            rule_matched = False
            glob_pattern = rule["pattern"].replace("\\", "/")
            base_path = self.get_base_path(pattern=glob_pattern)
            async for document in self.traverse_directory_for_syncrule(
                path=base_path,
                glob_pattern=glob_pattern,
                indexed_rules=indexed_rules,
            ):
                yield document
                rule_matched = True

            if not rule_matched:
                unmatched_rules.add(rule["pattern"])
            indexed_rules.add(glob_pattern)

        if len(unmatched_rules) > 0:
            self._logger.warning(
                f"Following advanced rules do not match with any path present in network drive or the rule is similar to another rule: {unmatched_rules}"
            )

    async def validate_config(self):
        await super().validate_config()
        path = self.configuration["drive_path"]
        if path.startswith("/") or path.startswith("\\"):
            message = f"SMB Path:{path} should not start with '/' in the beginning."
            raise ConfigurableFieldValueError(message)

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
            rid_groups.append(_prefix_rid(group_sid.split("-")[-1]))

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
                    if row:
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
            except requests.exceptions.ConnectionError as exception:
                msg = "Something went wrong"
                raise requests.exceptions.ConnectionError(msg) from exception

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
            async for document in self.fetch_filtered_directory(advanced_rules):
                yield (
                    document,
                    (
                        partial(self.get_content, document)
                        if document["type"] == "file"
                        else None
                    ),
                )

        else:
            groups_info = {}
            if self.drive_type == WINDOWS and self._dls_enabled():
                groups_info = await self.fetch_groups_info()

            async for document in self.traverse_diretory(
                path=rf"\\{self.server_ip}/{self.drive_path}"
            ):
                yield (
                    await self._decorate_with_access_control(
                        document, document["path"], document["type"], groups_info
                    ),
                    (
                        partial(self.get_content, document)
                        if document["type"] == "file"
                        else None
                    ),
                )


def _prefix_user(user):
    return prefix_identity("user", user)


def _prefix_rid(rid):
    return prefix_identity("rid", rid)
