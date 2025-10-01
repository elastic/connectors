#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Network Drive source module responsible to fetch documents from Network Drive."""


from functools import cached_property, partial

import fastjsonschema
import smbclient
import winrm
from connectors_sdk.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors_sdk.logger import logger

from smbprotocol.exceptions import SMBResponseException
from smbprotocol.file_info import (
    InfoType,
)
from smbprotocol.open import (
    SMB2QueryInfoRequest,
    SMB2QueryInfoResponse,
)
from smbprotocol.security_descriptor import (
    SMB2CreateSDBuffer,
)

from connectors.access_control import (
    prefix_identity,
)

ACCESS_ALLOWED_TYPE = 0
ACCESS_DENIED_TYPE = 1
ACCESS_MASK_ALLOWED_WRITE_PERMISSION = 1048854
ACCESS_MASK_DENIED_WRITE_PERMISSION = 278
GET_USERS_COMMAND = "Get-LocalUser | Select Name, SID"
GET_GROUPS_COMMAND = "Get-LocalGroup | Select-Object Name, SID"
GET_GROUP_MEMBERS = 'Get-LocalGroupMember -Name "{name}" | Select-Object Name, SID'
SECURITY_INFO_DACL = 0x00000004
STATUS_NO_LOGON_SERVERS = 3221225566
STATUS_INVALID_LOGON_HOURS = 3221225583
STATUS_INVALID_WORKSTATION = 3221225584
STATUS_ACCOUNT_DISABLED = 3221225586
STATUS_PASSWORD_MUST_CHANGE = 3221226020

MAX_CHUNK_SIZE = 65536
RETRIES = 3
RETRY_INTERVAL = 2

WINDOWS = "windows"
LINUX = "linux"


class UserAccountDisabledException(Exception):
    pass


class ClientPermissionException(Exception):
    pass


class InvalidLogonHoursException(Exception):
    pass


class PasswordChangeRequiredException(Exception):
    pass


class NoLogonServerException(Exception):
    pass


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

        return await self.validate_pattern(advanced_rules)

    async def validate_pattern(self, advanced_rules):
        try:
            NetworkDriveAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )
        start_with_slash = set()

        for rule in advanced_rules:
            pattern = rule["pattern"]
            if pattern.startswith("/"):
                start_with_slash.add(pattern)

        if start_with_slash:
            message = f"SMB Path should not start with '/' in the beginning. Incorrect path: {start_with_slash}"
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=message,
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
        self._logger = logger

    def create_connection(self):
        """Creates an SMB session to the shared drive."""
        try:
            self.session = smbclient.register_session(
                server=self.server_ip,
                username=self.username,
                password=self.password,
                port=self.port,
            )
        except SMBResponseException as exception:
            self.handle_smb_response_errors(exception=exception)

    def handle_smb_response_errors(self, exception):
        msg = ""
        if exception.status == STATUS_INVALID_WORKSTATION:
            msg = f"Client does not have permission to access server: ({self.server_ip}:{self.port})."
            self._logger.debug(msg=msg)
            raise ClientPermissionException(msg) from exception
        elif exception.status == STATUS_PASSWORD_MUST_CHANGE:
            msg = f"The password for User: {self.username} must be changed before logging on for the first time."
            self._logger.debug(msg=msg)
            raise PasswordChangeRequiredException(msg) from exception
        elif exception.status == STATUS_NO_LOGON_SERVERS:
            msg = f"No logon servers available for connecting to server: ({self.server_ip}:{self.port})."
            self._logger.debug(msg=msg)
            raise NoLogonServerException(msg) from exception
        elif exception.status == STATUS_ACCOUNT_DISABLED:
            msg = f"Account with Username: {self.username} is disabled."
            self._logger.debug(msg=msg)
            raise UserAccountDisabledException(msg) from exception
        elif exception.status == STATUS_INVALID_LOGON_HOURS:
            msg = (
                f"User: {self.username} cannot logon outside the specified logon hours."
            )
            self._logger.debug(msg=msg)
            raise InvalidLogonHoursException(msg) from exception
        else:
            self._logger.debug(
                f"Error occurred while creating SMB session. Error: {exception}"
            )
            raise
