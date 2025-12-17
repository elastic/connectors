#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import hashlib

from connectors_sdk.source import BaseDataSource
from connectors_sdk.utils import iso_utc

from connectors.sources.active_directory.client import ActiveDirectoryClient


class ActiveDirectoryDataSource(BaseDataSource):
    """Active Directory Data Source with USN-based incremental sync"""

    name = "Active Directory"
    service_type = "active_directory"
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """
        Initialize Active Directory connector

        Args:
            configuration: Connector configuration from Kibana
        """
        super().__init__(configuration=configuration)

        self.ad_client = ActiveDirectoryClient(
            host=self.configuration["host"],
            port=self.configuration["port"],
            username=self.configuration["username"],
            password=self.configuration["password"],
            base_dn=self.configuration["base_dn"],
            use_ssl=self.configuration["use_ssl"],
            auth_type=self.configuration["auth_type"],
            page_size=self.configuration["page_size"],
            logger=self._logger,
        )

    def _set_internal_logger(self):
        """Set logger for internal components"""
        self.ad_client.logger = self._logger

    @classmethod
    def get_default_configuration(cls):
        """
        Get the default configuration fields for AD connector

        Returns:
            dict: Configuration schema
        """
        return {
            "host": {
                "label": "Active Directory Host",
                "order": 1,
                "type": "str",
                "tooltip": "AD server hostname or IP address",
            },
            "port": {
                "display": "numeric",
                "label": "Port",
                "order": 2,
                "type": "int",
                "value": 636,  # Default LDAPS port
                "tooltip": "LDAP port (389 for LDAP, 636 for LDAPS)",
            },
            "username": {
                "label": "Username",
                "order": 3,
                "type": "str",
                "tooltip": "AD username (can be DOMAIN\\user or user@domain.com)",
            },
            "password": {
                "label": "Password",
                "order": 4,
                "sensitive": True,
                "type": "str",
            },
            "base_dn": {
                "label": "Base DN",
                "order": 5,
                "type": "str",
                "tooltip": "Base Distinguished Name (e.g., DC=example,DC=com)",
            },
            "use_ssl": {
                "display": "toggle",
                "label": "Use SSL/TLS",
                "order": 6,
                "type": "bool",
                "value": True,
                "tooltip": "Use LDAPS (recommended)",
            },
            "auth_type": {
                "display": "dropdown",
                "label": "Authentication Type",
                "options": [
                    {"label": "NTLM", "value": "NTLM"},
                    {"label": "Simple", "value": "SIMPLE"},
                ],
                "order": 7,
                "type": "str",
                "value": "NTLM",
            },
            "sync_users": {
                "display": "toggle",
                "label": "Sync Users",
                "order": 8,
                "type": "bool",
                "value": True,
            },
            "sync_groups": {
                "display": "toggle",
                "label": "Sync Groups",
                "order": 9,
                "type": "bool",
                "value": True,
            },
            "sync_computers": {
                "display": "toggle",
                "label": "Sync Computers",
                "order": 10,
                "type": "bool",
                "value": False,
            },
            "sync_ous": {
                "display": "toggle",
                "label": "Sync Organizational Units",
                "order": 11,
                "type": "bool",
                "value": False,
            },
            "page_size": {
                "default_value": 1000,
                "display": "numeric",
                "label": "Page Size",
                "order": 12,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "tooltip": "Number of objects to fetch per LDAP query",
            },
        }

    async def ping(self):
        """
        Verify connection to Active Directory

        Raises:
            Exception: If connection fails
        """
        self._logger.info("Pinging Active Directory server")
        try:
            await self.ad_client.ping()
            self._logger.info("Successfully connected to Active Directory")
        except Exception as e:
            msg = f"Failed to connect to Active Directory at {self.configuration['host']}"
            self._logger.error(msg)
            raise Exception(msg) from e

    def _create_doc_id(self, object_guid):
        """
        Create document ID from AD objectGUID

        Args:
            object_guid: AD objectGUID (binary or string)

        Returns:
            str: Hashed document ID
        """
        if isinstance(object_guid, bytes):
            guid_str = object_guid.hex()
        else:
            guid_str = str(object_guid)

        return hashlib.sha256(guid_str.encode()).hexdigest()

    def _ad_object_to_doc(self, ad_object, object_type):
        """
        Convert AD object to Elasticsearch document

        Args:
            ad_object: AD object dictionary
            object_type: Type of object (user, group, computer, ou)

        Returns:
            dict: Elasticsearch document
        """
        # Extract objectGUID for document ID
        object_guid = ad_object.get("objectGUID", "")
        doc_id = self._create_doc_id(object_guid)

        # Build document
        doc = {
            "_id": doc_id,
            "_timestamp": iso_utc(),
            "object_type": object_type,
            "distinguished_name": ad_object.get("distinguishedName", ""),
            "name": ad_object.get("name", ""),
            "description": ad_object.get("description", ""),
            "when_created": ad_object.get("whenCreated", ""),
            "when_changed": ad_object.get("whenChanged", ""),
            "usn_changed": ad_object.get("uSNChanged", 0),
        }

        # Add type-specific fields
        if object_type == "user":
            doc.update(
                {
                    "sam_account_name": ad_object.get("sAMAccountName", ""),
                    "user_principal_name": ad_object.get("userPrincipalName", ""),
                    "display_name": ad_object.get("displayName", ""),
                    "given_name": ad_object.get("givenName", ""),
                    "surname": ad_object.get("sn", ""),
                    "email": ad_object.get("mail", ""),
                    "telephone": ad_object.get("telephoneNumber", ""),
                    "title": ad_object.get("title", ""),
                    "department": ad_object.get("department", ""),
                    "company": ad_object.get("company", ""),
                    "manager": ad_object.get("manager", ""),
                    "member_of": ad_object.get("memberOf", []),
                    "user_account_control": ad_object.get("userAccountControl", 0),
                }
            )
        elif object_type == "group":
            doc.update(
                {
                    "sam_account_name": ad_object.get("sAMAccountName", ""),
                    "group_type": ad_object.get("groupType", 0),
                    "members": ad_object.get("member", []),
                    "member_of": ad_object.get("memberOf", []),
                }
            )
        elif object_type == "computer":
            doc.update(
                {
                    "sam_account_name": ad_object.get("sAMAccountName", ""),
                    "dns_host_name": ad_object.get("dNSHostName", ""),
                    "operating_system": ad_object.get("operatingSystem", ""),
                    "operating_system_version": ad_object.get(
                        "operatingSystemVersion", ""
                    ),
                    "last_logon": ad_object.get("lastLogonTimestamp", ""),
                }
            )
        elif object_type == "ou":
            doc.update(
                {
                    "ou": ad_object.get("ou", ""),
                }
            )

        return doc

    async def get_docs(self, filtering=None):
        """
        Get all documents from Active Directory (full sync)

        Args:
            filtering: Sync filtering rules

        Yields:
            dict: Elasticsearch documents
        """
        self._logger.info("Starting full sync from Active Directory")

        # Sync users
        if self.configuration["sync_users"]:
            self._logger.info("Syncing users")
            async for user in self.ad_client.get_users():
                yield self._ad_object_to_doc(user, "user"), None

        # Sync groups
        if self.configuration["sync_groups"]:
            self._logger.info("Syncing groups")
            async for group in self.ad_client.get_groups():
                yield self._ad_object_to_doc(group, "group"), None

        # Sync computers
        if self.configuration["sync_computers"]:
            self._logger.info("Syncing computers")
            async for computer in self.ad_client.get_computers():
                yield self._ad_object_to_doc(computer, "computer"), None

        # Sync OUs
        if self.configuration["sync_ous"]:
            self._logger.info("Syncing organizational units")
            async for ou in self.ad_client.get_organizational_units():
                yield self._ad_object_to_doc(ou, "ou"), None

        # Get and store highest USN for next incremental sync
        highest_usn = await self.ad_client.get_highest_usn()
        self._logger.info(f"Highest USN: {highest_usn}")

    async def get_docs_incrementally(self, sync_cursor, filtering=None):
        """
        Get changed documents from Active Directory using USN (incremental sync)

        Args:
            sync_cursor: Dictionary containing last sync state (last_usn)
            filtering: Sync filtering rules

        Yields:
            tuple: (document, operation, sync_cursor)
                operation: None for update, "delete" for deletion
                sync_cursor: Updated cursor with new last_usn
        """
        last_usn = sync_cursor.get("last_usn", 0) if sync_cursor else 0

        self._logger.info(
            f"Starting incremental sync from Active Directory (last_usn: {last_usn})"
        )

        # Track highest USN seen
        highest_usn = last_usn

        # Sync changed users
        if self.configuration["sync_users"]:
            self._logger.info(f"Syncing users changed since USN {last_usn}")
            async for user in self.ad_client.get_users(last_usn=last_usn):
                usn = user.get("uSNChanged", 0)
                if usn > highest_usn:
                    highest_usn = usn

                yield self._ad_object_to_doc(user, "user"), None

        # Sync changed groups
        if self.configuration["sync_groups"]:
            self._logger.info(f"Syncing groups changed since USN {last_usn}")
            async for group in self.ad_client.get_groups(last_usn=last_usn):
                usn = group.get("uSNChanged", 0)
                if usn > highest_usn:
                    highest_usn = usn

                yield self._ad_object_to_doc(group, "group"), None

        # Sync changed computers
        if self.configuration["sync_computers"]:
            self._logger.info(f"Syncing computers changed since USN {last_usn}")
            async for computer in self.ad_client.get_computers(last_usn=last_usn):
                usn = computer.get("uSNChanged", 0)
                if usn > highest_usn:
                    highest_usn = usn

                yield self._ad_object_to_doc(computer, "computer"), None

        # Sync changed OUs
        if self.configuration["sync_ous"]:
            self._logger.info(f"Syncing OUs changed since USN {last_usn}")
            async for ou in self.ad_client.get_organizational_units(last_usn=last_usn):
                usn = ou.get("uSNChanged", 0)
                if usn > highest_usn:
                    highest_usn = usn

                yield self._ad_object_to_doc(ou, "ou"), None

        # Update sync cursor with new highest USN
        new_cursor = {"last_usn": highest_usn}
        self._logger.info(f"Incremental sync complete. New highest USN: {highest_usn}")

        # Return final cursor
        yield None, None, new_cursor
