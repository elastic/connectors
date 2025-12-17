#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio
from functools import partial

import ldap3
from ldap3 import ALL, NTLM, Connection, Server
from ldap3.core.exceptions import LDAPException

from connectors.sources.active_directory.constants import (
    COMPUTER_ATTRIBUTES,
    DEFAULT_PAGE_SIZE,
    GROUP_ATTRIBUTES,
    OU_ATTRIBUTES,
    USER_ATTRIBUTES,
)


class ActiveDirectoryClient:
    """Client for connecting to and querying Active Directory via LDAP"""

    def __init__(
        self,
        host,
        port,
        username,
        password,
        base_dn,
        use_ssl=True,
        auth_type="NTLM",
        page_size=DEFAULT_PAGE_SIZE,
        logger=None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.base_dn = base_dn
        self.use_ssl = use_ssl
        self.auth_type = auth_type
        self.page_size = page_size
        self.logger = logger
        self.connection = None

    def _connect(self):
        """Establish connection to AD"""
        server = Server(
            self.host,
            port=self.port,
            use_ssl=self.use_ssl,
            get_info=ALL,
        )

        auth_method = NTLM if self.auth_type == "NTLM" else None

        self.connection = Connection(
            server,
            user=self.username,
            password=self.password,
            authentication=auth_method,
            auto_bind=True,
        )

        if self.logger:
            self.logger.info(f"Connected to AD: {self.host}:{self.port}")

    def _disconnect(self):
        """Close connection to AD"""
        if self.connection:
            self.connection.unbind()
            self.connection = None

    async def ping(self):
        """Test connection to AD"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._ping_sync)

    def _ping_sync(self):
        """Synchronous ping implementation"""
        try:
            self._connect()
            # Perform simple search to verify connection
            self.connection.search(
                search_base=self.base_dn,
                search_filter="(objectClass=*)",
                search_scope="BASE",
                attributes=["distinguishedName"],
            )
            self._disconnect()
        except LDAPException as e:
            raise Exception(f"Failed to connect to AD: {e}")

    async def get_objects(
        self, object_class, attributes, last_usn=None, search_filter=None
    ):
        """
        Get AD objects with optional USN-based incremental sync

        Args:
            object_class: AD object class (user, group, computer, organizationalUnit)
            attributes: List of attributes to retrieve
            last_usn: Last Update Sequence Number for incremental sync
            search_filter: Custom LDAP search filter

        Yields:
            dict: AD object as dictionary
        """
        loop = asyncio.get_event_loop()

        # Build LDAP filter
        if search_filter:
            ldap_filter = search_filter
        elif last_usn:
            # Incremental sync: only objects changed after last_usn
            ldap_filter = f"(&(objectClass={object_class})(uSNChanged>={last_usn}))"
        else:
            # Full sync: all objects of this class
            ldap_filter = f"(objectClass={object_class})"

        if self.logger:
            self.logger.debug(
                f"Searching AD with filter: {ldap_filter}, last_usn: {last_usn}"
            )

        # Execute search in thread pool
        entries = await loop.run_in_executor(
            None,
            partial(
                self._search_paged,
                search_base=self.base_dn,
                search_filter=ldap_filter,
                attributes=attributes,
            ),
        )

        for entry in entries:
            yield self._entry_to_dict(entry)

    def _search_paged(self, search_base, search_filter, attributes):
        """Perform paged LDAP search (synchronous)"""
        try:
            self._connect()

            # Use paged search for large result sets
            entry_generator = self.connection.extend.standard.paged_search(
                search_base=search_base,
                search_filter=search_filter,
                search_scope="SUBTREE",
                attributes=attributes,
                paged_size=self.page_size,
                generator=True,
            )

            results = []
            for entry in entry_generator:
                if entry["type"] == "searchResEntry":
                    results.append(entry)

            self._disconnect()
            return results

        except LDAPException as e:
            self._disconnect()
            if self.logger:
                self.logger.error(f"LDAP search failed: {e}")
            raise

    def _entry_to_dict(self, entry):
        """Convert LDAP entry to dictionary"""
        if "attributes" not in entry:
            return {}

        doc = {}
        for key, value in entry["attributes"].items():
            # Handle multi-value attributes
            if isinstance(value, list):
                if len(value) == 1:
                    doc[key] = value[0]
                else:
                    doc[key] = value
            else:
                doc[key] = value

        # Add DN
        doc["distinguishedName"] = entry.get("dn", "")

        return doc

    async def get_highest_usn(self):
        """
        Get the highest USN from the AD server

        Returns:
            int: Highest committed USN
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_highest_usn_sync)

    def _get_highest_usn_sync(self):
        """Synchronous implementation of get_highest_usn"""
        try:
            self._connect()

            # Query rootDSE for highestCommittedUSN
            self.connection.search(
                search_base="",
                search_filter="(objectClass=*)",
                search_scope="BASE",
                attributes=["highestCommittedUSN"],
            )

            if self.connection.entries:
                usn = self.connection.entries[0].highestCommittedUSN.value
                self._disconnect()
                return int(usn)

            self._disconnect()
            return 0

        except LDAPException as e:
            self._disconnect()
            if self.logger:
                self.logger.error(f"Failed to get highest USN: {e}")
            return 0

    async def get_users(self, last_usn=None):
        """Get all users from AD with optional incremental sync"""
        async for user in self.get_objects("user", USER_ATTRIBUTES, last_usn):
            # Filter out computer accounts (users with $ at end)
            sam_account = user.get("sAMAccountName", "")
            if not sam_account.endswith("$"):
                yield user

    async def get_groups(self, last_usn=None):
        """Get all groups from AD with optional incremental sync"""
        async for group in self.get_objects("group", GROUP_ATTRIBUTES, last_usn):
            yield group

    async def get_computers(self, last_usn=None):
        """Get all computers from AD with optional incremental sync"""
        async for computer in self.get_objects(
            "computer", COMPUTER_ATTRIBUTES, last_usn
        ):
            yield computer

    async def get_organizational_units(self, last_usn=None):
        """Get all OUs from AD with optional incremental sync"""
        async for ou in self.get_objects("organizationalUnit", OU_ATTRIBUTES, last_usn):
            yield ou
