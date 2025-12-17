#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

# AD Object Classes
OBJECT_CLASS_USER = "user"
OBJECT_CLASS_GROUP = "group"
OBJECT_CLASS_COMPUTER = "computer"
OBJECT_CLASS_OU = "organizationalUnit"

# AD Attributes for sync
BASE_ATTRIBUTES = [
    "distinguishedName",
    "objectGUID",
    "whenCreated",
    "whenChanged",
    "uSNChanged",  # USN for change tracking
    "objectClass",
    "name",
    "description",
]

USER_ATTRIBUTES = BASE_ATTRIBUTES + [
    "sAMAccountName",
    "userPrincipalName",
    "displayName",
    "givenName",
    "sn",  # surname
    "mail",
    "telephoneNumber",
    "title",
    "department",
    "company",
    "manager",
    "memberOf",
    "userAccountControl",
]

GROUP_ATTRIBUTES = BASE_ATTRIBUTES + [
    "sAMAccountName",
    "groupType",
    "member",
    "memberOf",
]

COMPUTER_ATTRIBUTES = BASE_ATTRIBUTES + [
    "sAMAccountName",
    "dNSHostName",
    "operatingSystem",
    "operatingSystemVersion",
    "lastLogonTimestamp",
]

OU_ATTRIBUTES = BASE_ATTRIBUTES + [
    "ou",
]

# Default page size for LDAP queries
DEFAULT_PAGE_SIZE = 1000

# AD User Account Control flags
UAC_ACCOUNTDISABLE = 0x00000002
UAC_NORMAL_ACCOUNT = 0x00000200
