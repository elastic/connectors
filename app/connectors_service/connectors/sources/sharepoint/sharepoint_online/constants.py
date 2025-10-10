#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os

from connectors_sdk.logger import logger

SPO_API_MAX_BATCH_SIZE = 20

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

if "OVERRIDE_URL" in os.environ:
    logger.warning("x" * 50)
    logger.warning(
        f"SHAREPOINT ONLINE CONNECTOR CALLS ARE REDIRECTED TO {os.environ['OVERRIDE_URL']}"
    )
    logger.warning("IT'S SUPPOSED TO BE USED ONLY FOR TESTING")
    logger.warning("x" * 50)
    override_url = os.environ["OVERRIDE_URL"]
    GRAPH_API_URL = override_url
    GRAPH_API_AUTH_URL = override_url
    REST_API_AUTH_URL = override_url
else:
    GRAPH_API_URL = "https://graph.microsoft.com/v1.0"
    GRAPH_API_AUTH_URL = "https://login.microsoftonline.com"
    REST_API_AUTH_URL = "https://accounts.accesscontrol.windows.net"

DEFAULT_RETRY_COUNT = 5
DEFAULT_RETRY_SECONDS = 30
DEFAULT_PARALLEL_CONNECTION_COUNT = 10
DEFAULT_BACKOFF_MULTIPLIER = 5
FILE_WRITE_CHUNK_SIZE = 1024 * 64  # 64KB default SSD page size
MAX_DOCUMENT_SIZE = 10485760
WILDCARD = "*"
DRIVE_ITEMS_FIELDS = "id,content.downloadUrl,lastModifiedDateTime,lastModifiedBy,root,deleted,file,folder,package,name,webUrl,createdBy,createdDateTime,size,parentReference"

CURSOR_SITE_DRIVE_KEY = "site_drives"

# Microsoft Graph API Delta constants
# https://learn.microsoft.com/en-us/graph/delta-query-overview

DELTA_NEXT_LINK_KEY = "@odata.nextLink"
DELTA_LINK_KEY = "@odata.deltaLink"

# See https://github.com/pnp/pnpcore/blob/dev/src/sdk/PnP.Core/Model/SharePoint/Core/Public/Enums/PermissionKind.cs
# See also: https://learn.microsoft.com/en-us/previous-versions/office/sharepoint-csom/ee536458(v=office.15)
VIEW_ITEM_MASK = 0x1  # View items in lists, documents in document libraries, and Web discussion comments.
VIEW_PAGE_MASK = 0x20000  # View pages in a Site.

# See https://github.com/pnp/pnpcore/blob/dev/src/sdk/PnP.Core/Model/SharePoint/Core/Public/Enums/RoleType.cs
# See also: https://learn.microsoft.com/en-us/dotnet/api/microsoft.sharepoint.client.roletype?view=sharepoint-csom
READER = 2
CONTRIBUTOR = 3
WEB_DESIGNER = 4
ADMINISTRATOR = 5
EDITOR = 6
REVIEWER = 7
SYSTEM = 0xFF
# Note the exclusion of NONE(0), GUEST(1), RESTRICTED_READER(8), and RESTRICTED_GUEST(9)
VIEW_ROLE_TYPES = [
    READER,
    CONTRIBUTOR,
    WEB_DESIGNER,
    ADMINISTRATOR,
    EDITOR,
    REVIEWER,
    SYSTEM,
]

# $expand param has a max of 20: see: https://developer.microsoft.com/en-us/graph/known-issues/?search=expand
SPO_MAX_EXPAND_SIZE = 20
