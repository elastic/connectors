#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#


import os

from connectors_sdk.logger import logger

RETRIES = 3
RETRY_INTERVAL = 2
DEFAULT_RETRY_SECONDS = 30
FETCH_SIZE = 999
DEFAULT_PARALLEL_CONNECTION_COUNT = 15
REQUEST_TIMEOUT = 300
FILE = "file"
FOLDER = "folder"

USERS = "users"
GROUPS = "groups"
PERMISSIONS = "permissions"
DELTA = "delta"
PING = "ping"
BATCH = "batch"
ITEM_FIELDS = "id,name,lastModifiedDateTime,content.downloadUrl,createdDateTime,size,webUrl,parentReference,file,folder"

ENDPOINTS = {
    PING: "drives",
    USERS: "users",
    GROUPS: "users/{user_id}/transitiveMemberOf",
    PERMISSIONS: "users/{user_id}/drive/items/{item_id}/permissions",
    DELTA: "users/{user_id}/drive/root/delta",
    BATCH: "$batch",
}

GRAPH_API_MAX_BATCH_SIZE = 20

if "OVERRIDE_URL" in os.environ:
    logger.warning("x" * 50)
    logger.warning(
        f"ONEDRIVE CONNECTOR CALLS ARE REDIRECTED TO {os.environ['OVERRIDE_URL']}"
    )
    logger.warning("IT'S SUPPOSED TO BE USED ONLY FOR TESTING")
    logger.warning("x" * 50)
    override_url = os.environ["OVERRIDE_URL"]
    BASE_URL = override_url
    GRAPH_API_AUTH_URL = override_url
else:
    BASE_URL = "https://graph.microsoft.com/v1.0/"
    GRAPH_API_AUTH_URL = "https://login.microsoftonline.com"
