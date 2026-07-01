#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
RETRIES = 3
RETRY_INTERVAL = 2
DEFAULT_RETRY_SECONDS = 30
LIMIT = 50
SPACE = "space"
SPACE_PERMISSION = "space_permission"
BLOGPOST = "blogpost"
PAGE = "page"
ATTACHMENT = "attachment"
CONTENT = "content"
DOWNLOAD = "download"
SEARCH = "search"
USER = "user"
USERS_FOR_DATA_CENTER = "users_for_data_center"
SEARCH_FOR_DATA_CENTER = "search_for_data_center"
USERS_FOR_SERVER = "users_for_server"
SPACE_QUERY_CLOUD = "limit=100&expand=permissions,history"
# DC/Server omits expand=permissions,history: permissions come from the
# SPACE_PERMISSION (Extender) endpoint and history is Cloud-only. Avoids HTTP 500
# on Confluence DC/Server versions affected by CONFSERVER-99908 and similar bugs.
SPACE_QUERY_DATA_CENTER = "limit=100"
ATTACHMENT_QUERY = "limit=100&expand=version,history"
CONTENT_QUERY_CLOUD = "limit=50&expand=ancestors,children.attachment,history.lastUpdated,body.storage,space,space.permissions,restrictions.read.restrictions.user,restrictions.read.restrictions.group"
# DC/Server omits space.permissions: space permissions come from the
# SPACE_PERMISSION (Extender) endpoint and space.permissions is only read on the
# Cloud code path. Avoids HTTP 500 on Confluence DC/Server versions affected by
# CONFSERVER-99908 and similar expand=permissions bugs.
CONTENT_QUERY_DATA_CENTER = "limit=50&expand=ancestors,children.attachment,history.lastUpdated,body.storage,space,restrictions.read.restrictions.user,restrictions.read.restrictions.group"
SEARCH_QUERY = "limit=100&expand=content.history,content.extensions,content.container,content.space,content.body.storage,space.description,space.history"
USER_QUERY = "expand=groups,applicationRoles"
LABEL = "label"
URLS = {
    SPACE: "rest/api/space?{api_query}",
    SPACE_PERMISSION: "rest/extender/1.0/permission/space/{space_key}/getSpacePermissionActors/VIEWSPACE",
    CONTENT: "rest/api/content/search?{api_query}",
    ATTACHMENT: "rest/api/content/{id}/child/attachment?{api_query}",
    SEARCH: "rest/api/search?cql={query}",
    SEARCH_FOR_DATA_CENTER: "rest/api/search?cql={query}&start={start}",
    USER: "rest/api/3/users/search",
    USERS_FOR_DATA_CENTER: "rest/api/user/list?limit={limit}&start={start}",
    USERS_FOR_SERVER: "rest/extender/1.0/user/getUsersWithConfluenceAccess?showExtendedDetails=true&startAt={start}&maxResults={limit}",
    LABEL: "rest/api/content/{id}/label",
}
PING_URL = "rest/api/space?limit=1"
MAX_CONCURRENT_DOWNLOADS = 50  # Max concurrent download supported by confluence
MAX_CONCURRENCY = 50
QUEUE_SIZE = 1024
QUEUE_MEM_SIZE = 25 * 1024 * 1024  # Size in Megabytes
SERVER_USER_BATCH = 1000
DATACENTER_USER_BATCH = 200
END_SIGNAL = "FINISHED_TASK"
CONFLUENCE_CLOUD = "confluence_cloud"
CONFLUENCE_SERVER = "confluence_server"
CONFLUENCE_DATA_CENTER = "confluence_data_center"
WILDCARD = "*"
