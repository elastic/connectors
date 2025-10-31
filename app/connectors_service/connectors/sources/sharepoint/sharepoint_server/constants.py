#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

BASIC_AUTH = "Basic"
NTLM_AUTH = "NTLM"
RETRY_INTERVAL = 2
DEFAULT_RETRY_SECONDS = 30
RETRIES = 3
TOP = 5000
PING = "ping"
SITE = "site"
SITES = "sites"
LISTS = "lists"
ATTACHMENT = "attachment"
DRIVE_ITEM = "drive_item"
LIST_ITEM = "list_item"
USERS = "users"
ADMIN_USERS = "admin_users"
ROLES = "roles"
UNIQUE_ROLES = "unique_roles"
UNIQUE_ROLES_FOR_ITEM = "unique_roles_for_item"
ROLES_BY_TITLE_FOR_LIST = "roles_by_title_for_list"
ROLES_BY_TITLE_FOR_ITEM = "roles_by_title_for_item"
ATTACHMENT_DATA = "attachment_data"
DOCUMENT_LIBRARY = "document_library"
SELECTED_FIELDS = "WikiField,CanvasContent1,Modified,Id,GUID,File,Folder,Author/Name,Editor/Name,Author/Id,Editor/Id"
VIEW_ITEM_MASK = 0x1  # View items in lists, documents in document libraries, and Web discussion comments.
VIEW_PAGE_MASK = 0x20000  # View pages in a Site.
IS_USER = 1  # To verify principal type value for user or group

READER = 2
CONTRIBUTOR = 3
WEB_DESIGNER = 4
ADMINISTRATOR = 5
EDITOR = 6
REVIEWER = 7
SYSTEM = 0xFF
VIEW_ROLE_TYPES = [
    READER,
    CONTRIBUTOR,
    WEB_DESIGNER,
    ADMINISTRATOR,
    EDITOR,
    REVIEWER,
    SYSTEM,
]


URLS = {
    PING: "{site_url}/_api/web/webs",
    SITE: "{parent_site_url}/_api/web",
    SITES: "{parent_site_url}/_api/web/webs?$skip={skip}&$top={top}",
    LISTS: "{parent_site_url}/_api/web/lists?$skip={skip}&$top={top}&$expand=RootFolder&$filter=(Hidden eq false)",
    ATTACHMENT: "{value}/_api/web/GetFileByServerRelativeUrl('{file_relative_url}')/$value",
    DRIVE_ITEM: "{parent_site_url}/_api/web/lists(guid'{list_id}')/items?$select={selected_field}&$expand=File,Folder,Author,Editor&$top={top}",
    LIST_ITEM: "{parent_site_url}/_api/web/lists(guid'{list_id}')/items?$expand=AttachmentFiles,Author,Editor&$select=*,FileRef,Author/Title,Editor/Title",
    ATTACHMENT_DATA: "{parent_site_url}/_api/web/getfilebyserverrelativeurl('{file_relative_url}')",
    USERS: "{parent_site_url}/_api/web/siteusers?$skip={skip}&$top={top}",
    ADMIN_USERS: "{parent_site_url}/_api/web/siteusers?$skip={skip}&$top={top}&$filter=IsSiteAdmin eq true",
    ROLES: "{parent_site_url}/_api/web/roleassignments?$expand=Member/users,RoleDefinitionBindings&$skip={skip}&$top={top}",
    UNIQUE_ROLES: "{parent_site_url}/_api/lists/GetByTitle('{site_list_name}')/HasUniqueRoleAssignments",
    ROLES_BY_TITLE_FOR_LIST: "{parent_site_url}/_api/lists/GetByTitle('{site_list_name}')/roleassignments?$expand=Member/users,RoleDefinitionBindings&$skip={skip}&$top={top}",
    UNIQUE_ROLES_FOR_ITEM: "{parent_site_url}/_api/lists/GetByTitle('{site_list_name}')/items({list_item_id})/HasUniqueRoleAssignments",
    ROLES_BY_TITLE_FOR_ITEM: "{parent_site_url}/_api/lists/GetByTitle('{site_list_name}')/items({list_item_id})/roleassignments?$expand=Member/users,RoleDefinitionBindings&$skip={skip}&$top={top}",
}
SCHEMA = {
    SITES: {
        "title": "Title",
        "url": "Url",
        "server_relative_url": "ServerRelativeUrl",
        "_timestamp": "LastItemModifiedDate",
        "creation_time": "Created",
    },
    LISTS: {
        "title": "Title",
        "parent_web_url": "ParentWebUrl",
        "_timestamp": "LastItemModifiedDate",
        "creation_time": "Created",
    },
    DOCUMENT_LIBRARY: {
        "title": "Title",
        "parent_web_url": "ParentWebUrl",
        "_timestamp": "LastItemModifiedDate",
        "creation_time": "Created",
    },
    LIST_ITEM: {
        "title": "Title",
        "author_id": "AuthorId",
        "editor_id": "EditorId",
        "creation_time": "Created",
        "_timestamp": "Modified",
    },
    DRIVE_ITEM: {
        "title": "Name",
        "creation_time": "TimeCreated",
        "_timestamp": "TimeLastModified",
    },
}
