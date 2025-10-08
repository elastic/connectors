#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

FINISHED = "FINISHED"
WILDCARD = "*"
RETRIES = 3
RETRY_INTERVAL = 2
DEFAULT_RETRY_SECONDS = 30
FETCH_SIZE = 100
MAX_USER_FETCH_LIMIT = 1000
QUEUE_MEM_SIZE = 5 * 1024 * 1024  # Size in Megabytes
MAX_CONCURRENCY = 5
MAX_CONCURRENT_DOWNLOADS = 100  # Max concurrent download supported by jira
PING = "ping"
PROJECT = "project"
PROJECT_BY_KEY = "project_by_key"
ISSUES = "all_issues"
ISSUE_DATA = "issue_data"
ATTACHMENT_CLOUD = "attachment_cloud"
ATTACHMENT_SERVER = "attachment_server"
USERS = "users"
USERS_FOR_DATA_CENTER = "users_for_data_center"
PERMISSIONS_BY_KEY = "permissions_by_key"
ISSUE_SECURITY_LEVEL = "issue_security_level"
SECURITY_LEVEL_MEMBERS = "issue_security_members"
PROJECT_ROLE_MEMBERS_BY_ROLE_ID = "project_role_members_by_role_id"
ALL_FIELDS = "all_fields"
URLS = {
    PING: "rest/api/2/myself",
    PROJECT: "rest/api/2/project?expand=description,lead,url",
    PROJECT_BY_KEY: "rest/api/2/project/{key}",
    ISSUES: "rest/api/3/search/jql?jql={jql}&fields=*all&maxResults={max_results}",
    ISSUE_DATA: "rest/api/2/issue/{id}",
    ATTACHMENT_CLOUD: "rest/api/2/attachment/content/{attachment_id}",
    ATTACHMENT_SERVER: "secure/attachment/{attachment_id}/{attachment_name}",
    USERS: "rest/api/3/users/search",
    USERS_FOR_DATA_CENTER: "rest/api/latest/user/search?username=''&startAt={start_at}&maxResults={max_results}",  # we can fetch only 1000 users for jira data center. Refer this doc see the limitations: https://auth0.com/docs/manage-users/user-search/retrieve-users-with-get-users-endpoint#limitations
    PERMISSIONS_BY_KEY: "rest/api/2/user/permission/search?{key}&permissions=BROWSE&maxResults={max_results}&startAt={start_at}",
    PROJECT_ROLE_MEMBERS_BY_ROLE_ID: "rest/api/3/project/{project_key}/role/{role_id}",
    ISSUE_SECURITY_LEVEL: "rest/api/2/issue/{issue_key}?fields=security",
    SECURITY_LEVEL_MEMBERS: "rest/api/3/issuesecurityschemes/level/member?maxResults={max_results}&startAt={start_at}&levelId={level_id}&expand=user,group,projectRole",
    ALL_FIELDS: "rest/api/2/field",
}
JIRA_CLOUD = "jira_cloud"
JIRA_SERVER = "jira_server"
JIRA_DATA_CENTER = "jira_data_center"
ATLASSIAN = "atlassian"
USER_QUERY = "expand=groups,applicationRoles"
