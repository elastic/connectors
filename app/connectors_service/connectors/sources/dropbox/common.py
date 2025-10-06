#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from enum import Enum

RETRY_COUNT = 3
DEFAULT_RETRY_AFTER = 300  # seconds
RETRY_INTERVAL = 2
MAX_CONCURRENT_DOWNLOADS = 100
REQUEST_BATCH_SIZE = 50
BATCH_RESPONSE_COUNT = 20
LIMIT = 300  # Limit for fetching shared files per call
FILE_LIMIT = 2000
PAPER = "paper"
FILE = "File"
FOLDER = "Folder"
RECEIVED_FILE = "Received File"
AUTHENTICATED_ADMIN_URL = (
    "https://api.dropboxapi.com/2/team/token/get_authenticated_admin"
)

API_VERSION = 2


class BreakingField(Enum):
    CURSOR = "cursor"
    HAS_MORE = "has_more"


class EndpointName(Enum):
    ACCESS_TOKEN = "access_token"  # noqa S105
    PING = "ping"
    CHECK_PATH = "check_path"
    FILES_FOLDERS = "files_folders"
    FILES_FOLDERS_CONTINUE = "files_folders_continue"
    FILE_MEMBERS = "file_members"
    FILE_MEMBERS_BATCH = "file_members_batch"
    FILE_MEMBERS_CONTINUE = "file_members_continue"
    FOLDER_MEMBERS = "folder_members"
    FOLDER_MEMBERS_CONTINUE = "folder_members_continue"
    RECEIVED_FILES = "received_files"
    RECEIVED_FILES_CONTINUE = "received_files_continue"
    RECEIVED_FILE_METADATA = "received_files_metadata"
    DOWNLOAD = "download"
    PAPER_FILE_DOWNLOAD = "paper_file_download"
    RECEIVED_FILE_DOWNLOAD = "received_files_download"
    SEARCH = "search"
    SEARCH_CONTINUE = "search_continue"
    MEMBERS = "members"
    MEMBERS_CONTINUE = "members_continue"
    AUTHENTICATED_ADMIN = "authenticated_admin"
    TEAM_FOLDER_LIST = "team_folder_list"
    TEAM_FOLDER_LIST_CONTINUE = "team_folder_list_continue"


ENDPOINTS = {
    EndpointName.ACCESS_TOKEN.value: "oauth2/token",
    EndpointName.PING.value: "users/get_current_account",
    EndpointName.CHECK_PATH.value: "files/get_metadata",
    EndpointName.FILES_FOLDERS.value: "files/list_folder",
    EndpointName.FILES_FOLDERS_CONTINUE.value: "files/list_folder/continue",
    EndpointName.FILE_MEMBERS.value: "sharing/list_file_members",
    EndpointName.FILE_MEMBERS_BATCH.value: "sharing/list_file_members/batch",
    EndpointName.FILE_MEMBERS_CONTINUE.value: "sharing/list_file_members/continue",
    EndpointName.FOLDER_MEMBERS.value: "sharing/list_folder_members",
    EndpointName.FOLDER_MEMBERS_CONTINUE.value: "sharing/list_folder_members/continue",
    EndpointName.RECEIVED_FILES.value: "sharing/list_received_files",
    EndpointName.RECEIVED_FILES_CONTINUE.value: "sharing/list_received_files/continue",
    EndpointName.RECEIVED_FILE_METADATA.value: "sharing/get_shared_link_metadata",
    EndpointName.DOWNLOAD.value: "files/download",
    EndpointName.PAPER_FILE_DOWNLOAD.value: "files/export",
    EndpointName.RECEIVED_FILE_DOWNLOAD.value: "sharing/get_shared_link_file",
    EndpointName.SEARCH.value: "files/search_v2",
    EndpointName.SEARCH_CONTINUE.value: "files/search/continue_v2",
    EndpointName.MEMBERS.value: "team/members/list_v2",
    EndpointName.MEMBERS_CONTINUE.value: "team/members/list/continue_v2",
    EndpointName.AUTHENTICATED_ADMIN.value: "team/token/get_authenticated_admin",
    EndpointName.TEAM_FOLDER_LIST.value: "team/team_folder/list",
    EndpointName.TEAM_FOLDER_LIST_CONTINUE.value: "team/team_folder/list/continue",
}


class InvalidClientCredentialException(Exception):
    pass


class InvalidRefreshTokenException(Exception):
    pass


class InvalidPathException(Exception):
    pass
