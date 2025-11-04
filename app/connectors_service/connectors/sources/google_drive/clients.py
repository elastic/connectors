#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from connectors.sources.shared.google import (
    GoogleServiceAccountClient,
    remove_universe_domain,
)

DRIVE_API_TIMEOUT = 1 * 60  # 1 min

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

DRIVE_ITEMS_FIELDS = "id,createdTime,driveId,modifiedTime,name,size,mimeType,fileExtension,webViewLink,owners,parents,trashed,trashedTime"
DRIVE_ITEMS_FIELDS_WITH_PERMISSIONS = f"{DRIVE_ITEMS_FIELDS},permissions"


class SyncCursorEmpty(Exception):
    """Exception class to notify that incremental sync can't run because sync_cursor is empty."""

    pass


class GoogleDriveClient(GoogleServiceAccountClient):
    """A google drive client to handle api calls made to Google Drive API."""

    def __init__(self, json_credentials, subject=None):
        """Initialize the GoogleApiClient superclass.

        Args:
            json_credentials (dict): Service account credentials json.
        """

        remove_universe_domain(json_credentials)
        if subject:
            json_credentials["subject"] = subject

        super().__init__(
            json_credentials=json_credentials,
            api="drive",
            api_version="v3",
            scopes=[
                "https://www.googleapis.com/auth/drive.readonly",
                "https://www.googleapis.com/auth/drive.metadata.readonly",
            ],
            api_timeout=DRIVE_API_TIMEOUT,
        )

    async def ping(self):
        return await self.api_call(resource="about", method="get", fields="kind")

    async def list_drives(self):
        """Fetch all shared drive (id, name) from Google Drive

        Yields:
            dict: Shared drive metadata.
        """

        async for drive in self.api_call_paged(
            resource="drives",
            method="list",
            fields="nextPageToken,drives(id,name)",
            pageSize=100,
        ):
            yield drive

    async def get_all_drives(self):
        """Retrieves all shared drives from Google Drive

        Returns:
            dict: mapping between drive id and its name
        """
        drives = {}
        async for page in self.list_drives():
            drives_chunk = page.get("drives", [])
            for drive in drives_chunk:
                drives[drive["id"]] = drive["name"]

        return drives

    async def list_folders(self):
        """Fetch all folders (id, name, parent) from Google Drive

        Yields:
            dict: Folder metadata.
        """
        async for folder in self.api_call_paged(
            resource="files",
            method="list",
            corpora="allDrives",
            fields="nextPageToken,files(id,name,parents)",
            q=f"mimeType='{FOLDER_MIME_TYPE}' and trashed=false",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=1000,
        ):
            yield folder

    async def get_all_folders(self):
        """Retrieves all folders from Google Drive

        Returns:
            dict: mapping between folder id and its (name, parents)
        """
        folders = {}
        async for page in self.list_folders():
            folders_chunk = page.get("files", [])
            for folder in folders_chunk:
                folders[folder["id"]] = {
                    "name": folder["name"],
                    "parents": folder.get("parents", None),
                }

        return folders

    async def list_files(self, fetch_permissions=False, last_sync_time=None):
        """Get files from Google Drive. Files can have any type.

        Args:
            include_permissions (bool): flag to select permissions in the request query
            last_sync_time (str): time when last sync happened

        Yields:
            dict: Documents from Google Drive.
        """

        files_fields = (
            DRIVE_ITEMS_FIELDS_WITH_PERMISSIONS
            if fetch_permissions
            else DRIVE_ITEMS_FIELDS
        )
        if last_sync_time is None:
            list_query = "trashed=false"
        else:
            list_query = f"trashed=true or modifiedTime > '{last_sync_time}' or createdTime > '{last_sync_time}'"
        async for file in self.api_call_paged(
            resource="files",
            method="list",
            corpora="allDrives",
            q=list_query,
            orderBy="modifiedTime desc",
            fields=f"files({files_fields}),incompleteSearch,nextPageToken",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=100,
        ):
            yield file

    async def list_files_from_my_drive(
        self, fetch_permissions=False, last_sync_time=None
    ):
        """Retrieves files from Google Drive, with an option to fetch permissions (DLS).

        This function optimizes the retrieval process based on the 'fetch_permissions' flag.
        If 'fetch_permissions' is True, the function filters for files the user can edit
        ("trashed=false and 'me' in writers") as permission fetching requires write access.
        If 'fetch_permissions' is False, it simply filters out trashed files ("trashed=false"),
        allowing a broader file retrieval.

        Args:
            include_permissions (bool): flag to select permissions in the request query
            last_sync_time (str): time when last sync happened

        Yields:
            dict: Documents from Google Drive.
        """

        if fetch_permissions and last_sync_time:
            files_fields = DRIVE_ITEMS_FIELDS_WITH_PERMISSIONS
            list_query = f"(trashed=true or modifiedTime > '{last_sync_time}' or createdTime > '{last_sync_time}') and 'me' in writers"
        elif fetch_permissions and not last_sync_time:
            files_fields = DRIVE_ITEMS_FIELDS_WITH_PERMISSIONS
            # Google Drive API required write access to fetch file's permissions
            list_query = "trashed=false and 'me' in writers"
        elif not fetch_permissions and last_sync_time:
            files_fields = DRIVE_ITEMS_FIELDS
            list_query = f"trashed=true or modifiedTime > '{last_sync_time}' or createdTime > '{last_sync_time}'"
        else:
            files_fields = DRIVE_ITEMS_FIELDS
            list_query = "trashed=false"

        async for file in self.api_call_paged(
            resource="files",
            method="list",
            corpora="user",
            q=list_query,
            orderBy="modifiedTime desc",
            fields=f"files({files_fields}),incompleteSearch,nextPageToken",
            includeItemsFromAllDrives=False,
            supportsAllDrives=False,
            pageSize=100,
        ):
            yield file

    async def list_permissions(self, file_id):
        """Get permissions for a given file ID from Google Drive.

        Args:
            file_id (str): File ID

        Yields:
            dictionary: Permissions from Google Drive for a file.
        """
        async for permission in self.api_call_paged(
            resource="permissions",
            method="list",
            fileId=file_id,
            fields="permissions(type,emailAddress,domain),nextPageToken",
            supportsAllDrives=True,
            pageSize=100,
        ):
            yield permission


class GoogleAdminDirectoryClient(GoogleServiceAccountClient):
    """A google admin directory client to handle api calls made to Google Admin API."""

    def __init__(self, json_credentials, subject):
        """Initialize the GoogleApiClient superclass.

        Args:
            json_credentials (dict): Service account credentials json.
            subject (str): For service accounts with domain-wide delegation enabled. A user
                           account to impersonate - e.g "admin@your-organization.com"
        """

        remove_universe_domain(json_credentials)
        if subject:
            json_credentials["subject"] = subject

        super().__init__(
            json_credentials=json_credentials,
            api="admin",
            api_version="directory_v1",
            scopes=[
                "https://www.googleapis.com/auth/admin.directory.group.readonly",
                "https://www.googleapis.com/auth/admin.directory.user.readonly",
            ],
            api_timeout=DRIVE_API_TIMEOUT,
        )
        self.domain = _get_domain_from_email(subject)

    async def list_users(self):
        """Get files from Google Drive. Files can have any type.

        Yields:
            dict: Documents from Google Drive.
        """
        async for user in self.api_call_paged(
            resource="users",
            method="list",
            domain=self.domain,
            fields="kind,users(id,name,primaryEmail),nextPageToken",
        ):
            yield user

    async def users(self):
        async for users_page in self.list_users():
            for user in users_page.get("users", []):
                yield user

    async def list_groups_for_user(self, user_id):
        """Get files from Google Drive. Files can have any type.

        Yields:
            dict: Documents from Google Drive.
        """
        async for group in self.api_call_paged(
            resource="groups",
            method="list",
            userKey=user_id,
            fields="kind,groups(email),nextPageToken",
        ):
            yield group


def _get_domain_from_email(email):
    return email.split("@")[-1]
