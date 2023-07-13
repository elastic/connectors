#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Google Drive source class methods.
"""
import asyncio
import re
from unittest import mock

import pytest
from aiogoogle import Aiogoogle, HTTPError
from aiogoogle.auth.managers import ServiceAccountManager
from aiogoogle.models import Request, Response

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.google_drive import RETRIES, GoogleDriveDataSource

SERVICE_ACCOUNT_CREDENTIALS = '{"project_id": "dummy123"}'

MORE_THAN_DEFAULT_FILE_SIZE_LIMIT = 10485760 + 1


def get_google_drive_source_object():
    """Creates the mocked Google Drive source object.

    Returns:
        GoogleDriveDataSource: Mocked object of the data source class.
    """
    configuration = DataSourceConfiguration(
        {"service_account_credentials": SERVICE_ACCOUNT_CREDENTIALS}
    )
    mocked_google_drive_object = GoogleDriveDataSource(configuration=configuration)
    return mocked_google_drive_object


@pytest.mark.asyncio
async def test_empty_configuration():
    """Tests the validity of the configurations passed to the Google Drive source class."""

    # Setup
    configuration = DataSourceConfiguration({"service_account_credentials": ""})
    gd_object = GoogleDriveDataSource(configuration=configuration)

    # Execute
    with pytest.raises(
        ConfigurableFieldValueError,
        match="Field validation errors: 'Service_account_credentials' cannot be empty.",
    ):
        await gd_object.validate_config()


@pytest.mark.asyncio
async def test_raise_on_invalid_configuration():
    """Test if invalid configuration raises an expected Exception"""

    # Setup
    configuration = DataSourceConfiguration(
        {"service_account_credentials": "{'abc':'bcd','cd'}"}
    )
    gd_object = GoogleDriveDataSource(configuration=configuration)

    # Execute
    with pytest.raises(
        ConfigurableFieldValueError,
        match="Google Drive service account is not a valid JSON",
    ):
        await gd_object.validate_config()


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    """Tests the ping functionality for ensuring connection to Google Drive."""

    # Setup

    expected_response = {
        "kind": "drive#about",
    }
    mocked_gd_object = get_google_drive_source_object()
    as_service_account_response = asyncio.Future()
    as_service_account_response.set_result(expected_response)

    # Execute

    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=as_service_account_response
    ):
        await mocked_gd_object.ping()


@mock.patch("connectors.utils.apply_retry_strategy", mock.AsyncMock())
@pytest.mark.asyncio
async def test_ping_for_failed_connection():
    """Tests the ping functionality when connection can not be established to Google Drive."""

    # Setup

    mocked_gd_object = get_google_drive_source_object()

    # Execute
    with mock.patch.object(
        Aiogoogle, "discover", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await mocked_gd_object.ping()


@pytest.mark.parametrize(
    "files, expected_files",
    [
        (
            [
                {
                    "kind": "drive#fileList",
                    "incompleteSearch": False,
                    "files": [
                        {
                            "kind": "drive#file",
                            "mimeType": "text/plain",
                            "id": "id1",
                            "name": "test.txt",
                            "parents": ["0APU6durKUAiqUk9PVA"],
                            "size": "28",
                            "modifiedTime": "2023-06-28T07:46:28.000Z",
                        }
                    ],
                }
            ],
            [
                {
                    "_id": "id1",
                    "created_at": None,
                    "last_updated": "2023-06-28T07:46:28.000Z",
                    "name": "test.txt",
                    "size": "28",
                    "_timestamp": "2023-06-28T07:46:28.000Z",
                    "mime_type": "text/plain",
                    "file_extension": None,
                    "url": None,
                    "type": "file",
                }
            ],
        )
    ],
)
@pytest.mark.asyncio
async def test_prepare_files(files, expected_files):
    """Tests the function which modifies the fetched blobs and maps the values to keys."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()

    processed_files = []

    # Execute
    async for file in mocked_gd_object.prepare_files(files_page=files[0], paths=dict()):
        processed_files.append(file)

    # Assert
    assert processed_files == expected_files


@pytest.mark.parametrize(
    "file, expected_file",
    [
        (
            {
                "kind": "drive#file",
                "mimeType": "text/plain",
                "id": "id1",
                "name": "test.txt",
                "parents": ["0APU6durKUAiqUk9PVA"],
                "size": "28",
                "modifiedTime": "2023-06-28T07:46:28.000Z",
            },
            {
                "_id": "id1",
                "created_at": None,
                "last_updated": "2023-06-28T07:46:28.000Z",
                "name": "test.txt",
                "size": "28",
                "_timestamp": "2023-06-28T07:46:28.000Z",
                "mime_type": "text/plain",
                "file_extension": None,
                "url": None,
                "type": "file",
            },
        ),
        (
            {
                "kind": "drive#file",
                "mimeType": "text/plain",
                "id": "id1",
                "name": "test.txt",
                "parents": ["0APU6durKUAiqUk9PVA"],
                "size": None,
                "modifiedTime": "2023-06-28T07:46:28.000Z",
            },
            {
                "_id": "id1",
                "created_at": None,
                "last_updated": "2023-06-28T07:46:28.000Z",
                "name": "test.txt",
                "size": 0,
                "_timestamp": "2023-06-28T07:46:28.000Z",
                "mime_type": "text/plain",
                "file_extension": None,
                "url": None,
                "type": "file",
            },
        ),
        (
            {
                "kind": "drive#file",
                "mimeType": "text/plain",
                "id": "id1",
                "name": "test.txt",
                "parents": ["0APU6durKUAiqUk9PVA"],
                "size": None,
                "modifiedTime": "2023-06-28T07:46:28.000Z",
                "owners": [
                    {
                        "displayName": "Test User",
                        "kind": "drive#user",
                        "emailAddress": "user@test.com",
                        "photoLink": "dummy_link",
                    }
                ],
            },
            {
                "_id": "id1",
                "created_at": None,
                "last_updated": "2023-06-28T07:46:28.000Z",
                "name": "test.txt",
                "size": 0,
                "_timestamp": "2023-06-28T07:46:28.000Z",
                "mime_type": "text/plain",
                "file_extension": None,
                "url": None,
                "type": "file",
                "author": "Test User",
                "created_by": "Test User",
                "created_by_email": "user@test.com",
            },
        ),
        (
            {
                "kind": "drive#file",
                "mimeType": "text/plain",
                "id": "id1",
                "name": "test.txt",
                "parents": ["folderId4"],
                "size": None,
                "modifiedTime": "2023-06-28T07:46:28.000Z",
                "owners": [
                    {
                        "displayName": "Test User",
                        "kind": "drive#user",
                        "emailAddress": "user@test.com",
                        "photoLink": "dummy_link",
                    }
                ],
                "lastModifyingUser": {
                    "displayName": "Test User 2",
                    "kind": "drive#user",
                    "emailAddress": "user2@test.com",
                    "photoLink": "dummy_link",
                },
            },
            {
                "_id": "id1",
                "created_at": None,
                "last_updated": "2023-06-28T07:46:28.000Z",
                "name": "test.txt",
                "size": 0,
                "_timestamp": "2023-06-28T07:46:28.000Z",
                "mime_type": "text/plain",
                "file_extension": None,
                "url": None,
                "type": "file",
                "author": "Test User",
                "created_by": "Test User",
                "created_by_email": "user@test.com",
                "updated_by": "Test User 2",
                "updated_by_email": "user2@test.com",
                "updated_by_photo_url": "dummy_link",
                "path": "Drive3/Folder4/test.txt",
            },
        ),
    ],
)
@pytest.mark.asyncio
async def test_prepare_file(file, expected_file):
    """Test the method that formats the blob metadata from Google Drive API"""
    # Setup
    mocked_gd_object = get_google_drive_source_object()

    dummy_paths = {
        "folderId4": {
            "name": "Folder4",
            "parents": ["driveId3"],
            "path": "Drive3/Folder4",
        }
    }

    # Execute and Assert
    assert expected_file == await mocked_gd_object.prepare_file(
        file=file, paths=dummy_paths
    )


@pytest.mark.asyncio
async def test_get_drives():
    """Tests the method which lists the shared drives from Google Drive."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()
    expected_response = {
        "kind": "drive#driveList",
        "drives": [
            {"id": "0ABHLjfsUwpHFUk9PVA", "name": "Test Drive", "kind": "drive#drive"},
        ],
    }
    expected_drives_list = [
        {
            "kind": "drive#driveList",
            "drives": [
                {
                    "id": "0ABHLjfsUwpHFUk9PVA",
                    "name": "Test Drive",
                    "kind": "drive#drive",
                },
            ],
        }
    ]
    dummy_url = "https://www.googleapis.com/drive/v3/drives"

    expected_response_object = Response(
        status_code=200,
        url=dummy_url,
        json=expected_response,
        req=Request(method="GET", url=dummy_url),
    )

    # Execute
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=expected_response_object
    ):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            drives_list = []
            async for drive in mocked_gd_object.google_drive_client.get_drives():
                drives_list.append(drive)

    # Assert
    assert drives_list == expected_drives_list


@pytest.mark.asyncio
async def test_get_folders():
    """Tests the method which lists the folders from Google Drive."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()
    expected_response = {
        "kind": "drive#fileList",
        "files": [
            {
                "kind": "drive#file",
                "mimeType": "application/vnd.google-apps.folder",
                "id": "1kGzmOTZgherwS9ODxZNC-owji_QZGGRU",
                "name": "test",
            }
        ],
    }
    expected_folders_list = [
        {
            "kind": "drive#fileList",
            "files": [
                {
                    "kind": "drive#file",
                    "mimeType": "application/vnd.google-apps.folder",
                    "id": "1kGzmOTZgherwS9ODxZNC-owji_QZGGRU",
                    "name": "test",
                }
            ],
        }
    ]
    dummy_url = "https://www.googleapis.com/drive/v3/files"

    expected_response_object = Response(
        status_code=200,
        url=dummy_url,
        json=expected_response,
        req=Request(method="GET", url=dummy_url),
    )

    # Execute
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=expected_response_object
    ):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            folders_list = []
            async for folder in mocked_gd_object.google_drive_client.get_folders():
                folders_list.append(folder)

    # Assert
    assert folders_list == expected_folders_list


@pytest.mark.asyncio
async def test_resolve_paths():
    """Test the method that builds a lookup between a folder id and its absolute path in Google Drive structure"""
    drives = {
        "driveId1": "Drive1",
        "driveId2": "Drive2",
        "driveId3": "Drive3",
    }
    drives_future = asyncio.Future()
    drives_future.set_result(drives)

    folders = {
        "folderId1": {"name": "Folder1", "parents": ["driveId1"]},
        "folderId2": {"name": "Folder2", "parents": ["folderId1"]},
        "folderId3": {"name": "Folder3", "parents": ["folderId2"]},
        "folderId4": {"name": "Folder4", "parents": ["driveId3"]},
    }

    expected_paths = {
        "folderId1": {
            "name": "Folder1",
            "parents": ["driveId1"],
            "path": "Drive1/Folder1",
        },
        "folderId2": {
            "name": "Folder2",
            "parents": ["folderId1"],
            "path": "Drive1/Folder1/Folder2",
        },
        "folderId3": {
            "name": "Folder3",
            "parents": ["folderId2"],
            "path": "Drive1/Folder1/Folder2/Folder3",
        },
        "folderId4": {
            "name": "Folder4",
            "parents": ["driveId3"],
            "path": "Drive3/Folder4",
        },
        "driveId1": {"name": "Drive1", "parents": [], "path": "Drive1"},
        "driveId2": {"name": "Drive2", "parents": [], "path": "Drive2"},
        "driveId3": {"name": "Drive3", "parents": [], "path": "Drive3"},
    }

    folders_future = asyncio.Future()
    folders_future.set_result(folders)

    mocked_gd_object = get_google_drive_source_object()
    mocked_gd_object.google_drive_client.retrieve_all_drives = mock.MagicMock(
        return_value=drives_future
    )
    mocked_gd_object.google_drive_client.retrieve_all_folders = mock.MagicMock(
        return_value=folders_future
    )

    paths = await mocked_gd_object.resolve_paths()

    assert paths == expected_paths


@pytest.mark.asyncio
async def test_fetch_files():
    """Tests the method responsible to yield files from Google Drive."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()
    expected_response = {
        "kind": "drive#fileList",
        "files": [
            {
                "kind": "drive#file",
                "mimeType": "text/plain",
                "id": "id1",
                "name": "test",
            }
        ],
    }
    expected_files_list = [
        {
            "kind": "drive#fileList",
            "files": [
                {
                    "kind": "drive#file",
                    "mimeType": "text/plain",
                    "id": "id1",
                    "name": "test",
                }
            ],
        }
    ]
    dummy_url = "https://www.googleapis.com/drive/v3/files"

    expected_response_object = Response(
        status_code=200,
        url=dummy_url,
        json=expected_response,
        req=Request(method="GET", url=dummy_url),
    )

    # Execute
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=expected_response_object
    ):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            files_list = []
            async for file in mocked_gd_object.google_drive_client.get_folders():
                files_list.append(file)

    # Assert
    assert files_list == expected_files_list


@pytest.mark.asyncio
async def test_get_docs():
    """Tests the module responsible to fetch and yield blobs documents from Google Drive."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()
    expected_response = {
        "kind": "drive#fileList",
        "files": [
            {
                "kind": "drive#file",
                "mimeType": "text/plain",
                "id": "id1",
                "name": "test.txt",
                "parents": ["0APU6durKUAiqUk9PVA"],
                "size": "28",
                "modifiedTime": "2023-06-28T07:46:28.000Z",
            }
        ],
    }
    expected_blob_document = {
        "_id": "id1",
        "created_at": None,
        "last_updated": "2023-06-28T07:46:28.000Z",
        "name": "test.txt",
        "size": "28",
        "_timestamp": "2023-06-28T07:46:28.000Z",
        "mime_type": "text/plain",
        "file_extension": None,
        "url": None,
        "type": "file",
    }
    dummy_url = "https://www.googleapis.com/drive/v3/files"

    expected_response_object = Response(
        status_code=200,
        url=dummy_url,
        json=expected_response,
        req=Request(method="GET", url=dummy_url),
    )

    # Execute and Assert
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=expected_response_object
    ):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            async for blob_document in mocked_gd_object.get_docs():
                assert blob_document[0] == expected_blob_document


@pytest.mark.asyncio
async def test_get_content():
    """Test the module responsible for fetching the content of the file if it is extractable."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()
    blob_document = {
        "id": "id1",
        "created_at": None,
        "last_updated": "2023-06-28T07:46:28.000Z",
        "name": "test.txt",
        "size": 28,
        "_timestamp": "2023-06-28T07:46:28.000Z",
        "mime_type": "text/plain",
        "file_extension": "txt",
        "url": None,
        "type": "file",
    }
    expected_blob_document = {
        "_id": "id1",
        "_timestamp": "2023-06-28T07:46:28.000Z",
        "_attachment": "",
    }
    blob_content_response = ""

    # Execute and Assert
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=blob_content_response
    ):
        async with Aiogoogle(
            service_account_creds=mocked_gd_object.google_drive_client.service_account_credentials
        ) as google_client:
            drive_client = await google_client.discover(
                api_name="drive", api_version="v3"
            )
            drive_client.files = mock.MagicMock()
            content = await mocked_gd_object.get_content(
                file=blob_document,
                doit=True,
            )
            assert content == expected_blob_document


@pytest.mark.asyncio
async def test_get_content_doit_false():
    """Test the module responsible for fetching the content of the file with `doit` set to False"""

    # Setup
    mocked_gd_object = get_google_drive_source_object()
    blob_document = {
        "id": "id1",
        "created_at": None,
        "last_updated": "2023-06-28T07:46:28.000Z",
        "name": "test.txt",
        "size": 28,
        "_timestamp": "2023-06-28T07:46:28.000Z",
        "mime_type": "text/plain",
        "file_extension": "txt",
        "url": None,
        "type": "file",
    }

    # Execute and Assert
    content = await mocked_gd_object.get_content(
        file=blob_document,
        doit=False,
    )
    assert content is None


@pytest.mark.asyncio
async def test_get_content_google_workspace_called():
    """Test the method responsible for selecting right extraction method depending on MIME type"""

    # Setup
    mocked_gd_object = get_google_drive_source_object()

    timestamp = "1234"

    blob_document = {
        "id": "id1",
        "created_at": None,
        "last_updated": "2023-06-28T07:46:28.000Z",
        "name": "Google docs test",
        "size": 28,
        "_timestamp": timestamp,
        "mime_type": "application/vnd.google-apps.document",
        "file_extension": None,
        "url": None,
        "type": "file",
    }
    expected_content = {
        "_id": "id1",
        "_timestamp": timestamp,
        "_attachment": "Test content",
    }
    expected_content_future = asyncio.Future()
    expected_content_future.set_result(expected_content)

    mocked_gd_object.get_google_workspace_content = mock.MagicMock(
        return_value=expected_content_future
    )
    mocked_gd_object.get_generic_file_content = mock.MagicMock()

    # Execute and Assert
    await mocked_gd_object.get_content(
        file=blob_document,
        timestamp=timestamp,
        doit=True,
    )
    mocked_gd_object.get_google_workspace_content.assert_called_once_with(
        blob_document, timestamp=timestamp
    )
    mocked_gd_object.get_generic_file_content.assert_not_called()


@pytest.mark.asyncio
async def test_get_content_generic_files_called():
    """Test the method responsible for selecting right extraction method depending on MIME type"""

    # Setup
    mocked_gd_object = get_google_drive_source_object()

    timestamp = "1234"

    blob_document = {
        "id": "id1",
        "created_at": None,
        "last_updated": "2023-06-28T07:46:28.000Z",
        "name": "text.txt",
        "size": 28,
        "_timestamp": timestamp,
        "mime_type": "text/plain",
        "file_extension": "txt",
        "url": None,
        "type": "file",
    }
    expected_content = {
        "_id": "id1",
        "_timestamp": timestamp,
        "_attachment": "Test content",
    }
    expected_content_future = asyncio.Future()
    expected_content_future.set_result(expected_content)

    mocked_gd_object.get_google_workspace_content = mock.MagicMock()
    mocked_gd_object.get_generic_file_content = mock.MagicMock(
        return_value=expected_content_future
    )

    # Execute and Assert
    await mocked_gd_object.get_content(
        file=blob_document,
        timestamp=timestamp,
        doit=True,
    )
    mocked_gd_object.get_google_workspace_content.assert_not_called()
    mocked_gd_object.get_generic_file_content.assert_called_once_with(
        blob_document, timestamp=timestamp
    )


@pytest.mark.asyncio
async def test_get_google_workspace_content():
    """Test the module responsible for fetching the content of the Google Suite document."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()
    blob_document = {
        "id": "id1",
        "created_at": None,
        "last_updated": "2023-06-28T07:46:28.000Z",
        "name": "test.txt",
        "size": 28,
        "_timestamp": "2023-06-28T07:46:28.000Z",
        "mime_type": "application/vnd.google-apps.document",
        "file_extension": None,
        "url": None,
        "type": "file",
    }
    expected_blob_document = {
        "_id": "id1",
        "_timestamp": "2023-06-28T07:46:28.000Z",
        "_attachment": "I love unit tests",
    }
    blob_content_response = ("I love unit tests", 1234)
    future_blob_content_response = asyncio.Future()
    future_blob_content_response.set_result(blob_content_response)

    # Execute and Assert
    mocked_gd_object._download_content = mock.MagicMock(
        return_value=future_blob_content_response
    )
    content = await mocked_gd_object.get_content(
        file=blob_document,
        doit=True,
    )
    assert content == expected_blob_document


@pytest.mark.asyncio
async def test_get_google_workspace_content_size_limit():
    """Test the module responsible for fetching the content of the Google Suite document if its size
    is above the limit."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()
    blob_document = {
        "id": "id1",
        "created_at": None,
        "last_updated": "2023-06-28T07:46:28.000Z",
        "name": "test.txt",
        "size": 28,
        "_timestamp": "2023-06-28T07:46:28.000Z",
        "mime_type": "application/vnd.google-apps.document",
        "file_extension": None,
        "url": None,
        "type": "file",
    }

    blob_content_response = ("I love unit tests", MORE_THAN_DEFAULT_FILE_SIZE_LIMIT)
    future_blob_content_response = asyncio.Future()
    future_blob_content_response.set_result(blob_content_response)

    # Execute and Assert
    mocked_gd_object._download_content = mock.MagicMock(
        return_value=future_blob_content_response
    )
    content = await mocked_gd_object.get_content(
        file=blob_document,
        doit=True,
    )
    assert content is None


@pytest.mark.asyncio
async def test_get_generic_file_content():
    """Test the module responsible for fetching the content of the file if it is extractable."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()
    blob_document = {
        "id": "id1",
        "created_at": None,
        "last_updated": "2023-06-28T07:46:28.000Z",
        "name": "test.txt",
        "size": 28,
        "_timestamp": "2023-06-28T07:46:28.000Z",
        "mime_type": "text/plain",
        "file_extension": "txt",
        "url": None,
        "type": "file",
    }
    expected_blob_document = {
        "_id": "id1",
        "_timestamp": "2023-06-28T07:46:28.000Z",
        "_attachment": "I love unit tests generic file",
    }
    blob_content_response = ("I love unit tests generic file", 1234)
    future_blob_content_response = asyncio.Future()
    future_blob_content_response.set_result(blob_content_response)

    # Execute and Assert
    mocked_gd_object._download_content = mock.MagicMock(
        return_value=future_blob_content_response
    )
    content = await mocked_gd_object.get_content(
        file=blob_document,
        doit=True,
    )
    assert content == expected_blob_document


@pytest.mark.asyncio
async def test_get_generic_file_content_size_limit():
    """Test the module responsible for fetching the content of the file size is above the limit."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()
    blob_document = {
        "id": "id1",
        "created_at": None,
        "last_updated": "2023-06-28T07:46:28.000Z",
        "name": "test.txt",
        "size": MORE_THAN_DEFAULT_FILE_SIZE_LIMIT,
        "_timestamp": "2023-06-28T07:46:28.000Z",
        "mime_type": "text/plain",
        "file_extension": "txt",
        "url": None,
        "type": "file",
    }

    # Execute and Assert
    mocked_gd_object._download_content = mock.MagicMock()
    content = await mocked_gd_object.get_content(
        file=blob_document,
        doit=True,
    )
    assert content is None


@pytest.mark.asyncio
async def test_get_generic_file_content_empty_file():
    """Test the module responsible for fetching the content of the file if the file size is 0."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()
    blob_document = {
        "id": "id1",
        "created_at": None,
        "last_updated": "2023-06-28T07:46:28.000Z",
        "name": "test.txt",
        "size": 0,
        "_timestamp": "2023-06-28T07:46:28.000Z",
        "mime_type": "text/plain",
        "file_extension": "txt",
        "url": None,
        "type": "file",
    }

    # Execute and Assert
    mocked_gd_object._download_content = mock.MagicMock()
    content = await mocked_gd_object.get_content(
        file=blob_document,
        doit=True,
    )
    assert content is None


@pytest.mark.asyncio
async def test_get_content_when_type_not_supported():
    """Test the module responsible for fetching the content of the file if it is not extractable."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()
    blob_document = {
        "id": "id1",
        "created_at": None,
        "last_updated": "2023-06-28T07:46:28.000Z",
        "name": "test.xd",
        "size": 28,
        "_timestamp": "2023-06-28T07:46:28.000Z",
        "mime_type": "text/plain",
        "file_extension": "xd",
        "url": None,
        "type": "file",
    }

    # Execute and Assert
    async with Aiogoogle(
        service_account_creds=mocked_gd_object.google_drive_client.service_account_credentials
    ) as google_client:
        drive_client = await google_client.discover(
            api_name="drive", api_version="v3"
        )
        drive_client.files = mock.MagicMock()
        content = await mocked_gd_object.get_content(
            file=blob_document,
            doit=True,
        )
        assert content is None


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_api_call_for_attribute_error(mock_apply_retry_strategy):
    """Tests the api_call method when resource attribute is not present in the getattr."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()

    # Execute
    with pytest.raises(AttributeError):
        await mocked_gd_object._google_drive_client.api_call(
            resource="buckets_dummy", method="list"
        )


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_api_call_http_error(mock_apply_retry_strategy):
    """Test handling retries for HTTPError exception in api_call() method."""
    # Setup
    mocked_gd_object = get_google_drive_source_object()

    with mock.patch.object(
        Aiogoogle,
        "as_service_account",
        side_effect=HTTPError("Ooops exception", res=mock.MagicMock()),
    ):
        with pytest.raises(HTTPError):
            await mocked_gd_object.ping()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_api_call_other_exception(mock_apply_retry_strategy):
    """Test handling retries for generic Exception in api_call() method."""
    # Setup
    mocked_gd_object = get_google_drive_source_object()

    with mock.patch.object(
        Aiogoogle, "as_service_account", side_effect=Exception("other")
    ):
        with pytest.raises(Exception):
            await mocked_gd_object.ping()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_api_call_ping_retries(mock_apply_retry_strategy, mock_responses):
    """Test handling retries for generic Exception in api_call() method."""
    # Setup
    mocked_gd_object = get_google_drive_source_object()

    mock_responses.get(url=re.compile(".*"), status=401)

    with pytest.raises(Exception):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            await mocked_gd_object.ping()

    # Expect retry function to be triggered the expected number of retries,
    # substract the first call
    assert mock_apply_retry_strategy.call_count == RETRIES - 1


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_api_call_get_drives_retries(mock_apply_retry_strategy, mock_responses):
    """Test handling retries for generic Exception in api_call() method."""
    # Setup
    mocked_gd_object = get_google_drive_source_object()

    mock_responses.get(url=re.compile(".*"), status=401)

    with pytest.raises(Exception):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            async for _ in mocked_gd_object.google_drive_client.get_drives():
                continue

    # Expect retry function to be triggered the expected number of retries,
    # substract the first call
    assert mock_apply_retry_strategy.call_count == RETRIES - 1


@pytest.mark.parametrize(
    "file, permissions, expected_file",
    [
        (
            {
                "kind": "drive#file",
                "mimeType": "text/plain",
                "id": "id1",
                "name": "test.txt",
                "parents": ["0APU6durKUAiqUk9PVA"],
                "size": "28",
                "modifiedTime": "2023-06-28T07:46:28.000Z",
                "driveId": "drive1",
            },
            [
                {"type": "user", "emailAddress": "user@xd.com"},
                {"type": "group", "emailAddress": "group@xd.com"},
                {"type": "domain", "domain": "xd.com"},
                {"type": "anyone"},
            ],
            {
                "_id": "id1",
                "created_at": None,
                "last_updated": "2023-06-28T07:46:28.000Z",
                "name": "test.txt",
                "size": "28",
                "_timestamp": "2023-06-28T07:46:28.000Z",
                "mime_type": "text/plain",
                "file_extension": None,
                "url": None,
                "type": "file",
                "shared_drive": "drive1",
                "_allow_access_control": [
                    "user:user@xd.com",
                    "group:group@xd.com",
                    "domain:xd.com",
                    "anyone",
                ],
            },
        ),
        (
            {
                "kind": "drive#file",
                "mimeType": "text/plain",
                "id": "id1",
                "name": "test.txt",
                "parents": ["0APU6durKUAiqUk9PVA"],
                "size": None,
                "modifiedTime": "2023-06-28T07:46:28.000Z",
                "driveId": "drive1",
            },
            [
                {"type": "user", "emailAddress": "user@xd.com"},
                {"type": "group", "emailAddress": "group@xd.com"},
                {"type": "domain", "domain": "xd.com"},
                {"type": "anyone"},
            ],
            {
                "_id": "id1",
                "created_at": None,
                "last_updated": "2023-06-28T07:46:28.000Z",
                "name": "test.txt",
                "size": 0,
                "_timestamp": "2023-06-28T07:46:28.000Z",
                "mime_type": "text/plain",
                "file_extension": None,
                "url": None,
                "type": "file",
                "shared_drive": "drive1",
                "_allow_access_control": [
                    "user:user@xd.com",
                    "group:group@xd.com",
                    "domain:xd.com",
                    "anyone",
                ],
            },
        ),
    ],
)
@pytest.mark.asyncio
async def test_prepare_file_on_shared_drive_with_dls_enabled(
    file, permissions, expected_file
):
    """Test the method that formats the blob metadata from Google Drive API"""
    # Setup

    mocked_gd_object = get_google_drive_source_object()

    mocked_gd_object._dls_enabled = mock.MagicMock(return_value=True)

    dummy_paths = {
        "folderId4": {
            "name": "Folder4",
            "parents": ["driveId3"],
            "path": "Drive3/Folder4",
        },
        "drive1": {"name": "drive1"},
    }

    expected_response_object = Response(
        status_code=200,
        url="dummy_url",
        json={"permissions": permissions},
        req=Request(method="GET", url="dummy_url"),
    )

    # Execute and Assert
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=expected_response_object
    ):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            assert expected_file == await mocked_gd_object.prepare_file(
                file=file, paths=dummy_paths
            )


@pytest.mark.parametrize(
    "file, expected_file",
    [
        (
            {
                "kind": "drive#file",
                "mimeType": "text/plain",
                "id": "id1",
                "name": "test.txt",
                "parents": ["0APU6durKUAiqUk9PVA"],
                "size": "28",
                "modifiedTime": "2023-06-28T07:46:28.000Z",
                "permissions": [
                    {"type": "user", "emailAddress": "user@xd.com"},
                    {"type": "group", "emailAddress": "group@xd.com"},
                    {"type": "domain", "domain": "xd.com"},
                    {"type": "anyone"},
                ],
            },
            {
                "_id": "id1",
                "created_at": None,
                "last_updated": "2023-06-28T07:46:28.000Z",
                "name": "test.txt",
                "size": "28",
                "_timestamp": "2023-06-28T07:46:28.000Z",
                "mime_type": "text/plain",
                "file_extension": None,
                "url": None,
                "type": "file",
                "_allow_access_control": [
                    "user:user@xd.com",
                    "group:group@xd.com",
                    "domain:xd.com",
                    "anyone",
                ],
            },
        ),
        (
            {
                "kind": "drive#file",
                "mimeType": "text/plain",
                "id": "id1",
                "name": "test.txt",
                "parents": ["0APU6durKUAiqUk9PVA"],
                "size": None,
                "modifiedTime": "2023-06-28T07:46:28.000Z",
                "permissions": [
                    {"type": "user", "emailAddress": "user@xd.com"},
                    {"type": "group", "emailAddress": "group@xd.com"},
                    {"type": "domain", "domain": "xd.com"},
                    {"type": "anyone"},
                ],
            },
            {
                "_id": "id1",
                "created_at": None,
                "last_updated": "2023-06-28T07:46:28.000Z",
                "name": "test.txt",
                "size": 0,
                "_timestamp": "2023-06-28T07:46:28.000Z",
                "mime_type": "text/plain",
                "file_extension": None,
                "url": None,
                "type": "file",
                "_allow_access_control": [
                    "user:user@xd.com",
                    "group:group@xd.com",
                    "domain:xd.com",
                    "anyone",
                ],
            },
        ),
    ],
)
@pytest.mark.asyncio
async def test_prepare_file_on_my_drive_with_dls_enabled(file, expected_file):
    """Test the method that formats the blob metadata from Google Drive API"""
    # Setup

    mocked_gd_object = get_google_drive_source_object()

    mocked_gd_object._dls_enabled = mock.MagicMock(return_value=True)

    dummy_paths = {
        "folderId4": {
            "name": "Folder4",
            "parents": ["driveId3"],
            "path": "Drive3/Folder4",
        }
    }

    # Execute and Assert
    assert expected_file == await mocked_gd_object.prepare_file(
        file=file, paths=dummy_paths
    )


@pytest.mark.parametrize(
    "user, groups, access_control_doc",
    [
        (
            {
                "id": "user1",
                "primaryEmail": "user1@test.com",
                "name": {"fullName": "User 1"},
            },
            [
                {"email": "group-1@test.com"},
                {"email": "group-2@test.com"},
                {"email": "group-3@test.com"},
            ],
            {
                "_id": "user1@test.com",
                "identity": {"name": "User 1", "email": "user1@test.com"},
                "query": {
                    "template": {
                        "params": {
                            "access_control": [
                                "user:user1@test.com",
                                "domain:test.com",
                                "group:group-1@test.com",
                                "group:group-2@test.com",
                                "group:group-3@test.com",
                            ]
                        }
                    },
                    "source": {
                        "bool": {
                            "filter": {
                                "bool": {
                                    "should": [
                                        {
                                            "bool": {
                                                "must_not": {
                                                    "exists": {
                                                        "field": "_allow_access_control"
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "terms": {
                                                "_allow_access_control.enum": [
                                                    "user:user1@test.com",
                                                    "domain:test.com",
                                                    "group:group-1@test.com",
                                                    "group:group-2@test.com",
                                                    "group:group-3@test.com",
                                                ]
                                            }
                                        },
                                    ]
                                }
                            }
                        }
                    },
                },
            },
        ),
    ],
)
@pytest.mark.asyncio
async def test_prepare_access_control_doc(user, groups, access_control_doc):
    """Test the method that formats the blob metadata from Google Drive API"""
    # Setup

    mocked_gd_object = get_google_drive_source_object()

    mocked_gd_object._dls_enabled = mock.MagicMock(return_value=True)

    expected_response_object = Response(
        status_code=200,
        url="dummy_url",
        json={"groups": groups},
        req=Request(method="GET", url="dummy_url"),
    )

    # Execute and Assert
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=expected_response_object
    ):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            assert (
                access_control_doc
                == await mocked_gd_object.prepare_single_access_control_document(
                    user=user
                )
            )


@pytest.mark.parametrize(
    "users_page, groups, access_control_docs",
    [
        (
            {
                "users": [
                    {
                        "id": "user1",
                        "primaryEmail": "user1@test.com",
                        "name": {"fullName": "User 1"},
                    }
                ]
            },
            [
                {"email": "group-1@test.com"},
                {"email": "group-2@test.com"},
                {"email": "group-3@test.com"},
            ],
            [
                {
                    "_id": "user1@test.com",
                    "identity": {"name": "User 1", "email": "user1@test.com"},
                    "query": {
                        "template": {
                            "params": {
                                "access_control": [
                                    "user:user1@test.com",
                                    "domain:test.com",
                                    "group:group-1@test.com",
                                    "group:group-2@test.com",
                                    "group:group-3@test.com",
                                ]
                            }
                        },
                        "source": {
                            "bool": {
                                "filter": {
                                    "bool": {
                                        "should": [
                                            {
                                                "bool": {
                                                    "must_not": {
                                                        "exists": {
                                                            "field": "_allow_access_control"
                                                        }
                                                    }
                                                }
                                            },
                                            {
                                                "terms": {
                                                    "_allow_access_control.enum": [
                                                        "user:user1@test.com",
                                                        "domain:test.com",
                                                        "group:group-1@test.com",
                                                        "group:group-2@test.com",
                                                        "group:group-3@test.com",
                                                    ]
                                                }
                                            },
                                        ]
                                    }
                                }
                            }
                        },
                    },
                },
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_prepare_access_control_documents(
    users_page, groups, access_control_docs
):
    """Test the method that formats the blob metadata from Google Drive API"""
    # Setup

    mocked_gd_object = get_google_drive_source_object()

    mocked_gd_object._dls_enabled = mock.MagicMock(return_value=True)

    expected_response_object = Response(
        status_code=200,
        url="dummy_url",
        json={"groups": groups},
        req=Request(method="GET", url="dummy_url"),
    )

    # Execute and Assert
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=expected_response_object
    ):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            assert access_control_docs[0] == await anext(
                mocked_gd_object.prepare_access_control_documents(users_page=users_page)
            )


@pytest.mark.asyncio
async def test_get_access_control_dls_disabled():

    mocked_gd_object = get_google_drive_source_object()

    mocked_gd_object._dls_enabled = mock.MagicMock(return_value=False)

    acl = []
    async for access_control in mocked_gd_object.get_access_control():
        acl.append(access_control)

    assert len(acl) == 0


@pytest.mark.asyncio
async def test_get_access_control_dls_enabled():
    """Tests the module responsible to fetch and yield blobs documents from Google Drive."""

    # Setup
    mocked_gd_object = get_google_drive_source_object()

    mocked_gd_object._dls_enabled = mock.MagicMock(return_value=True)

    mock_access_control = {'_id': 'user1@test.com', 'identity': {'name': 'User 1', 'email': 'user1@test.com'}, 'query': {}}

    mocked_gd_object.prepare_single_access_control_document = mock.AsyncMock(
        return_value=mock_access_control
    )
    expected_response = {
        "users": [
                    {
                        "id": "user1",
                        "primaryEmail": "user1@test.com",
                        "name": {"fullName": "User 1"},
                    }
                ]
    }
    expected_response_object = Response(
        status_code=200,
        url="dummy_url",
        json=expected_response,
        req=Request(method="GET", url="dummy_url"),
    )

    # Execute and Assert
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=expected_response_object
    ):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            async for access_control in mocked_gd_object.get_access_control():
                assert access_control == mock_access_control
