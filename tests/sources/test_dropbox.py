#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Dropbox source class methods"""
import datetime
from copy import copy
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from dropbox.exceptions import ApiError, AuthError, BadInputError
from dropbox.files import FileMetadata, FolderMetadata, ListFolderResult
from dropbox.sharing import (
    FileLinkMetadata,
    FolderPolicy,
    ListFilesResult,
    SharedFileMetadata,
)
from freezegun import freeze_time

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.dropbox import DropboxDataSource
from tests.commons import AsyncIterator
from tests.sources.support import AsyncSourceContextManager, create_source

PATH = "/"
DUMMY_VALUES = "abc#123"

MOCK_FILE_DATA = FileMetadata(
    id="file_1",
    name="test_file.txt",
    path_display="/test/test_file.txt",
    is_downloadable=True,
    server_modified=datetime.datetime(2023, 1, 1, 6, 6, 6),
    size=200,
)

MOCK_PAPER_FILE_DATA = FileMetadata(
    id="paper_1",
    name="dummy_file.paper",
    path_display="/test/dummy_file.paper",
    is_downloadable=False,
    server_modified=datetime.datetime(2023, 1, 1, 6, 6, 6),
    size=200,
)

MOCK_FOLDER_DATA = FolderMetadata(
    id="folder_1",
    name="test",
    path_display="/test",
)

MOCK_SHARED_FILE_DATA_1 = SharedFileMetadata(
    id="id:shared_1",
    name="shared_file_1.py",
    path_display="/shared/shared_file_1.py",
    preview_url="https://www.dropbox.com/scl/fi/12345/shared_file_1.py",
    time_invited=datetime.datetime(2023, 1, 1, 6, 6, 6),
    policy=FolderPolicy(),
)

MOCK_SHARED_FILE_DATA_2 = SharedFileMetadata(
    id="id:shared_2",
    name="shared_file_2.py",
    path_display="/shared/shared_file_2.py",
    preview_url="https://www.dropbox.com/scl/fi/12345/shared_file_2.py",
    time_invited=datetime.datetime(2023, 1, 1, 6, 6, 6),
    policy=FolderPolicy(),
)

FILE_LINK_METADATA = FileLinkMetadata(
    id="id:shared_1",
    name="shared_file_1.py",
    size=200,
    server_modified=datetime.datetime(2023, 1, 1, 6, 6, 6),
)

EXPECTED_FILE = {
    "_id": "file_1",
    "type": "File",
    "name": "test_file.txt",
    "file path": "/test/test_file.txt",
    "size": 200,
    "_timestamp": "2023-01-01T06:06:06",
}

EXPECTED_FOLDER = {
    "_id": "folder_1",
    "type": "Folder",
    "name": "test",
    "file path": "/test",
    "size": 0,
    "_timestamp": "2023-01-01T06:06:06+00:00",
}

EXPECTED_SHARED_FILE_1 = {
    "_id": "id:shared_1",
    "type": "File",
    "name": "shared_file_1.py",
    "file path": "/shared/shared_file_1.py",
    "url": "https://www.dropbox.com/scl/fi/12345/shared_file_1.py",
    "size": 200,
    "_timestamp": "2023-01-01T06:06:06",
}

EXPECTED_SHARED_FILE_2 = {
    "_id": "id:shared_2",
    "type": "File",
    "name": "shared_file_2.py",
    "file path": "/shared/shared_file_2.py",
    "url": "https://www.dropbox.com/scl/fi/12345/shared_file_2.py",
    "size": 200,
    "_timestamp": "2023-01-01T06:06:06",
}

EXPECTED_CONTENT_FOR_TEXT_FILE = {
    "_id": "file_1",
    "_timestamp": datetime.datetime(2023, 1, 1, 6, 6, 6),
    "_attachment": "VGhpcyBpcyBhIHRleHQgY29udGVudA==",
}

EXPECTED_CONTENT_FOR_PAPER_FILE = {
    "_id": "paper_1",
    "_timestamp": datetime.datetime(2023, 1, 1, 6, 6, 6),
    "_attachment": "IyBUaGlzIGlzIGEgbWFya2Rvd24gY29udGVudA==",
}

MOCK_FILES_LIST_FOLDER = ListFolderResult(
    entries=[MOCK_FILE_DATA], has_more=True, cursor="cursor"
)

MOCK_FILES_LIST_FOLDER_CONTINUE = ListFolderResult(
    entries=[MOCK_FOLDER_DATA], has_more=False, cursor=None
)

MOCK_SHARING_LIST_RECEIVED_FILES = ListFilesResult(
    entries=[MOCK_SHARED_FILE_DATA_1],
    cursor="cursor",
)

MOCK_SHARING_LIST_RECEIVED_FILES_CONTINUE = ListFilesResult(
    entries=[MOCK_SHARED_FILE_DATA_2], cursor=None
)


class MockResponse:
    def __init__(self):
        self.name = "test name"
        self.content = b"test content"
        self.size = 200


class MockError:
    def is_path(self):
        return True

    def get_path(self):
        return LookupError()


class LookupError:
    def is_not_found(self):
        return True


@pytest.mark.asyncio
async def test_configuration():
    """Tests the get configurations method of the Dropbox source class."""
    config = DataSourceConfiguration(
        config=DropboxDataSource.get_default_configuration()
    )
    assert config["path"] == PATH
    assert config["app_key"] == DUMMY_VALUES
    assert config["app_secret"] == DUMMY_VALUES
    assert config["refresh_token"] == DUMMY_VALUES


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    ["path", "app_key", "app_secret", "refresh_token"],
)
async def test_validate_configuration_with_empty_fields_then_raise_exception(field):
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.configuration.set_field(name=field, value="")

        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_for_valid_path():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.configuration.set_field(name="path", value="/shared")

        with patch.object(
            source.dropbox_client._connection,
            "files_get_metadata",
            return_value=AsyncMock(),
        ):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_path_then_raise_exception():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.path = "/abc"

        with patch.object(
            source.dropbox_client._connection,
            "files_get_metadata",
            side_effect=ApiError(
                request_id=1,
                error=MockError(),
                user_message_text="Api Error Occurred",
                user_message_locale=None,
            ),
        ):
            with pytest.raises(
                ConfigurableFieldValueError, match="Configured Path: /abc is invalid"
            ):
                await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_app_key_and_app_secret_then_raise_exception():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.path = "/abc"

        with patch.object(
            source.dropbox_client._connection,
            "files_get_metadata",
            side_effect=BadInputError(request_id=2, message="Bad Input Error"),
        ):
            with pytest.raises(
                ConfigurableFieldValueError,
                match="Configured App Key or App Secret is invalid",
            ):
                await source.validate_config()


@pytest.mark.asyncio
async def test_validate_configuration_with_invalid_refresh_token_then_raise_exception():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.path = "/abc"

        with patch.object(
            source.dropbox_client._connection,
            "files_get_metadata",
            side_effect=AuthError(request_id=3, error="Auth Error"),
        ):
            with pytest.raises(
                ConfigurableFieldValueError, match="Configured Refresh Token is invalid"
            ):
                await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_invalid_concurrent_downloads():
    async with AsyncSourceContextManager(
        DropboxDataSource, concurrent_downloads=1000
    ) as source:
        with pytest.raises(
            ConfigurableFieldValueError,
            match="Field validation errors: 'Maximum concurrent downloads' value '1000' should be less than 101.",
        ):
            await source.validate_config()


def test_tweak_bulk_options():
    source = create_source(DropboxDataSource)

    source.concurrent_downloads = 10
    options = {"concurrent_downloads": 5}

    source.tweak_bulk_options(options)
    assert options["concurrent_downloads"] == 10


@pytest.mark.asyncio
async def test_ping():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        with patch.object(
            source.dropbox_client._connection,
            "users_get_current_account",
            return_value=AsyncMock(return_value="Mock..."),
        ):
            await source.ping()


@pytest.mark.asyncio
async def test_ping_for_failed_connection_exception_then_raise_exception():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        with patch.object(
            source.dropbox_client._connection,
            "users_get_current_account",
            side_effect=Exception("Something went wrong"),
        ):
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
async def test_ping_for_incorrect_app_key_and_app_secret_then_raise_exception():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        with patch.object(
            source.dropbox_client._connection,
            "users_get_current_account",
            side_effect=BadInputError(request_id=2, message="Bad Input Error"),
        ):
            with pytest.raises(
                Exception, match="Configured App Key or App Secret is invalid"
            ):
                await source.ping()


@pytest.mark.asyncio
async def test_ping_for_incorrect_refresh_token_then_raise_exception():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        with patch.object(
            source.dropbox_client._connection,
            "users_get_current_account",
            side_effect=AuthError(request_id=3, error="Auth Error"),
        ):
            with pytest.raises(Exception, match="Configured Refresh Token is invalid"):
                await source.ping()


@pytest.mark.asyncio
async def test_close_with_client_session():
    source = create_source(DropboxDataSource)
    _ = source.dropbox_client._connection

    await source.close()
    assert hasattr(source.dropbox_client.__dict__, "_connection") is False


@pytest.mark.asyncio
async def test_close_without_client_session():
    source = create_source(DropboxDataSource)

    await source.close()
    assert source.dropbox_client._session is None


@freeze_time("2023-01-01T06:06:06")
@pytest.mark.asyncio
async def test_fetch_files_folders():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source.dropbox_client.path = "/test"

        mock_connection = Mock()
        source.dropbox_client._connection = mock_connection

        source.dropbox_client._connection.files_list_folder = Mock(
            return_value=MOCK_FILES_LIST_FOLDER
        )

        source.dropbox_client._connection.files_list_folder_continue = Mock(
            return_value=MOCK_FILES_LIST_FOLDER_CONTINUE
        )

        expected_response = [EXPECTED_FILE, EXPECTED_FOLDER]
        actual_response = []
        async for _, result in source._fetch_files_folders():
            actual_response.append(result)

        assert actual_response == expected_response


@pytest.mark.asyncio
async def test_fetch_shared_files():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        mock_connection = Mock()
        source.dropbox_client._connection = mock_connection

        source.dropbox_client._connection.sharing_list_received_files = Mock(
            return_value=MOCK_SHARING_LIST_RECEIVED_FILES
        )

        source.dropbox_client._connection.sharing_list_received_files_continue = Mock(
            return_value=MOCK_SHARING_LIST_RECEIVED_FILES_CONTINUE
        )

        source.dropbox_client._connection.sharing_get_shared_link_file = Mock(
            return_value=(FILE_LINK_METADATA, MockResponse())
        )

        expected_response = [EXPECTED_SHARED_FILE_1, EXPECTED_SHARED_FILE_2]
        actual_response = []
        async for _, _, result in source._fetch_shared_files():
            actual_response.append(result)

        assert actual_response == expected_response


@pytest.mark.asyncio
async def test_get_content_with_text_file():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        mock_connection = Mock()
        source.dropbox_client._connection = mock_connection

        source.dropbox_client._connection.files_download.return_value = (
            FileMetadata("/test/test_file.txt"),
            MagicMock(content=b"This is a text content"),
        )

        response = await source.get_content(attachment=MOCK_FILE_DATA, doit=True)
        assert response == EXPECTED_CONTENT_FOR_TEXT_FILE


@pytest.mark.asyncio
async def test_get_content_with_paper_file():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        mock_connection = Mock()
        source.dropbox_client._connection = mock_connection

        source.dropbox_client._connection.files_export.return_value = (
            FileMetadata("/test/dummy_file.paper"),
            MagicMock(content=b"# This is a markdown content"),
        )

        response = await source.get_content(attachment=MOCK_PAPER_FILE_DATA, doit=True)
        assert response == EXPECTED_CONTENT_FOR_PAPER_FILE


@pytest.mark.asyncio
async def test_get_content_with_file_size_zero():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        mock_file_with_size_zero = copy(MOCK_FILE_DATA)
        mock_file_with_size_zero.size = 0
        response = await source.get_content(
            attachment=mock_file_with_size_zero, doit=True
        )
        assert response is None


@pytest.mark.asyncio
async def test_get_content_with_large_file_size():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        mock_file_with_large_size = copy(MOCK_FILE_DATA)
        mock_file_with_large_size.size = 23000000
        response = await source.get_content(
            attachment=mock_file_with_large_size, doit=True
        )
        assert response is None


@pytest.mark.asyncio
async def test_get_content_with_unsupported_tika_file_type_then_skip():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        mock_file_with_unsupported_tika_extension = copy(MOCK_FILE_DATA)
        mock_file_with_unsupported_tika_extension.name = "screenshot.png"
        response = await source.get_content(
            attachment=mock_file_with_unsupported_tika_extension, doit=True
        )
        assert response is None


@pytest.mark.asyncio
async def test_get_content_without_extension_then_skip():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        mock_file_without_extension = copy(MOCK_FILE_DATA)
        mock_file_without_extension.name = "ssh-new"
        response = await source.get_content(
            attachment=mock_file_without_extension, doit=True
        )
        assert response is None


@pytest.mark.asyncio
async def test_get_docs():
    async with AsyncSourceContextManager(DropboxDataSource) as source:
        source._fetch_files_folders = AsyncIterator(
            [(MOCK_FILE_DATA, EXPECTED_FILE), (MOCK_FOLDER_DATA, EXPECTED_FOLDER)]
        )

        source._fetch_shared_files = AsyncIterator(
            [(FILE_LINK_METADATA, MockResponse(), EXPECTED_SHARED_FILE_1)]
        )

        source.get_content = AsyncMock(return_value=EXPECTED_CONTENT_FOR_TEXT_FILE)

        response = []
        async for document, _ in source.get_docs():
            response.append(document)

        assert response == [EXPECTED_FILE, EXPECTED_FOLDER, EXPECTED_SHARED_FILE_1]
