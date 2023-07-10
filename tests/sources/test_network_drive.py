#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Network Drive source class methods.
"""
import asyncio
import datetime
from io import BytesIO
from unittest import mock

import pytest
import smbclient
from smbprotocol.exceptions import LogonFailure, SMBOSError

from connectors.source import DataSourceConfiguration
from connectors.sources.network_drive import NASDataSource
from tests.sources.support import create_source

READ_COUNT = 0
MAX_CHUNK_SIZE = 65536


def mock_file(name):
    """Generates the smbprotocol object for a file

    Args:
        name (str): The name of the mocked file
    """
    mock_response = mock.Mock()
    mock_response.name = name
    mock_response.path = f"\\1.2.3.4/dummy_path/{name}"
    mock_stats = {}
    mock_stats["file_id"] = mock.Mock()
    mock_stats["file_id"].get_value.return_value = "1"
    mock_stats["allocation_size"] = mock.Mock()
    mock_stats["allocation_size"].get_value.return_value = "30"
    mock_stats["creation_time"] = mock.Mock()
    mock_stats["creation_time"].get_value.return_value = datetime.datetime(
        2022, 1, 11, 12, 12, 30
    )
    mock_stats["change_time"] = mock.Mock()
    mock_stats["change_time"].get_value.return_value = datetime.datetime(
        2022, 4, 21, 12, 12, 30
    )

    mock_response._dir_info.fields = mock_stats
    mock_response.is_dir.return_value = False

    return mock_response


def mock_folder(name):
    """Generates the smbprotocol object for a folder

    Args:
        name (str): The name of the mocked folder
    """
    mock_response = mock.Mock()
    mock_response.name = name
    mock_response.path = f"\\1.2.3.4/dummy_path/{name}"
    mock_response.is_dir.return_value = True
    mock_stats = {}
    mock_stats["file_id"] = mock.Mock()
    mock_stats["file_id"].get_value.return_value = "122"
    mock_stats["allocation_size"] = mock.Mock()
    mock_stats["allocation_size"].get_value.return_value = "200"
    mock_stats["creation_time"] = mock.Mock()
    mock_stats["creation_time"].get_value.return_value = datetime.datetime(
        2022, 2, 11, 12, 12, 30
    )
    mock_stats["change_time"] = mock.Mock()
    mock_stats["change_time"].get_value.return_value = datetime.datetime(
        2022, 5, 21, 12, 12, 30
    )
    mock_response._dir_info.fields = mock_stats
    return mock_response


def side_effect_function(MAX_CHUNK_SIZE):
    """Dynamically changing return values during reading a file in chunks
    Args:
        MAX_CHUNK_SIZE: Maximum bytes allowed to be read at a given time
    """
    global READ_COUNT
    if READ_COUNT:
        return None
    READ_COUNT += 1
    return b"Mock...."


def test_get_configuration():
    """Tests the get configurations method of the Network Drive source class."""
    # Setup
    klass = NASDataSource

    # Execute
    config = DataSourceConfiguration(config=klass.get_default_configuration())

    # Assert
    assert config["server_ip"] == "127.0.0.1"


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    """Tests the ping functionality for ensuring connection to the Network Drive."""
    # Setup
    expected_response = True
    response = asyncio.Future()
    response.set_result(expected_response)

    # Execute
    with mock.patch.object(smbclient, "register_session", return_value=response):
        source = create_source(NASDataSource)

        await source.ping()


@pytest.mark.asyncio
@mock.patch("smbclient.register_session")
async def test_ping_for_failed_connection(session_mock):
    """Tests the ping functionality when connection can not be established to Network Drive.

    Args:
        session_mock (patch): The patch of register_session method
    """
    # Setup
    response = asyncio.Future()
    response.set_result(None)
    session_mock.side_effect = ValueError
    source = create_source(NASDataSource)

    # Execute
    with pytest.raises(Exception):
        await source.ping()


@mock.patch("smbclient.register_session")
def test_create_connection_with_invalid_credentials(session_mock):
    """Tests the create_connection fails with invalid credentials

    Args:
        session_mock (patch): The patch of register_session method
    """
    # Setup
    source = create_source(NASDataSource)
    session_mock.side_effect = LogonFailure

    # Execute
    with pytest.raises(LogonFailure):
        source.create_connection()


@mock.patch("smbclient.scandir")
@pytest.mark.asyncio
async def test_get_files_with_invalid_path(dir_mock):
    """Tests the scandir method of smbclient throws error on invalid path

    Args:
        dir_mock (patch): The patch of scandir method
    """
    # Setup
    source = create_source(NASDataSource)
    path = "unknown_path"
    dir_mock.side_effect = SMBOSError(ntstatus=3221225487, filename="unknown_path")

    # Execute
    async for file in source.get_files(path=path):
        assert file == []


@pytest.mark.asyncio
@mock.patch("smbclient.scandir")
async def test_get_files(dir_mock):
    """Tests the get_files method for network drive

    Args:
        dir_mock (patch): The patch of scandir method
    """
    # Setup
    source = create_source(NASDataSource)
    path = "\\1.2.3.4/dummy_path"
    dir_mock.return_value = [mock_file(name="a1.md"), mock_folder(name="A")]
    expected_output = [
        {
            "_id": "1",
            "_timestamp": "2022-04-21T12:12:30",
            "path": "\\1.2.3.4/dummy_path/a1.md",
            "title": "a1.md",
            "created_at": "2022-01-11T12:12:30",
            "size": "30",
            "type": "file",
        },
        {
            "_id": "122",
            "_timestamp": "2022-05-21T12:12:30",
            "path": "\\1.2.3.4/dummy_path/A",
            "title": "A",
            "created_at": "2022-02-11T12:12:30",
            "size": "200",
            "type": "folder",
        },
    ]

    # Execute
    response = []
    async for file in source.get_files(path=path):
        response.append(file)

    # Assert
    assert response == expected_output


@mock.patch("smbclient.open_file")
def test_fetch_file_when_file_is_inaccessible(file_mock):
    """Tests the open_file method of smbclient throws error when file cannot be accessed

    Args:
        file_mock (patch): The patch of open_file method
    """
    # Setup
    source = create_source(NASDataSource)
    path = "\\1.2.3.4/Users/file1.txt"
    file_mock.side_effect = SMBOSError(ntstatus=0xC0000043, filename="file1.txt")

    # Execute
    response = source.fetch_file_content(path=path)

    # Assert
    assert response is None


@pytest.mark.asyncio
@mock.patch("smbclient.open_file")
async def test_get_content(file_mock):
    """Test get_content method of Network Drive

    Args:
        file_mock (patch): The patch of open_file method
    """
    # Setup
    source = create_source(NASDataSource)
    file_mock.return_value.__enter__.return_value.read.return_value = bytes(
        "Mock....", "utf-8"
    )

    mock_response = {
        "id": "1",
        "_timestamp": "2022-04-21T12:12:30",
        "title": "file1.txt",
        "path": "\\1.2.3.4/Users/folder1/file1.txt",
        "size": "50",
    }

    mocked_content_response = BytesIO(b"Mock....")

    expected_output = {
        "_id": "1",
        "_timestamp": "2022-04-21T12:12:30",
        "_attachment": "TW9jay4uLi4=",
    }

    # Execute
    source.fetch_file_content = mock.MagicMock(return_value=mocked_content_response)
    actual_response = await source.get_content(mock_response, doit=True)

    # Assert
    assert actual_response == expected_output


@pytest.mark.asyncio
@mock.patch("smbclient.open_file")
async def test_get_content_with_upper_extension(file_mock):
    """Test get_content method of Network Drive

    Args:
        file_mock (patch): The patch of open_file method
    """
    # Setup
    source = create_source(NASDataSource)
    file_mock.return_value.__enter__.return_value.read.return_value = bytes(
        "Mock....", "utf-8"
    )

    mock_response = {
        "id": "1",
        "_timestamp": "2022-04-21T12:12:30",
        "title": "file1.TXT",
        "path": "\\1.2.3.4/Users/folder1/file1.txt",
        "size": "50",
    }

    mocked_content_response = BytesIO(b"Mock....")

    expected_output = {
        "_id": "1",
        "_timestamp": "2022-04-21T12:12:30",
        "_attachment": "TW9jay4uLi4=",
    }

    # Execute
    source.fetch_file_content = mock.MagicMock(return_value=mocked_content_response)
    actual_response = await source.get_content(mock_response, doit=True)

    # Assert
    assert actual_response == expected_output


@pytest.mark.asyncio
async def test_get_content_when_doit_false():
    """Test get_content method when doit is false."""
    # Setup
    source = create_source(NASDataSource)
    mock_response = {
        "id": "1",
        "_timestamp": "2022-04-21T12:12:30",
        "title": "file1.txt",
    }

    # Execute
    actual_response = await source.get_content(mock_response)

    # Assert
    assert actual_response is None


@pytest.mark.asyncio
async def test_get_content_when_file_size_is_large():
    """Test the module responsible for fetching the content of the file if it is not extractable"""
    # Setup
    source = create_source(NASDataSource)
    mock_response = {
        "id": "1",
        "_timestamp": "2022-04-21T12:12:30",
        "title": "file1.txt",
        "size": "20000000000",
    }

    # Execute
    actual_response = await source.get_content(mock_response, doit=True)

    # Assert
    assert actual_response is None


@pytest.mark.asyncio
async def test_get_content_when_file_type_not_supported():
    """Test get_content method when the file content type is not supported"""
    # Setup
    source = create_source(NASDataSource)
    mock_response = {
        "id": "1",
        "_timestamp": "2022-04-21T12:12:30",
        "title": "file2.dmg",
    }

    # Execute
    actual_response = await source.get_content(mock_response, doit=True)

    # Assert
    assert actual_response is None


@pytest.mark.asyncio
@mock.patch.object(NASDataSource, "get_files", return_value=mock.MagicMock())
@mock.patch("smbclient.walk")
async def test_get_doc(mock_get_files, mock_walk):
    """Test get_doc method of NASDataSource Class

    Args:
        mock_get_files (coroutine): The patch of get_files coroutine
        mock_walk (patch): The patch of walk method of smbclient
    """
    # Setup
    source = create_source(NASDataSource)

    # Execute
    async for _, _ in source.get_docs():
        # Assert
        mock_get_files.assert_awaited()


@mock.patch("smbclient.open_file")
def test_fetch_file_when_file_is_accessible(file_mock):
    """Tests the open_file method of smbclient when file can be accessed

    Args:
        file_mock (patch): The patch of open_file method
    """
    # Setup
    source = create_source(NASDataSource)
    path = "\\1.2.3.4/Users/file1.txt"

    file_mock.return_value.__enter__.return_value.read = mock.MagicMock(
        side_effect=side_effect_function
    )

    # Execute
    response = source.fetch_file_content(path=path)

    # Assert
    assert response.read() == b"Mock...."
