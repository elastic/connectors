#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest import mock

import pytest

from connectors_sdk.config import DataSourceFrameworkConfig
from connectors_sdk.source import BaseDataSource, DataSourceConfiguration
from tests.test_source import CONFIG, DataSource

MAX_FILE_SIZE = 100


@pytest.fixture
def source():
    ds = DataSource(configuration=DataSourceConfiguration(CONFIG))
    ds.set_framework_config(DataSourceFrameworkConfig(max_file_size=MAX_FILE_SIZE))
    return ds


def test_is_valid_file_type_rejects_missing_extension(source):
    assert source.is_valid_file_type("", "readme") is False


def test_is_valid_file_type_rejects_unsupported_extension(source):
    assert source.is_valid_file_type(".xyzunsupported", "file.xyzunsupported") is False


def test_is_valid_file_type_accepts_supported_extension(source):
    assert source.is_valid_file_type(".pdf", "doc.pdf") is True


def test_is_file_size_within_limit(source):
    assert source.is_file_size_within_limit(50, "small.pdf") is True
    assert source.is_file_size_within_limit(MAX_FILE_SIZE + 1, "large.pdf") is False


def test_is_file_size_within_limit_allows_large_file_with_extraction_service(source):
    original_get = source.configuration.get

    def patched_get(key, default=None):
        if key == "use_text_extraction_service":
            return True
        return original_get(key, default)

    with mock.patch.object(source.configuration, "get", side_effect=patched_get):
        assert source.is_file_size_within_limit(MAX_FILE_SIZE + 1, "large.pdf") is True


def test_can_file_be_downloaded(source):
    assert source.can_file_be_downloaded(".pdf", "doc.pdf", 50) is True
    assert source.can_file_be_downloaded("", "doc", 50) is False


def test_sync_cursor_helpers(source):
    assert source.sync_cursor() is None
    assert source.last_sync_time()  # default epoch when cursor empty

    source.update_sync_timestamp_cursor("2024-01-01T00:00:00Z")
    assert source.sync_cursor() == {"cursor_timestamp": "2024-01-01T00:00:00Z"}
    assert source.last_sync_time() == "2024-01-01T00:00:00Z"


@pytest.mark.asyncio
@mock.patch.object(BaseDataSource, "download_to_temp_file", new_callable=mock.AsyncMock)
async def test_download_and_extract_file_returns_none_on_failure(mock_download, source):
    download_error = RuntimeError("download failed")
    mock_download.side_effect = download_error

    doc = {"_id": "1"}
    result = await source.download_and_extract_file(
        doc,
        "missing.pdf",
        ".pdf",
        lambda: None,
    )
    assert result is None


@pytest.mark.asyncio
@mock.patch.object(BaseDataSource, "download_to_temp_file", new_callable=mock.AsyncMock)
async def test_download_and_extract_file_returns_doc_when_requested(
    mock_download, source
):
    download_error = RuntimeError("download failed")
    mock_download.side_effect = download_error

    doc = {"_id": "1"}
    result = await source.download_and_extract_file(
        doc,
        "missing.pdf",
        ".pdf",
        lambda: None,
        return_doc_if_failed=True,
    )
    assert result == doc


@pytest.mark.asyncio
@mock.patch.object(
    BaseDataSource, "handle_file_content_extraction", new_callable=mock.AsyncMock
)
@mock.patch.object(BaseDataSource, "download_to_temp_file", new_callable=mock.AsyncMock)
async def test_download_and_extract_file_success(mock_download, mock_extract, source):
    mock_extract.return_value = {"_id": "1", "_attachment": "data"}

    async def download():
        yield b"chunk"

    doc = {"_id": "1"}
    result = await source.download_and_extract_file(doc, "file.pdf", ".pdf", download)

    mock_download.assert_awaited_once()
    mock_extract.assert_awaited_once()
    assert mock_extract.await_args.args[0] == doc
    assert mock_extract.await_args.args[1] == "file.pdf"
    assert result == {"_id": "1", "_attachment": "data"}
