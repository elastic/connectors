#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Azure Blob Storage source class methods"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from azure.storage.blob.aio import BlobClient, BlobServiceClient, ContainerClient

from connectors.source import ConfigurableFieldValueError
from connectors.sources.azure_blob_storage import AzureBlobStorageDataSource
from tests.commons import AsyncIterator
from tests.sources.support import create_source


@asynccontextmanager
async def create_abs_source(
    use_text_extraction_service=False,
):
    async with create_source(
        AzureBlobStorageDataSource,
        account_name="foo",
        account_key="bar",
        blob_endpoint="https://foo.endpoint.com",
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source


@pytest.mark.asyncio
async def test_ping_for_successful_connection():
    """Test ping method of AzureBlobStorageDataSource class"""

    # Setup
    mock_response = asyncio.Future()
    mock_response.set_result(
        {
            "client_request_id": "dummy",
            "request_id": "dummy",
            "version": "v1",
            "date": "dummy",
            "sku_name": "dummy",
            "account_kind": "StorageV2",
            "is_hns_enabled": False,
        }
    )
    with patch.object(
        BlobServiceClient, "get_account_information", return_value=mock_response
    ):
        async with create_abs_source() as source:
            # Execute
            await source.ping()


@pytest.mark.asyncio
async def test_ping_for_failed_connection():
    """Test ping method of AzureBlobStorageDataSource class with negative case"""

    # Setup
    async with create_abs_source() as source:
        with patch.object(
            BlobServiceClient,
            "get_account_information",
            side_effect=Exception("Something went wrong"),
        ):
            # Execute
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
async def test_prepare_blob_doc():
    """Test prepare_blob_doc method of AzureBlobStorageDataSource Class"""

    # Setup
    async with create_abs_source() as source:
        document = {
            "container": "container1",
            "name": "blob1",
            "content_settings": {"content_type": "plain/text"},
            "last_modified": datetime(2022, 4, 21, 12, 12, 30),
            "creation_time": datetime(2022, 4, 21, 12, 12, 30),
            "metadata": "{'key1': 'value1', 'key2': 'value2'}",
            "lease": {"status": "Locked", "state": "Leased", "duration": "Infinite"},
            "blob_tier": "private",
            "size": 1000,
        }
        expected_output = {
            "_id": "container1/blob1",
            "_timestamp": "2022-04-21T12:12:30",
            "created at": "2022-04-21T12:12:30",
            "content type": "plain/text",
            "container metadata": "{'key1': 'value1', 'key2': 'value2'}",
            "metadata": "{'key1': 'value1', 'key2': 'value2'}",
            "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
            "title": "blob1",
            "tier": "private",
            "size": 1000,
            "container": "container1",
        }

        # Execute
        actual_output = source.prepare_blob_doc(
            document, {"key1": "value1", "key2": "value2"}
        )

        # Assert
        assert actual_output == expected_output


@pytest.mark.asyncio
async def test_get_container():
    """Test get_container method of AzureBlobStorageDataSource Class"""

    # Setup
    async with create_abs_source() as source:
        source.connection_string = source._configure_connection_string()
        mock_repsonse = AsyncIterator(
            [
                {
                    "name": "container1",
                    "last_modified": datetime(2022, 4, 21, 12, 12, 30),
                    "metadata": {"key1": "value1"},
                    "lease": {
                        "status": "Locked",
                        "state": "Leased",
                        "duration": "Infinite",
                    },
                    "public_access": "private",
                }
            ]
        )
        with patch.object(
            BlobServiceClient, "list_containers", return_value=mock_repsonse
        ):
            expected_output = {"name": "container1", "metadata": {"key1": "value1"}}

            # Execute
            async for actual_document in source.get_container():
                # Assert
                assert actual_document == expected_output


@pytest.mark.asyncio
async def test_get_blob():
    """Test get_blob method of AzureBlobStorageDataSource Class"""

    # Setup
    async with create_abs_source() as source:
        source.connection_string = source._configure_connection_string()
        mock_response = AsyncIterator(
            [
                {
                    "container": "container1",
                    "name": "blob1",
                    "content_settings": {"content_type": "plain/text"},
                    "last_modified": datetime(2022, 4, 21, 12, 12, 30),
                    "creation_time": datetime(2022, 4, 21, 12, 12, 30),
                    "metadata": "{'key1': 'value1', 'key2': 'value2'}",
                    "lease": {
                        "status": "Locked",
                        "state": "Leased",
                        "duration": "Infinite",
                    },
                    "blob_tier": "private",
                    "size": 1000,
                }
            ]
        )
        with patch.object(ContainerClient, "list_blobs", return_value=mock_response):
            expected_output = {
                "_id": "container1/blob1",
                "_timestamp": "2022-04-21T12:12:30",
                "created at": "2022-04-21T12:12:30",
                "content type": "plain/text",
                "container metadata": "{'key1': 'value1'}",
                "metadata": "{'key1': 'value1', 'key2': 'value2'}",
                "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
                "title": "blob1",
                "tier": "private",
                "size": 1000,
                "container": "container1",
            }

            # Execute
            async for actual_document in source.get_blob(
                {"name": "container1", "metadata": {"key1": "value1"}}
            ):
                # Assert
                assert actual_document == expected_output


@pytest.mark.asyncio
async def test_get_blob_negative():
    """Test get_blob negative method of AzureBlobStorageDataSource Class"""

    async with create_abs_source() as source:
        source.connection_string = source._configure_connection_string()
        async for actual_document in source.get_blob(
            {"name": "container1", "metadata": {"key1": "value1"}}
        ):
            assert actual_document is None


@pytest.mark.asyncio
async def test_get_doc():
    """Test get_doc method of AzureBlobStorageDataSource Class"""

    # Setup
    async with create_abs_source() as source:
        source.containers = ["*"]
        source.get_container = Mock(
            return_value=AsyncIterator(
                [
                    {
                        "type": "container",
                        "_id": "container1",
                        "timestamp": "2022-04-21T12:12:30",
                        "metadata": "key1=value1",
                        "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
                        "title": "container1",
                        "access": "private",
                    }
                ]
            )
        )
        source.get_blob = Mock(
            return_value=AsyncIterator(
                [
                    {
                        "type": "blob",
                        "_id": "container1/blob1",
                        "timestamp": "2022-04-21T12:12:30",
                        "created at": "2022-04-21T12:12:30",
                        "content type": "plain/text",
                        "container metadata": "{'key1': 'value1'}",
                        "metadata": "{'key1': 'value1', 'key2': 'value2'}",
                        "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
                        "title": "blob1",
                        "tier": "private",
                        "size": 1000,
                        "container": "container1",
                    }
                ]
            )
        )
        expected_response = [
            {
                "type": "blob",
                "_id": "container1/blob1",
                "timestamp": "2022-04-21T12:12:30",
                "created at": "2022-04-21T12:12:30",
                "content type": "plain/text",
                "container metadata": "{'key1': 'value1'}",
                "metadata": "{'key1': 'value1', 'key2': 'value2'}",
                "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
                "title": "blob1",
                "tier": "private",
                "size": 1000,
                "container": "container1",
            }
        ]
        actual_response = []

        # Execute
        async for document, _ in source.get_docs():
            actual_response.append(document)

        # Assert
        assert actual_response == expected_response


async def create_fake_coroutine(item):
    """create a method for returning fake coroutine value for
    Args:
        item: Value for converting into coroutine
    """
    return item


@pytest.mark.asyncio
async def test_get_doc_for_specific_container():
    """Test get_doc for specific container method of AzureBlobStorageDataSource Class"""

    # Setup
    async with create_abs_source() as source:
        source.containers = ["container1"]
        source.get_blob = Mock(
            return_value=AsyncIterator(
                [
                    {
                        "type": "blob",
                        "_id": "container1/blob1",
                        "timestamp": "2022-04-21T12:12:30",
                        "created at": "2022-04-21T12:12:30",
                        "content type": "plain/text",
                        "container metadata": "{'key1': 'value1'}",
                        "metadata": "{'key1': 'value1', 'key2': 'value2'}",
                        "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
                        "title": "blob1",
                        "tier": "private",
                        "size": 1000,
                        "container": "container1",
                    }
                ]
            )
        )

        source.get_container_properties = Mock(
            return_value=create_fake_coroutine(
                {
                    "metadata": "{'key1': 'value1', 'key2': 'value2'}",
                    "name": "container1",
                }
            )
        )
        expected_response = [
            {
                "type": "blob",
                "_id": "container1/blob1",
                "timestamp": "2022-04-21T12:12:30",
                "created at": "2022-04-21T12:12:30",
                "content type": "plain/text",
                "container metadata": "{'key1': 'value1'}",
                "metadata": "{'key1': 'value1', 'key2': 'value2'}",
                "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
                "title": "blob1",
                "tier": "private",
                "size": 1000,
                "container": "container1",
            }
        ]
        actual_response = []

        # Execute
        async for document, _ in source.get_docs():
            actual_response.append(document)

        # Assert
        assert actual_response == expected_response


@pytest.mark.asyncio
async def test_get_content():
    """Test get_content method of AzureBlobStorageDataSource Class"""

    # Setup
    async with create_abs_source() as source:
        source.connection_string = source._configure_connection_string()

        class DownloadBlobMock:
            """This class is used Mock object of download_blob"""

            async def chunks(self):
                """This Method is used to read content"""
                yield b"Mock...."

        mock_response = {
            "type": "blob",
            "id": "container1/blob1",
            "_timestamp": "2022-04-21T12:12:30",
            "created at": "2022-04-21T12:12:30",
            "content type": "plain/text",
            "container metadata": "{'key1': 'value1'}",
            "metadata": "{'key1': 'value1', 'key2': 'value2'}",
            "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
            "title": "blob1.txt",
            "tier": "private",
            "size": 1000,
            "container": "container1",
        }
        with patch.object(BlobClient, "download_blob", return_value=DownloadBlobMock()):
            expected_output = {
                "_id": "container1/blob1",
                "_timestamp": "2022-04-21T12:12:30",
                "_attachment": "TW9jay4uLi4=",
            }
            actual_response = await source.get_content(mock_response, doit=True)

            assert actual_response == expected_output
            assert "body" not in actual_response


@pytest.mark.asyncio
async def test_get_content_with_upper_extension():
    """Test get_content method of AzureBlobStorageDataSource Class"""

    # Setup
    async with create_abs_source() as source:
        source.connection_string = source._configure_connection_string()

        class DownloadBlobMock:
            """This class is used Mock object of download_blob"""

            async def chunks(self):
                """This Method is used to read content"""
                yield b"Mock...."

        mock_response = {
            "type": "blob",
            "id": "container1/blob1",
            "_timestamp": "2022-04-21T12:12:30",
            "created at": "2022-04-21T12:12:30",
            "content type": "plain/text",
            "container metadata": "{'key1': 'value1'}",
            "metadata": "{'key1': 'value1', 'key2': 'value2'}",
            "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
            "title": "blob1.TXT",
            "tier": "private",
            "size": 1000,
            "container": "container1",
        }
        with patch.object(BlobClient, "download_blob", return_value=DownloadBlobMock()):
            expected_output = {
                "_id": "container1/blob1",
                "_timestamp": "2022-04-21T12:12:30",
                "_attachment": "TW9jay4uLi4=",
            }

            # Execute
            actual_response = await source.get_content(mock_response, doit=True)

            # Assert
            assert actual_response == expected_output


@pytest.mark.asyncio
async def test_get_content_when_doit_false():
    """Test get_content method when doit is false."""

    # Setup
    async with create_abs_source() as source:
        mock_response = {
            "type": "blob",
            "id": "container1/blob1",
            "timestamp": "2022-04-21T12:12:30",
            "created at": "2022-04-21T12:12:30",
            "content type": "plain/text",
            "container metadata": "{'key1': 'value1'}",
            "metadata": "{'key1': 'value1', 'key2': 'value2'}",
            "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
            "title": "blob1.txt",
            "tier": "private",
            "size": 1000,
            "container": "container1",
        }

        # Execute
        actual_response = await source.get_content(mock_response)

        # Assert
        assert actual_response is None


@pytest.mark.asyncio
async def test_get_content_when_file_size_0b():
    """Test get_content method when the file size is 0b"""

    # Setup
    async with create_abs_source() as source:
        mock_response = {
            "type": "blob",
            "id": "container1/blob1",
            "timestamp": "2022-04-21T12:12:30",
            "created at": "2022-04-21T12:12:30",
            "content type": "plain/text",
            "container metadata": "{'key1': 'value1'}",
            "metadata": "{'key1': 'value1', 'key2': 'value2'}",
            "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
            "title": "blob1.pdf",
            "tier": "private",
            "size": 0,
            "container": "container1",
        }

        # Execute
        actual_response = await source.get_content(mock_response, doit=True)

        # Assert
        assert actual_response is None


@pytest.mark.asyncio
async def test_get_content_when_size_limit_exceeded():
    """Test get_content method when the file size is 10MB"""

    # Setup
    async with create_abs_source() as source:
        mock_response = {
            "type": "blob",
            "id": "container1/blob1",
            "timestamp": "2022-04-21T12:12:30",
            "created at": "2022-04-21T12:12:30",
            "content type": "plain/text",
            "container metadata": "{'key1': 'value1'}",
            "metadata": "{'key1': 'value1', 'key2': 'value2'}",
            "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
            "title": "blob1.txt",
            "tier": "private",
            "size": 10000000000000,
            "container": "container1",
        }

        # Execute
        actual_response = await source.get_content(mock_response, doit=True)

        # Assert
        assert actual_response is None


@pytest.mark.asyncio
async def test_get_content_when_type_not_supported():
    """Test get_content method when the file type is not supported"""

    # Setup
    async with create_abs_source() as source:
        mock_response = {
            "type": "blob",
            "id": "container1/blob1",
            "timestamp": "2022-04-21T12:12:30",
            "created at": "2022-04-21T12:12:30",
            "content type": "plain/text",
            "container metadata": "{'key1': 'value1'}",
            "metadata": "{'key1': 'value1', 'key2': 'value2'}",
            "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
            "title": "blob1.xyz",
            "tier": "private",
            "size": 10,
            "container": "container1",
        }

        # Execute
        actual_response = await source.get_content(mock_response, doit=True)

        # Assert
        assert actual_response is None


@pytest.mark.asyncio
async def test_validate_config_no_account_name():
    """Test configure connection string method of AzureBlobStorageDataSource class"""

    # Setup
    async with create_abs_source() as source:
        source.configuration.get_field("account_name").value = ""

        with pytest.raises(ConfigurableFieldValueError):
            # Execute
            await source.validate_config()


@pytest.mark.asyncio
async def test_tweak_bulk_options():
    """Test tweak_bulk_options method of BaseDataSource class"""

    # Setup
    async with create_abs_source() as source:
        options = {}
        options["concurrent_downloads"] = 10

        # Execute
        source.tweak_bulk_options(options)


@pytest.mark.asyncio
async def test_validate_config_invalid_concurrent_downloads():
    """Test tweak_bulk_options method of BaseDataSource class with invalid concurrent downloads"""

    # Setup
    async with create_source(
        AzureBlobStorageDataSource, concurrent_downloads=1000
    ) as source:
        with pytest.raises(ConfigurableFieldValueError):
            # Execute
            await source.validate_config()


@pytest.mark.asyncio
async def test_get_content_when_blob_tier_archive():
    """Test get_content method when the blob tier is archive"""

    # Setup
    async with create_abs_source() as source:
        mock_response = {
            "type": "blob",
            "id": "container1/blob1",
            "timestamp": "2022-04-21T12:12:30",
            "created at": "2022-04-21T12:12:30",
            "content type": "plain/text",
            "container metadata": "{'key1': 'value1'}",
            "metadata": "{'key1': 'value1', 'key2': 'value2'}",
            "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
            "title": "blob1.pdf",
            "tier": "Archive",
            "size": 10,
            "container": "container1",
        }

        # Execute
        actual_response = await source.get_content(mock_response, doit=True)

        # Assert
        assert actual_response is None


@pytest.mark.asyncio
@patch(
    "connectors.content_extraction.ContentExtraction._check_configured",
    lambda *_: True,
)
async def test_get_content_with_text_extraction_enabled_adds_body():
    mock_response = {
        "type": "blob",
        "id": "container1/blob1",
        "_timestamp": "2022-04-21T12:12:30",
        "created at": "2022-04-21T12:12:30",
        "content type": "plain/text",
        "container metadata": "{'key1': 'value1'}",
        "metadata": "{'key1': 'value1', 'key2': 'value2'}",
        "leasedata": "{'status': 'Locked', 'state': 'Leased', 'duration': 'Infinite'}",
        "title": "blob1.txt",
        "tier": "private",
        "size": 1000,
        "container": "container1",
    }
    mock_download = b"Mock...."

    with patch(
        "connectors.content_extraction.ContentExtraction.extract_text",
        return_value=str(mock_download),
    ) as extraction_service_mock, patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
    ):
        async with create_abs_source(use_text_extraction_service=True) as source:
            source.connection_string = source._configure_connection_string()

            class DownloadBlobMock:
                """This class is used Mock object of download_blob"""

                async def chunks(self):
                    """This Method is used to read content"""
                    yield mock_download

            with patch.object(
                BlobClient, "download_blob", return_value=DownloadBlobMock()
            ):
                expected_output = {
                    "_id": "container1/blob1",
                    "_timestamp": "2022-04-21T12:12:30",
                    "body": str(mock_download),
                }
                actual_response = await source.get_content(mock_response, doit=True)

                extraction_service_mock.assert_called_once()
                assert actual_response == expected_output
                assert "_attachment" not in actual_response


@pytest.mark.asyncio
async def test_get_container_properties_negative():
    """Test get_container_properties negative method of AzureBlobStorageDataSource Class"""
    async with create_abs_source() as source:
        source.connection_string = source._configure_connection_string()
        actual_document = await source.get_container_properties(container="container1")
        assert actual_document is None
