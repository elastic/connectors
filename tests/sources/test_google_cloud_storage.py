#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Google Cloud Storage source class methods.
"""
import asyncio
from contextlib import asynccontextmanager
from unittest import mock
from unittest.mock import patch, Mock

import pytest
from aiogoogle import Aiogoogle
from aiogoogle.auth.managers import ServiceAccountManager
from aiogoogle.models import Request, Response

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.google_cloud_storage import GoogleCloudStorageDataSource
from tests.sources.support import create_source

SERVICE_ACCOUNT_CREDENTIALS = '{"project_id": "dummy123"}'
API_NAME = "storage"
API_VERSION = "v1"


@asynccontextmanager
async def create_gcs_source(use_text_extraction_service=False):
    async with create_source(
        GoogleCloudStorageDataSource,
        service_account_credentials=SERVICE_ACCOUNT_CREDENTIALS,
        retry_count=0,
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source


@pytest.mark.asyncio
async def test_empty_configuration():
    """Tests the validity of the configurations passed to the Google Cloud source class."""

    configuration = DataSourceConfiguration({"service_account_credentials": ""})
    gcs_object = GoogleCloudStorageDataSource(configuration=configuration)

    with pytest.raises(
        ConfigurableFieldValueError,
        match="Field validation errors: 'Service_account_credentials' cannot be empty.",
    ):
        await gcs_object.validate_config()


@pytest.mark.asyncio
async def test_raise_on_invalid_configuration():
    configuration = DataSourceConfiguration(
        {"service_account_credentials": "{'abc':'bcd','cd'}"}
    )
    gcs_object = GoogleCloudStorageDataSource(configuration=configuration)

    with pytest.raises(
        ConfigurableFieldValueError,
        match="Google Cloud Storage service account is not a valid JSON",
    ):
        await gcs_object.validate_config()


@pytest.mark.asyncio
async def test_ping_for_successful_connection(catch_stdout):
    """Tests the ping functionality for ensuring connection to Google Cloud Storage."""

    expected_response = {
        "kind": "storage#serviceAccount",
        "email_address": "serviceaccount@email.com",
    }
    async with create_gcs_source() as source:
        as_service_account_response = asyncio.Future()
        as_service_account_response.set_result(expected_response)

        with mock.patch.object(
            Aiogoogle, "as_service_account", return_value=as_service_account_response
        ):
            await source.ping()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_ping_for_failed_connection(catch_stdout):
    """Tests the ping functionality when connection can not be established to Google Cloud Storage."""

    async with create_gcs_source() as source:
        with mock.patch.object(
            Aiogoogle, "discover", side_effect=Exception("Something went wrong")
        ):
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "previous_documents_list, updated_documents_list",
    [
        (
            [
                {
                    "items": [
                        {
                            "id": "path_to_blob",
                            "updated": "2011-10-12T00:00:00Z",
                            "name": "path_to_blob",
                        }
                    ]
                },
                {
                    "kind": "storage#buckets",
                },
            ],
            [
                {
                    "_id": "path_to_blob",
                    "component_count": None,
                    "content_encoding": None,
                    "content_language": None,
                    "created_at": None,
                    "last_updated": "2011-10-12T00:00:00Z",
                    "metadata": None,
                    "name": "path_to_blob",
                    "size": None,
                    "storage_class": None,
                    "_timestamp": "2011-10-12T00:00:00Z",
                    "type": None,
                    "url": "https://console.cloud.google.com/storage/browser/_details/None/path_to_blob;tab=live_object?project=dummy123",
                    "version": None,
                    "bucket_name": None,
                }
            ],
        )
    ],
)
async def test_get_blob_document(previous_documents_list, updated_documents_list):
    """Tests the function which modifies the fetched blobs and maps the values to keys.

    Args:
        previous_documents_list (list): List of the blobs documents fetched from Google Cloud Storage.
        updated_documents_list (list): List of the documents returned by the method.
    """

    async with create_gcs_source() as source:
        assert updated_documents_list == list(
            source.get_blob_document(blobs=previous_documents_list[0])
        )


@pytest.mark.asyncio
async def test_fetch_buckets():
    """Tests the method which lists the storage buckets available in Google Cloud Storage."""

    async with create_gcs_source() as source:
        expected_response = {
            "kind": "storage#objects",
            "items": [
                {
                    "kind": "storage#object",
                    "id": "bucket_1",
                    "updated": "2011-10-12T00:00:00Z",
                    "name": "bucket_1",
                }
            ],
        }
        expected_bucket_list = [
            {
                "kind": "storage#objects",
                "items": [
                    {
                        "kind": "storage#object",
                        "id": "bucket_1",
                        "updated": "2011-10-12T00:00:00Z",
                        "name": "bucket_1",
                    }
                ],
            }
        ]
        dummy_url = "https://dummy.gcs.com/buckets/b1/objects"

        expected_response_object = Response(
            status_code=200,
            url=dummy_url,
            json=expected_response,
            req=Request(method="GET", url=dummy_url),
        )

        with mock.patch.object(
            Aiogoogle, "as_service_account", return_value=expected_response_object
        ):
            with mock.patch.object(ServiceAccountManager, "refresh"):
                bucket_list = []
                async for bucket in source.fetch_buckets():
                    bucket_list.append(bucket)

        assert bucket_list == expected_bucket_list


@pytest.mark.asyncio
async def test_fetch_blobs():
    """Tests the method responsible to yield blobs from Google Cloud Storage bucket."""

    async with create_gcs_source() as source:
        expected_bucket_response = {
            "kind": "storage#objects",
            "items": [
                {
                    "kind": "storage#object",
                    "id": "bucket_1",
                    "updated": "2011-10-12T00:01:00Z",
                    "name": "bucket_1",
                }
            ],
        }
        dummy_blob_response = {
            "kind": "storage#objects",
            "items": [
                {
                    "kind": "storage#object",
                    "id": "bucket_1/blob_1/123123123",
                    "updated": "2011-10-12T00:01:00Z",
                    "name": "blob_1",
                }
            ],
        }
        dummy_url = "https://dummy.gcs.com/buckets/b1/objects"

        expected_response_object = Response(
            status_code=200,
            url=dummy_url,
            json=dummy_blob_response,
            req=Request(method="GET", url=dummy_url),
        )

        with mock.patch.object(
            Aiogoogle, "as_service_account", return_value=expected_response_object
        ):
            with mock.patch.object(ServiceAccountManager, "refresh"):
                async for blob_result in source.fetch_blobs(
                    buckets=expected_bucket_response
                ):
                    assert blob_result == dummy_blob_response


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_fetch_blobs_negative():
    """Tests the method responsible to yield blobs(negative) from Google Cloud Storage bucket."""

    bucket_response = {
        "kind": "storage#objects",
        "items": [
            {
                "kind": "storage#object",
                "id": "bucket_1",
                "updated": "2011-10-12T00:01:00Z",
                "name": "bucket_1",
            }
        ],
    }
    async with create_gcs_source() as source:
        with mock.patch.object(
            Aiogoogle, "discover", side_effect=Exception("Something Went Wrong")
        ):
            async for blob_result in source.fetch_blobs(buckets=bucket_response):
                assert blob_result is None


@pytest.mark.asyncio
async def test_get_docs():
    """Tests the module responsible to fetch and yield blobs documents from Google Cloud Storage."""

    async with create_gcs_source() as source:
        expected_response = {
            "kind": "storage#objects",
            "items": [
                {
                    "kind": "storage#object",
                    "id": "bucket_1/blob_1/123123123",
                    "updated": "2011-10-12T00:01:00Z",
                    "name": "blob_1",
                }
            ],
        }
        expected_blob_document = {
            "_id": "bucket_1/blob_1/123123123",
            "component_count": None,
            "content_encoding": None,
            "content_language": None,
            "created_at": None,
            "last_updated": "2011-10-12T00:01:00Z",
            "metadata": None,
            "name": "blob_1",
            "size": None,
            "storage_class": None,
            "_timestamp": "2011-10-12T00:01:00Z",
            "type": None,
            "url": "https://console.cloud.google.com/storage/browser/_details/None/blob_1;tab=live_object?project=dummy123",
            "version": None,
            "bucket_name": None,
        }
        dummy_url = "https://dummy.gcs.com/buckets/b1/objects"

        expected_response_object = Response(
            status_code=200,
            url=dummy_url,
            json=expected_response,
            req=Request(method="GET", url=dummy_url),
        )

        with mock.patch.object(
            Aiogoogle, "as_service_account", return_value=expected_response_object
        ):
            with mock.patch.object(ServiceAccountManager, "refresh"):
                async for blob_document in source.get_docs():
                    assert blob_document[0] == expected_blob_document


@pytest.mark.asyncio
async def test_get_docs_when_no_buckets_present():
    """Tests the module responsible to fetch and yield blobs documents from Google Cloud Storage. When
    Cloud storage does not have any buckets.
    """

    async with create_gcs_source() as source:
        expected_response = {
            "kind": "storage#objects",
        }
        dummy_url = "https://dummy.gcs.com/buckets/b1/objects"

        expected_response_object = Response(
            status_code=200,
            url=dummy_url,
            json=expected_response,
            req=Request(method="GET", url=dummy_url),
        )

        with mock.patch.object(
            Aiogoogle, "as_service_account", return_value=expected_response_object
        ):
            with mock.patch.object(ServiceAccountManager, "refresh"):
                async for blob_document in source.get_docs():
                    assert blob_document[0] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "blob_document, expected_blob_document",
    [
        (
            {
                "id": "bucket_1/blob_1/123123123",
                "component_count": None,
                "content_encoding": None,
                "content_language": None,
                "created_at": None,
                "last_updated": "2011-10-12T00:01:00Z",
                "metadata": None,
                "name": "blob_1.txt",
                "size": "15",
                "storage_class": None,
                "_timestamp": "2011-10-12T00:01:00Z",
                "type": None,
                "url": "https://console.cloud.google.com/storage/browser/_details/bucket_1/blob_1;tab=live_object?project=dummy123",
                "version": None,
                "bucket_name": "bucket_1",
            },
            {
                "_id": "bucket_1/blob_1/123123123",
                "_timestamp": "2011-10-12T00:01:00Z",
                "_attachment": "",
            },
        ),
        (
            {
                "id": "bucket_1/demo_folder/blob_2/321321321",
                "component_count": None,
                "content_encoding": None,
                "content_language": None,
                "created_at": None,
                "last_updated": "2011-10-12T00:01:00Z",
                "metadata": None,
                "name": "demo_folder/blob_2.txt",
                "size": "15",
                "storage_class": None,
                "_timestamp": "2011-10-12T00:01:00Z",
                "type": None,
                "url": "https://console.cloud.google.com/storage/browser/_details/bucket_1/blob_1;tab=live_object?project=dummy123",
                "version": None,
                "bucket_name": "bucket_1",
            },
            {
                "_id": "bucket_1/demo_folder/blob_2/321321321",
                "_timestamp": "2011-10-12T00:01:00Z",
                "_attachment": "",
            },
        ),
    ],
)
async def test_get_content(blob_document, expected_blob_document):
    """Test the module responsible for fetching the content of the file if it is extractable."""

    async with create_gcs_source() as source:
        blob_content_response = ""

        with mock.patch.object(
            Aiogoogle, "as_service_account", return_value=blob_content_response
        ):
            async with Aiogoogle(
                service_account_creds=source._google_storage_client.service_account_credentials
            ) as google_client:
                storage_client = await google_client.discover(
                    api_name=API_NAME, api_version=API_VERSION
                )
                storage_client.objects = mock.MagicMock()
                content = await source.get_content(
                    blob=blob_document,
                    doit=True,
                )
                assert content == expected_blob_document


@pytest.mark.asyncio
@patch(
    "connectors.content_extraction.ContentExtraction._check_configured",
    lambda *_: True,
)
async def test_get_content_with_text_extraction_enabled_adds_body():
    """Test the module responsible for fetching the content of the file if it is extractable."""

    with patch(
        "connectors.content_extraction.ContentExtraction.extract_text",
        return_value="file content",
    ), patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
    ):
        async with create_gcs_source(use_text_extraction_service=True) as source:
            blob_document = {
                "id": "bucket_1/blob_1/123123123",
                "component_count": None,
                "content_encoding": None,
                "content_language": None,
                "created_at": None,
                "last_updated": "2011-10-12T00:01:00Z",
                "metadata": None,
                "name": "blob_1.txt",
                "size": "15",
                "storage_class": None,
                "_timestamp": "2011-10-12T00:01:00Z",
                "type": None,
                "url": "https://console.cloud.google.com/storage/browser/_details/bucket_1/blob_1;tab=live_object?project=dummy123",
                "version": None,
                "bucket_name": "bucket_1",
            }
            expected_blob_document = {
                "_id": "bucket_1/blob_1/123123123",
                "_timestamp": "2011-10-12T00:01:00Z",
                "body": "file content",
            }
            blob_content_response = ""

            with mock.patch.object(
                Aiogoogle, "as_service_account", return_value=blob_content_response
            ):
                async with Aiogoogle(
                    service_account_creds=source._google_storage_client.service_account_credentials
                ) as google_client:
                    storage_client = await google_client.discover(
                        api_name=API_NAME, api_version=API_VERSION
                    )
                    storage_client.objects = mock.MagicMock()
                    content = await source.get_content(
                        blob=blob_document,
                        doit=True,
                    )
                    assert content == expected_blob_document


@pytest.mark.asyncio
async def test_get_content_with_upper_extension():
    """Test the module responsible for fetching the content of the file if it is extractable."""

    async with create_gcs_source() as source:
        blob_document = {
            "id": "bucket_1/blob_1/123123123",
            "component_count": None,
            "content_encoding": None,
            "content_language": None,
            "created_at": None,
            "last_updated": "2011-10-12T00:01:00Z",
            "metadata": None,
            "name": "blob_1.TXT",
            "size": "15",
            "storage_class": None,
            "_timestamp": "2011-10-12T00:01:00Z",
            "type": None,
            "url": "https://console.cloud.google.com/storage/browser/_details/bucket_1/blob_1;tab=live_object?project=dummy123",
            "version": None,
            "bucket_name": "bucket_1",
        }
        expected_blob_document = {
            "_id": "bucket_1/blob_1/123123123",
            "_timestamp": "2011-10-12T00:01:00Z",
            "_attachment": "",
        }
        blob_content_response = ""

        with mock.patch.object(
            Aiogoogle, "as_service_account", return_value=blob_content_response
        ):
            async with Aiogoogle(
                service_account_creds=source._google_storage_client.service_account_credentials
            ) as google_client:
                storage_client = await google_client.discover(
                    api_name=API_NAME, api_version=API_VERSION
                )
                storage_client.objects = mock.MagicMock()
                content = await source.get_content(
                    blob=blob_document,
                    doit=True,
                )
                assert content == expected_blob_document


@pytest.mark.asyncio
async def test_get_content_when_type_not_supported():
    """Test the module responsible for fetching the content of the file if it is not extractable or doit is not true."""

    async with create_gcs_source() as source:
        blob_document = {
            "_id": "bucket_1/blob_1/123123123",
            "component_count": None,
            "content_encoding": None,
            "content_language": None,
            "created_at": None,
            "last_updated": "2011-10-12T00:01:00Z",
            "metadata": None,
            "name": "blob_1.cc",
            "size": "15",
            "storage_class": None,
            "_timestamp": "2011-10-12T00:01:00Z",
            "type": None,
            "url": "https://console.cloud.google.com/storage/browser/_details/None/blob_1;tab=live_object?project=dummy123",
            "version": None,
            "bucket_name": None,
        }

        async with Aiogoogle(
            service_account_creds=source._google_storage_client.service_account_credentials
        ) as google_client:
            storage_client = await google_client.discover(
                api_name=API_NAME, api_version=API_VERSION
            )
            storage_client.objects = mock.MagicMock()
            content = await source.get_content(
                blob=blob_document,
                doit=True,
            )
            assert content is None

            content = await source.get_content(
                blob=blob_document,
            )
            assert content is None


@pytest.mark.asyncio
async def test_get_content_when_file_size_is_large(catch_stdout):
    """Test the module responsible for fetching the content of the file if it is not extractable or doit is not true."""

    async with create_gcs_source() as source:
        blob_document = {
            "_id": "bucket_1/blob_1/123123123",
            "component_count": None,
            "content_encoding": None,
            "content_language": None,
            "created_at": None,
            "last_updated": "2011-10-12T00:01:00Z",
            "metadata": None,
            "name": "blob_1.txt",
            "size": 10000000000000,
            "storage_class": None,
            "_timestamp": "2011-10-12T00:01:00Z",
            "type": None,
            "url": "https://console.cloud.google.com/storage/browser/_details/None/blob_1;tab=live_object?project=dummy123",
            "version": None,
            "bucket_name": None,
        }

        async with Aiogoogle(
            service_account_creds=source._google_storage_client.service_account_credentials
        ) as google_client:
            storage_client = await google_client.discover(
                api_name=API_NAME, api_version=API_VERSION
            )
            storage_client.objects = mock.MagicMock()
            content = await source.get_content(
                blob=blob_document,
                doit=True,
            )
            assert content is None

            content = await source.get_content(
                blob=blob_document,
            )
            assert content is None


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_api_call_for_attribute_error(catch_stdout):
    """Tests the api_call method when resource attribute is not present in the getattr."""

    async with create_gcs_source() as source:
        with mock.patch.object(
            Aiogoogle, "discover", side_effect=AttributeError
        ):
            with pytest.raises(AttributeError):
                async for resp in source._google_storage_client.api_call(
                    resource="buckets_dummy",
                    method="list",
                    full_response=True,
                    project=source._google_storage_client.user_project_id,
                    userProject=source._google_storage_client.user_project_id,
                ):
                    assert resp is not None
