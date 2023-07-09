#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Google Cloud Storage source class methods.
"""
import asyncio
import json
from unittest import mock

import pytest
from aiogoogle import Aiogoogle
from aiogoogle.auth.managers import ServiceAccountManager
from aiogoogle.models import Request, Response

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.google_cloud_storage import GoogleCloudStorageDataSource

SERVICE_ACCOUNT_CREDENTIALS = '{"project_id": "dummy123"}'
API_NAME = "storage"
API_VERSION = "v1"


def get_gcs_source_object():
    """Creates the mocked Google cloud storage object.

    Returns:
        GoogleCloudStorageDataSource: Mocked object of the data source class.
    """
    configuration = DataSourceConfiguration(
        {"service_account_credentials": SERVICE_ACCOUNT_CREDENTIALS, "retry_count": 0}
    )
    mocked_gcs_object = GoogleCloudStorageDataSource(configuration=configuration)
    return mocked_gcs_object


def test_get_configuration():
    """Tests the get configurations method of the Google Cloud source class."""

    # Setup

    google_cloud_storage_object = GoogleCloudStorageDataSource

    # Execute

    config = DataSourceConfiguration(
        config=google_cloud_storage_object.get_default_configuration()
    )

    # Assert

    assert type(config["service_account_credentials"]) == str
    assert json.loads(
        config["service_account_credentials"].encode("unicode_escape").decode()
    )


@pytest.mark.asyncio
async def test_empty_configuration():
    """Tests the validity of the configurations passed to the Google Cloud source class."""

    # Setup
    configuration = DataSourceConfiguration({"service_account_credentials": ""})
    gcs_object = GoogleCloudStorageDataSource(configuration=configuration)

    # Execute
    with pytest.raises(
        ConfigurableFieldValueError,
        match="Field validation errors: 'Service_account_credentials' cannot be empty.",
    ):
        await gcs_object.validate_config()


@pytest.mark.asyncio
async def test_raise_on_invalid_configuration():
    # Setup
    configuration = DataSourceConfiguration(
        {"service_account_credentials": "{'abc':'bcd','cd'}"}
    )
    gcs_object = GoogleCloudStorageDataSource(configuration=configuration)

    # Execute
    with pytest.raises(
        ConfigurableFieldValueError,
        match="Google Cloud service account is not a valid JSON",
    ):
        await gcs_object.validate_config()


@pytest.mark.asyncio
async def test_ping_for_successful_connection(catch_stdout):
    """Tests the ping functionality for ensuring connection to Google Cloud Storage."""

    # Setup

    expected_response = {
        "kind": "storage#serviceAccount",
        "email_address": "serviceaccount@email.com",
    }
    mocked_gcs_object = get_gcs_source_object()
    as_service_account_response = asyncio.Future()
    as_service_account_response.set_result(expected_response)

    # Execute

    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=as_service_account_response
    ):
        await mocked_gcs_object.ping()


@pytest.mark.asyncio
async def test_ping_for_failed_connection(catch_stdout):
    """Tests the ping functionality when connection can not be established to Google Cloud Storage."""

    # Setup

    mocked_gcs_object = get_gcs_source_object()

    # Execute
    with mock.patch.object(
        Aiogoogle, "discover", side_effect=Exception("Something went wrong")
    ):
        with pytest.raises(Exception):
            await mocked_gcs_object.ping()


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
def test_get_blob_document(previous_documents_list, updated_documents_list):
    """Tests the function which modifies the fetched blobs and maps the values to keys.

    Args:
        previous_documents_list (list): List of the blobs documents fetched from Google Cloud Storage.
        updated_documents_list (list): List of the documents returned by the method.
    """

    # Setup
    mocked_gcs_object = get_gcs_source_object()

    # Execute and Assert
    assert updated_documents_list == list(
        mocked_gcs_object.get_blob_document(blobs=previous_documents_list[0])
    )


@pytest.mark.asyncio
async def test_fetch_buckets():
    """Tests the method which lists the storage buckets available in Google Cloud Storage."""

    # Setup
    mocked_gcs_object = get_gcs_source_object()
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

    # Execute
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=expected_response_object
    ):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            bucket_list = []
            async for bucket in mocked_gcs_object.fetch_buckets():
                bucket_list.append(bucket)

    # Assert
    assert bucket_list == expected_bucket_list


@pytest.mark.asyncio
async def test_fetch_blobs():
    """Tests the method responsible to yield blobs from Google Cloud Storage bucket."""

    # Setup
    mocked_gcs_object = get_gcs_source_object()
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

    # Execute
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=expected_response_object
    ):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            async for blob_result in mocked_gcs_object.fetch_blobs(
                buckets=expected_bucket_response
            ):
                # Assert
                assert blob_result == dummy_blob_response


@pytest.mark.asyncio
async def test_get_docs():
    """Tests the module responsible to fetch and yield blobs documents from Google Cloud Storage."""

    # Setup
    mocked_gcs_object = get_gcs_source_object()
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

    # Execute and Assert
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=expected_response_object
    ):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            async for blob_document in mocked_gcs_object.get_docs():
                assert blob_document[0] == expected_blob_document


@pytest.mark.asyncio
async def test_get_docs_when_no_buckets_present():
    """Tests the module responsible to fetch and yield blobs documents from Google Cloud Storage. When
    Cloud storage does not have any buckets.
    """

    # Setup
    mocked_gcs_object = get_gcs_source_object()
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

    # Execute and Assert
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=expected_response_object
    ):
        with mock.patch.object(ServiceAccountManager, "refresh"):
            async for blob_document in mocked_gcs_object.get_docs():
                assert blob_document[0] is None


@pytest.mark.asyncio
async def test_get_content():
    """Test the module responsible for fetching the content of the file if it is extractable."""

    # Setup
    mocked_gcs_object = get_gcs_source_object()
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
        "_attachment": "",
    }
    blob_content_response = ""

    # Execute and Assert
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=blob_content_response
    ):
        async with Aiogoogle(
            service_account_creds=mocked_gcs_object._google_storage_client.service_account_credentials
        ) as google_client:
            storage_client = await google_client.discover(
                api_name=API_NAME, api_version=API_VERSION
            )
            storage_client.objects = mock.MagicMock()
            content = await mocked_gcs_object.get_content(
                blob=blob_document,
                doit=True,
            )
            assert content == expected_blob_document


@pytest.mark.asyncio
async def test_get_content_with_upper_extension():
    """Test the module responsible for fetching the content of the file if it is extractable."""

    # Setup
    mocked_gcs_object = get_gcs_source_object()
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

    # Execute and Assert
    with mock.patch.object(
        Aiogoogle, "as_service_account", return_value=blob_content_response
    ):
        async with Aiogoogle(
            service_account_creds=mocked_gcs_object._google_storage_client.service_account_credentials
        ) as google_client:
            storage_client = await google_client.discover(
                api_name=API_NAME, api_version=API_VERSION
            )
            storage_client.objects = mock.MagicMock()
            content = await mocked_gcs_object.get_content(
                blob=blob_document,
                doit=True,
            )
            assert content == expected_blob_document


@pytest.mark.asyncio
async def test_get_content_when_type_not_supported():
    """Test the module responsible for fetching the content of the file if it is not extractable or doit is not true."""

    # Setup
    mocked_gcs_object = get_gcs_source_object()
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

    # Execute and Assert
    async with Aiogoogle(
        service_account_creds=mocked_gcs_object._google_storage_client.service_account_credentials
    ) as google_client:
        storage_client = await google_client.discover(
            api_name=API_NAME, api_version=API_VERSION
        )
        storage_client.objects = mock.MagicMock()
        content = await mocked_gcs_object.get_content(
            blob=blob_document,
            doit=True,
        )
        assert content is None

        content = await mocked_gcs_object.get_content(
            blob=blob_document,
        )
        assert content is None


@pytest.mark.asyncio
async def test_get_content_when_file_size_is_large(catch_stdout):
    """Test the module responsible for fetching the content of the file if it is not extractable or doit is not true."""

    # Setup
    mocked_gcs_object = get_gcs_source_object()
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

    # Execute and Assert
    async with Aiogoogle(
        service_account_creds=mocked_gcs_object._google_storage_client.service_account_credentials
    ) as google_client:
        storage_client = await google_client.discover(
            api_name=API_NAME, api_version=API_VERSION
        )
        storage_client.objects = mock.MagicMock()
        content = await mocked_gcs_object.get_content(
            blob=blob_document,
            doit=True,
        )
        assert content is None

        content = await mocked_gcs_object.get_content(
            blob=blob_document,
        )
        assert content is None


@pytest.mark.asyncio
async def test_api_call_for_attribute_error(catch_stdout):
    """Tests the api_call method when resource attribute is not present in the getattr."""

    # Setup

    mocked_gcs_object = get_gcs_source_object()

    # Execute
    with pytest.raises(AttributeError):
        async for _ in mocked_gcs_object._google_storage_client.api_call(
            resource="buckets_dummy",
            method="list",
            full_response=True,
            project=mocked_gcs_object._google_storage_client.user_project_id,
            userProject=mocked_gcs_object._google_storage_client.user_project_id,
        ):
            print("Method called successfully....")
