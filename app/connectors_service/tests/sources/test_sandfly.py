#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio
from aiohttp.client_exceptions import ClientResponseError
from connectors_sdk.logger import logger
from connectors_sdk.source import (
    CURSOR_SYNC_TIMESTAMP,
)

from connectors.sources.sandfly import (
    SandflyClient,
    SandflyDataSource,
)
from connectors.sources.sandfly.client import (
    FetchTokenError,
    ResourceNotFound,
)
from connectors.sources.sandfly.datasource import (
    CURSOR_SEQUENCE_ID_KEY,
    SandflyLicenseExpired,
    SandflyNotLicensed,
    SyncCursorEmpty,
    extract_sandfly_date,
    format_sandfly_date,
)
from tests.sources.support import create_source

SANDFLY_SERVER_URL = "https://blackbird.sandflysecurity.com/v4"
URL_SANDFLY_LOGIN = SANDFLY_SERVER_URL + "/auth/login"
URL_SANDFLY_LICENSE = SANDFLY_SERVER_URL + "/license"
URL_SANDFLY_HOSTS = SANDFLY_SERVER_URL + "/hosts"
URL_SANDFLY_SSH_SUMMARY = SANDFLY_SERVER_URL + "/sshhunter/summary"
URL_SANDFLY_SSH_KEY1 = SANDFLY_SERVER_URL + "/sshhunter/key/1"
URL_SANDFLY_SSH_KEY2 = SANDFLY_SERVER_URL + "/sshhunter/key/2"
URL_SANDFLY_RESULTS = SANDFLY_SERVER_URL + "/results"

configuration = {
    "server_url": SANDFLY_SERVER_URL,
    "username": "elastic_api_user",
    "password": "elastic_api_password@@",
    "enable_pass": False,
    "verify_ssl": True,
    "fetch_days": 30,
}

# Login Token Response Data
TOKEN_RESPONSE_DATA = {"access_token": "Token#123", "refresh_token": "Refresh#123"}
FAILED_LOGIN_RESPONSE_DATA = {
    "data": "",
    "detail": "authentication failed",
    "status": 403,
    "title": "Forbidden",
}

# License Response Data
LICENSE_EXPIRED_RESPONSE_DATA = {
    "version": 3,
    "date": {"expiry": "2025-04-30T19:30:45Z"},
    "customer": {"name": "Sandfly"},
    "limits": {"features": ["demo", "elasticsearch_replication"]},
}
NOT_LICENSED_RESPONSE_DATA = {
    "version": 3,
    "date": {"expiry": "2026-12-30T19:30:45Z"},
    "customer": {"name": "Sandfly"},
    "limits": {"features": ["demo"]},
}
LICENSE_RESPONSE_DATA = {
    "version": 3,
    "date": {"expiry": "2026-12-30T19:30:45Z"},
    "customer": {"name": "Sandfly"},
    "limits": {"features": ["demo", "elasticsearch_replication"]},
}

# Hosts Response Data
HOSTS_RESPONSE_DATA = {
    "data": [
        {
            "host_id": "1001",
            "hostname": "192.168.11.201",
            "data": {"os": {"info": {"node": "sandfly-target"}}},
        },
        {
            "host_id": "1002",
            "hostname": "192.168.11.199",
            "data": {"os": {"info": {"node": "sandfly-server"}}},
        },
        {
            "host_id": "1003",
            "hostname": "192.168.11.197",
            "data": None,
        },
    ],
}

# SSH Keys Response Data
SSH_SUMMARY_RESPONSE_DATA = {
    "more_results": False,
    "data": [
        {"id": "1"},
        {"id": "2"},
    ],
}
SSH_KEY1_RESPONSE_DATA = {
    "id": "1",
    "friendly_name": "a b c",
    "key_value": "KeyValue#123",
}
SSH_KEY2_RESPONSE_DATA = {
    "id": "2",
    "friendly_name": "d e f",
    "key_value": "KeyValue#456",
}

# Results Response Data
RESULTS_MORE_RESPONSE_DATA = {
    "more_results": True,
    "total": 2,
    "data": [
        {
            "sequence_id": "1001",
            "external_id": "1001",
            "header": {"end_time": "2025-06-01T13:45:29Z"},
            "data": {"key_data": "my key data", "status": "alert"},
        },
        {
            "sequence_id": "1002",
            "external_id": "1002",
            "header": {"end_time": "2025-06-01T13:46:17Z"},
            "data": {"key_data": "", "status": "alert"},
        },
    ],
}
RESULTS_NO_MORE_RESPONSE_DATA = {
    "more_results": False,
    "total": 2,
    "data": [
        {
            "sequence_id": "1003",
            "external_id": "1003",
            "header": {"end_time": "2025-06-01T13:47:41Z"},
            "data": {"key_data": "my key data", "status": "alert"},
        },
        {
            "sequence_id": "1004",
            "external_id": "1004",
            "header": {"end_time": "2025-06-01T13:48:10Z"},
            "data": {"key_data": "", "status": "alert"},
        },
    ],
}


@pytest_asyncio.fixture
async def sandfly_client():
    client = SandflyClient(configuration)
    client.set_logger(logger)
    yield client
    await client.close()


@pytest_asyncio.fixture
async def sandfly_data_source():
    async with create_source(SandflyDataSource, **configuration) as source:
        source.set_logger(logger)
        yield source
        await source.close()


@pytest.mark.asyncio
async def test_sandfly_date(sandfly_client, mock_responses):
    expiry = "2025-06-23T17:35:23Z"
    expiry_date = extract_sandfly_date(expiry)
    assert type(expiry_date) is datetime

    now = datetime.utcnow()
    date_str1 = format_sandfly_date(now, True)
    assert type(date_str1) is str and len(date_str1) == 20
    date_str2 = format_sandfly_date(now, False)
    assert type(date_str2) is str and len(date_str2) == 20


# Tests for SandflyClient


@pytest.mark.asyncio
async def test_client_ping_success(sandfly_client, mock_responses):
    mock_responses.head(
        SANDFLY_SERVER_URL,
        status=401,  # Error code 401 Unauthorized means server is running
    )
    assert await sandfly_client.ping() is True


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_client_ping_failure(sandfly_client, mock_responses):
    request_error = ClientResponseError(None, None)
    request_error.status = 403
    request_error.message = "Forbidden"
    mock_responses.head(
        SANDFLY_SERVER_URL,
        exception=request_error,
    )

    with pytest.raises(ClientResponseError):
        await sandfly_client.ping()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_client_login_failures(sandfly_client, mock_responses):
    request_error = FetchTokenError(None, None)
    request_error.status = 403
    request_error.message = "Forbidden"

    mock_responses.post(
        URL_SANDFLY_LOGIN,
        exception=request_error,
    )

    mock_responses.get(
        URL_SANDFLY_LICENSE,
        exception=request_error,
    )

    with pytest.raises(FetchTokenError):
        async for _ in sandfly_client.get_license():
            pass

    mock_responses.post(
        URL_SANDFLY_RESULTS,
        exception=request_error,
    )

    with pytest.raises(FetchTokenError):
        time_since = "2025-06-01T00:00:00Z"
        async for _, _ in sandfly_client.get_results_by_time(time_since, False):
            pass


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_client_resource_not_found(sandfly_client, mock_responses):
    mock_responses.post(
        URL_SANDFLY_LOGIN,
        status=200,
        payload=TOKEN_RESPONSE_DATA,
    )

    request_error = ClientResponseError(None, None)
    request_error.status = 404
    request_error.message = "Resource Not Found"

    mock_responses.get(
        URL_SANDFLY_LICENSE,
        exception=request_error,
    )

    with pytest.raises(ResourceNotFound):
        async for _ in sandfly_client.get_license():
            pass

    mock_responses.post(
        URL_SANDFLY_RESULTS,
        exception=request_error,
    )

    with pytest.raises(ResourceNotFound):
        time_since = "2025-06-01T00:00:00Z"
        async for _, _ in sandfly_client.get_results_by_time(time_since, False):
            pass


@pytest.mark.asyncio
async def test_client_get_license(sandfly_client, mock_responses):
    mock_responses.post(
        URL_SANDFLY_LOGIN,
        status=200,
        payload=TOKEN_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_LICENSE,
        status=200,
        payload=LICENSE_RESPONSE_DATA,
    )

    async for license_data in sandfly_client.get_license():
        customer_name = license_data["customer"]["name"]
        assert customer_name == "Sandfly"
        expiry = license_data["date"]["expiry"]
        assert expiry == "2026-12-30T19:30:45Z"


@pytest.mark.asyncio
async def test_client_get_hosts(sandfly_client, mock_responses):
    mock_responses.post(
        URL_SANDFLY_LOGIN,
        status=200,
        payload=TOKEN_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_HOSTS,
        status=200,
        payload=HOSTS_RESPONSE_DATA,
    )

    async for host_item in sandfly_client.get_hosts():
        hostid = host_item["host_id"]
        assert hostid in ("1001", "1002", "1003")
        hostname = host_item["hostname"]
        assert "192.168.11." in hostname
        if "data" in host_item:
            if host_item["data"] is not None:
                nodename = host_item["data"]["os"]["info"]["node"]
                assert "sandfly-" in nodename


@pytest.mark.asyncio
async def test_client_get_ssh_keys(sandfly_client, mock_responses):
    mock_responses.post(
        URL_SANDFLY_LOGIN,
        status=200,
        payload=TOKEN_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_SSH_SUMMARY,
        status=200,
        payload=SSH_SUMMARY_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_SSH_KEY1,
        status=200,
        payload=SSH_KEY1_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_SSH_KEY2,
        status=200,
        payload=SSH_KEY2_RESPONSE_DATA,
    )

    async for key_item, get_more_results in sandfly_client.get_ssh_keys():
        assert get_more_results is False
        friendly = key_item["friendly_name"]
        assert friendly in ("a b c", "d e f")
        key_value = key_item["key_value"]
        assert "KeyValue#" in key_value


@pytest.mark.asyncio
async def test_client_get_results_by_time(sandfly_client, mock_responses):
    mock_responses.post(
        URL_SANDFLY_LOGIN,
        status=200,
        payload=TOKEN_RESPONSE_DATA,
    )

    mock_responses.post(
        URL_SANDFLY_RESULTS,
        status=200,
        payload=RESULTS_NO_MORE_RESPONSE_DATA,
    )

    time_since = "2025-06-01T00:00:00Z"
    async for result_item, get_more_results in sandfly_client.get_results_by_time(
        time_since, False
    ):
        assert get_more_results is False
        last_sequence_id = result_item["sequence_id"]
        assert last_sequence_id in ("1003", "1004")
        status = result_item["data"]["status"]
        assert status == "alert"

    mock_responses.post(
        URL_SANDFLY_RESULTS,
        status=200,
        payload=RESULTS_NO_MORE_RESPONSE_DATA,
    )

    async for result_item, get_more_results in sandfly_client.get_results_by_time(
        time_since, True
    ):
        assert get_more_results is False
        last_sequence_id = result_item["sequence_id"]
        assert last_sequence_id in ("1003", "1004")
        status = result_item["data"]["status"]
        assert status == "alert"


@pytest.mark.asyncio
async def test_client_get_results_by_id(sandfly_client, mock_responses):
    mock_responses.post(
        URL_SANDFLY_LOGIN,
        status=200,
        payload=TOKEN_RESPONSE_DATA,
    )

    mock_responses.post(
        URL_SANDFLY_RESULTS,
        status=200,
        payload=RESULTS_NO_MORE_RESPONSE_DATA,
    )

    last_sequence_id = "1000"
    async for result_item, get_more_results in sandfly_client.get_results_by_id(
        last_sequence_id, False
    ):
        assert get_more_results is False
        last_sequence_id = result_item["sequence_id"]
        assert last_sequence_id in ("1003", "1004")
        status = result_item["data"]["status"]
        assert status == "alert"

    mock_responses.post(
        URL_SANDFLY_RESULTS,
        status=200,
        payload=RESULTS_NO_MORE_RESPONSE_DATA,
    )

    async for result_item, get_more_results in sandfly_client.get_results_by_id(
        last_sequence_id, True
    ):
        assert get_more_results is False
        last_sequence_id = result_item["sequence_id"]
        assert last_sequence_id in ("1003", "1004")
        status = result_item["data"]["status"]
        assert status == "alert"


# Tests for SandflyDataSource


@pytest.mark.asyncio
async def test_data_source_ping_success(sandfly_data_source, mock_responses):
    mock_responses.head(
        SANDFLY_SERVER_URL,
        status=401,  # Error code 401 Unauthorized means server is running
    )
    assert await sandfly_data_source.ping() is True


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_data_source_ping_failure(sandfly_data_source, mock_responses):
    request_error = ClientResponseError(None, None)
    request_error.status = 403
    request_error.message = "Forbidden"
    mock_responses.head(
        SANDFLY_SERVER_URL,
        exception=request_error,
    )
    with pytest.raises(ClientResponseError):
        await sandfly_data_source.ping()


# get_docs()


@pytest.mark.asyncio
async def test_data_source_get_docs_license_expired(
    sandfly_data_source, mock_responses
):
    mock_responses.post(
        URL_SANDFLY_LOGIN,
        status=200,
        payload=TOKEN_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_LICENSE,
        status=200,
        payload=LICENSE_EXPIRED_RESPONSE_DATA,
    )

    with pytest.raises(SandflyLicenseExpired):
        docs = []
        async for doc, _ in sandfly_data_source.get_docs():
            docs.append(doc)


@pytest.mark.asyncio
async def test_data_source_get_docs_not_licensed(sandfly_data_source, mock_responses):
    mock_responses.post(
        URL_SANDFLY_LOGIN,
        status=200,
        payload=TOKEN_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_LICENSE,
        status=200,
        payload=NOT_LICENSED_RESPONSE_DATA,
    )

    with pytest.raises(SandflyNotLicensed):
        docs = []
        async for doc, _ in sandfly_data_source.get_docs():
            docs.append(doc)


@pytest.mark.asyncio
async def test_data_source_get_docs(sandfly_data_source, mock_responses):
    mock_responses.post(
        URL_SANDFLY_LOGIN,
        status=200,
        payload=TOKEN_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_LICENSE,
        status=200,
        payload=LICENSE_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_HOSTS,
        status=200,
        payload=HOSTS_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_SSH_SUMMARY,
        status=200,
        payload=SSH_SUMMARY_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_SSH_KEY1,
        status=200,
        payload=SSH_KEY1_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_SSH_KEY2,
        status=200,
        payload=SSH_KEY2_RESPONSE_DATA,
    )

    mock_responses.post(
        URL_SANDFLY_RESULTS,
        status=200,
        payload=RESULTS_MORE_RESPONSE_DATA,
    )

    mock_responses.post(
        URL_SANDFLY_RESULTS,
        status=200,
        payload=RESULTS_NO_MORE_RESPONSE_DATA,
    )

    docs = []
    async for doc, _ in sandfly_data_source.get_docs():
        docs.append(doc)


# get_docs_incrementally()


@pytest.mark.asyncio
@pytest.mark.parametrize("sync_cursor", [None, {}])
async def test_data_source_get_docs_inc_empty_sync_cursor(
    sandfly_data_source, mock_responses, sync_cursor
):
    with pytest.raises(SyncCursorEmpty):
        docs = []
        async for doc, _, _ in sandfly_data_source.get_docs_incrementally(
            sync_cursor=sync_cursor
        ):
            docs.append(doc)


@pytest.mark.asyncio
async def test_data_source_get_docs_inc_license_expired(
    sandfly_data_source, mock_responses
):
    mock_responses.post(
        URL_SANDFLY_LOGIN,
        status=200,
        payload=TOKEN_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_LICENSE,
        status=200,
        payload=LICENSE_EXPIRED_RESPONSE_DATA,
    )

    with pytest.raises(SandflyLicenseExpired):
        docs = []
        sync_cursor = {
            CURSOR_SYNC_TIMESTAMP: "2025-05-28T11:15:35Z",
            CURSOR_SEQUENCE_ID_KEY: "1000",
        }
        async for doc, _, _ in sandfly_data_source.get_docs_incrementally(
            sync_cursor=sync_cursor
        ):
            docs.append(doc)


@pytest.mark.asyncio
async def test_data_source_get_docs_inc_not_licensed(
    sandfly_data_source, mock_responses
):
    mock_responses.post(
        URL_SANDFLY_LOGIN,
        status=200,
        payload=TOKEN_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_LICENSE,
        status=200,
        payload=NOT_LICENSED_RESPONSE_DATA,
    )

    with pytest.raises(SandflyNotLicensed):
        docs = []
        sync_cursor = {
            CURSOR_SYNC_TIMESTAMP: "2025-05-28T11:15:35Z",
            CURSOR_SEQUENCE_ID_KEY: "1000",
        }
        async for doc, _, _ in sandfly_data_source.get_docs_incrementally(
            sync_cursor=sync_cursor
        ):
            docs.append(doc)


@pytest.mark.asyncio
async def test_data_source_get_docs_inc(sandfly_data_source, mock_responses):
    mock_responses.post(
        URL_SANDFLY_LOGIN,
        status=200,
        payload=TOKEN_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_LICENSE,
        status=200,
        payload=LICENSE_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_HOSTS,
        status=200,
        payload=HOSTS_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_SSH_SUMMARY,
        status=200,
        payload=SSH_SUMMARY_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_SSH_KEY1,
        status=200,
        payload=SSH_KEY1_RESPONSE_DATA,
    )

    mock_responses.get(
        URL_SANDFLY_SSH_KEY2,
        status=200,
        payload=SSH_KEY2_RESPONSE_DATA,
    )

    mock_responses.post(
        URL_SANDFLY_RESULTS,
        status=200,
        payload=RESULTS_MORE_RESPONSE_DATA,
    )

    mock_responses.post(
        URL_SANDFLY_RESULTS,
        status=200,
        payload=RESULTS_NO_MORE_RESPONSE_DATA,
    )

    docs = []
    sync_cursor = {
        CURSOR_SYNC_TIMESTAMP: "2025-05-28T11:15:35Z",
        CURSOR_SEQUENCE_ID_KEY: "1000",
    }
    async for doc, _, _ in sandfly_data_source.get_docs_incrementally(
        sync_cursor=sync_cursor
    ):
        docs.append(doc)
