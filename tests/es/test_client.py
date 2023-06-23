#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64
from unittest import mock
from unittest.mock import AsyncMock, Mock

import pytest
from elasticsearch import ConflictError, ConnectionError

from connectors.es.client import ESClient, License, with_concurrency_control

BASIC_CONFIG = {"username": "elastic", "password": "changeme"}


def test_esclient():
    # creating a client with a minimal config should create one with sane
    # defaults

    es_client = ESClient(BASIC_CONFIG)
    assert es_client.host.host == "localhost"
    assert es_client.host.port == 9200
    assert es_client.host.scheme == "http"

    # TODO: find a more elegant way
    assert es_client.client._retry_on_timeout
    basic = f"Basic {base64.b64encode(b'elastic:changeme').decode()}"
    assert es_client.client._headers["Authorization"] == basic


@pytest.mark.asyncio
async def test_es_client_auth_error(mock_responses, patch_logger):
    headers = {"X-Elastic-Product": "Elasticsearch"}

    # if we get auth issues, we want to know about them
    config = {
        "username": "elastic",
        "password": "changeme",
        "host": "http://nowhere.com:9200",
    }
    es_client = ESClient(config)

    mock_responses.get("http://nowhere.com:9200", headers=headers, status=401)
    assert not await es_client.ping()

    es_error = {
        "error": {
            "root_cause": [
                {
                    "type": "security_exception",
                    "reason": "missing authentication credentials for REST request [/]",
                    "header": {
                        "WWW-Authenticate": [
                            'Basic realm="security" charset="UTF-8"',
                            'Bearer realm="security"',
                            "ApiKey",
                        ]
                    },
                }
            ],
            "type": "security_exception",
            "reason": "missing authentication credentials for REST request [/]",
            "header": {
                "WWW-Authenticate": [
                    'Basic realm="security" charset="UTF-8"',
                    'Bearer realm="security"',
                    "ApiKey",
                ]
            },
        },
        "status": 401,
    }

    mock_responses.get(
        "http://nowhere.com:9200", headers=headers, status=401, payload=es_error
    )
    assert not await es_client.ping()

    await es_client.close()
    patch_logger.assert_present("The server returned a 401 code")
    patch_logger.assert_present("missing authentication credentials")


@pytest.mark.asyncio
async def test_es_client_no_server():
    # if we can't reach the server, we need to catch it cleanly
    config = {
        "username": "elastic",
        "password": "changeme",
        "host": "http://nowhere.com:9200",
        "max_wait_duration": 0.1,
        "initial_backoff_duration": 0.1,
    }
    es_client = ESClient(config)

    with mock.patch.object(
        es_client.client,
        "info",
        side_effect=ConnectionError("Cannot connect - no route to host."),
    ):
        # Execute
        assert not await es_client.ping()
        await es_client.close()


@pytest.mark.asyncio
async def test_delete_indices():
    config = {
        "username": "elastic",
        "password": "changeme",
        "host": "http://nowhere.com:9200",
    }
    indices = ["search-mongo"]
    es_client = ESClient(config)
    es_client.client = Mock()
    es_client.client.indices.delete = AsyncMock()

    await es_client.delete_indices(indices=indices)
    es_client.client.indices.delete.assert_awaited_with(
        index=indices, ignore_unavailable=True
    )


@pytest.mark.asyncio
async def test_with_concurrency_control():
    mock_func = Mock()
    num_retries = 10

    @with_concurrency_control(retries=num_retries)
    async def conflict():
        mock_func()
        raise ConflictError(
            message="This is an error message from test_with_concurrency_control",
            meta=None,
            body={},
        )

    with pytest.raises(ConflictError):
        await conflict()

        assert mock_func.call_count == num_retries

    mock_func = Mock()

    @with_concurrency_control(retries=num_retries)
    async def does_not_raise():
        mock_func()
        pass

    await does_not_raise()

    assert mock_func.call_count == 1


@pytest.mark.parametrize(
    "enabled_license, licenses_enabled",
    [
        (License.BASIC, [License.BASIC]),
        (License.GOLD, [License.BASIC, License.GOLD]),
        (License.PLATINUM, [License.BASIC, License.GOLD, License.PLATINUM]),
        (
            License.ENTERPRISE,
            [License.BASIC, License.GOLD, License.PLATINUM, License.ENTERPRISE],
        ),
        (
            License.TRIAL,
            [
                License.BASIC,
                License.GOLD,
                License.PLATINUM,
                License.ENTERPRISE,
                License.TRIAL,
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_has_license_enabled(enabled_license, licenses_enabled):
    es_client = ESClient(BASIC_CONFIG)
    es_client.client = AsyncMock()
    es_client.client.license.get = AsyncMock(
        return_value={"license": {"type": enabled_license.value}}
    )

    for license_ in licenses_enabled:
        is_enabled, _ = await es_client.has_active_license_enabled(license_)
        assert is_enabled


@pytest.mark.parametrize(
    "enabled_license, licenses_disabled",
    [
        (
            License.BASIC,
            [License.GOLD, License.PLATINUM, License.ENTERPRISE, License.TRIAL],
        ),
        (License.GOLD, [License.PLATINUM, License.ENTERPRISE, License.TRIAL]),
        (License.PLATINUM, [License.ENTERPRISE, License.TRIAL]),
        (License.ENTERPRISE, [License.TRIAL]),
        (License.TRIAL, []),
    ],
)
@pytest.mark.asyncio
async def test_has_licenses_disabled(enabled_license, licenses_disabled):
    es_client = ESClient(BASIC_CONFIG)
    es_client.client = AsyncMock()
    es_client.client.license.get = AsyncMock(
        return_value={"license": {"type": enabled_license.value}}
    )

    for license_ in licenses_disabled:
        is_enabled, _ = await es_client.has_active_license_enabled(license_)
        assert not is_enabled


@pytest.mark.asyncio
async def test_has_license_disabled_with_expired_license():
    es_client = ESClient(BASIC_CONFIG)
    es_client.client = AsyncMock()
    es_client.client.license.get = AsyncMock(
        return_value={"license": {"type": License.PLATINUM, "status": "expired"}}
    )

    is_enabled, license_ = await es_client.has_active_license_enabled(License.PLATINUM)

    assert not is_enabled
    assert license_ == License.EXPIRED
