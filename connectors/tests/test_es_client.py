#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64
from unittest import mock

import pytest
from elasticsearch import ConnectionError

from connectors.es.client import ESClient


def test_esclient():
    # creating a client with a minimal config should create one with sane
    # defaults
    config = {"username": "elastic", "password": "changeme"}
    es_client = ESClient(config)
    assert es_client.host.host == "localhost"
    assert es_client.host.port == 9200
    assert es_client.host.scheme == "http"

    # XXX find a more elegant way
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
async def test_es_client_no_server(patch_logger):
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
