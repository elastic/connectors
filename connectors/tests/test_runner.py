#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import pytest
import asyncio
from unittest import mock

from connectors.runner import ConnectorService, run
from connectors.byoc import _CONNECTORS_CACHE


CONFIG = os.path.join(os.path.dirname(__file__), "config.yml")


FAKE_CONFIG = {
    "api_key_id": "",
    "configuration": {
        "host": {"value": "mongodb://127.0.0.1:27021", "label": "MongoDB Host"},
        "database": {"value": "sample_airbnb", "label": "MongoDB Database"},
        "collection": {
            "value": "listingsAndReviews",
            "label": "MongoDB Collection",
        },
    },
    "index_name": "search-airbnb",
    "service_type": "fake",
    "status": "configured",
    "last_sync_status": "null",
    "last_sync_error": "",
    "last_synced": "",
    "last_seen": "",
    "created_at": "",
    "updated_at": "",
    "scheduling": {"enabled": True, "interval": "0 * * * *"},
    "sync_now": True,
}


def test_bad_config():
    with pytest.raises(OSError):
        ConnectorService("BEEUUUAH")


@pytest.mark.asyncio
async def test_connector_service_list(patch_logger):
    service = ConnectorService(CONFIG)
    await service.get_list()
    assert patch_logger.logs == ["Registered connectors:", "- Fakey"]


class FakeSource:
    """Fakey"""

    service_type = "fake"

    def __init__(self, *args):
        pass

    async def changed(self):
        return True

    async def ping(self):
        pass

    async def get_docs(self):
        yield {"_id": 1}, None


def set_server_responses(mock_responses):

    _CONNECTORS_CACHE.clear()
    headers = {"X-Elastic-Product": "Elasticsearch"}

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={"hits": {"hits": [{"_id": "1", "_source": FAKE_CONFIG}]}},
        headers=headers,
    )
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors-sync-jobs/_doc",
        payload={"_id": "1"},
        headers=headers,
    )

    mock_responses.put(
        "http://nowhere.com:9200/.elastic-connectors-sync-jobs/_doc/1",
        payload={"_id": "1"},
        headers=headers,
    )

    mock_responses.put(
        "http://nowhere.com:9200/.elastic-connectors/_doc/1",
        payload={"_id": "1"},
        headers=headers,
    )

    mock_responses.put(
        "http://nowhere.com:9200/.elastic-connectors/_doc/1",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.head(
        "http://nowhere.com:9200/search-airbnb?expand_wildcards=hidden", headers=headers
    )

    mock_responses.get(
        "http://nowhere.com:9200/search-airbnb",
        payload={"hits": {"hits": [{"_id": "1", "_source": FAKE_CONFIG}]}},
        headers=headers,
    )
    mock_responses.get(
        "http://nowhere.com:9200/search-airbnb/_search?scroll=5m",
        payload={"hits": {"hits": [{"_id": "1", "_source": FAKE_CONFIG}]}},
        headers=headers,
    )
    mock_responses.post(
        "http://nowhere.com:9200/search-airbnb/_search?scroll=5m",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-airbnb/_search?scroll=5m",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/_bulk",
        payload={"items": []},
        headers=headers,
    )


@pytest.mark.asyncio
async def test_connector_service_poll(mock_responses, patch_logger):
    from connectors.byoc import BYOIndex

    async def _ping(*args):
        return True

    BYOIndex.ping = _ping

    set_server_responses(mock_responses)
    service = ConnectorService(CONFIG)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.poll()


def test_connector_service_run(mock_responses, patch_logger):
    args = mock.MagicMock()
    args.config_file = CONFIG
    args.action = "list"
    assert run(args) == 0
    assert patch_logger.logs == ["Registered connectors:", "- Fakey"]
