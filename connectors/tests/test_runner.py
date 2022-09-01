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
from connectors.source import DataSourceError, _CACHED_SOURCES


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


FAKE_CONFIG_BUGGY_SERVICE = {
    "api_key_id": "",
    "configuration": {"raise": {"value": True, "label": ""}},
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


FAKE_CONFIG_UNKNOWN_SERVICE = {
    "api_key_id": "",
    "configuration": {},
    "index_name": "search-airbnb",
    "service_type": "UNKNOWN",
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
async def test_connector_service_list(patch_logger, set_env):
    service = ConnectorService(CONFIG)
    await service.get_list()
    assert patch_logger.logs == ["Registered connectors:", "- Fakey"]


class FakeSource:
    """Fakey"""

    service_type = "fake"

    def __init__(self, connector):
        if connector.configuration.has_field("raise"):
            raise Exception("I break")

    async def changed(self):
        return True

    async def ping(self):
        pass

    async def get_docs(self):
        yield {"_id": 1}, None

    @classmethod
    def get_default_configuration(cls):
        return []


def set_server_responses(mock_responses, config=FAKE_CONFIG):

    _CONNECTORS_CACHE.clear()
    _CACHED_SOURCES.clear()

    headers = {"X-Elastic-Product": "Elasticsearch"}

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={"hits": {"hits": [{"_id": "1", "_source": config}]}},
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
        payload={"hits": {"hits": [{"_id": "1", "_source": config}]}},
        headers=headers,
    )
    mock_responses.get(
        "http://nowhere.com:9200/search-airbnb/_search?scroll=5m",
        payload={"hits": {"hits": [{"_id": "1", "_source": config}]}},
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
async def test_connector_service_poll(
    mock_responses, patch_logger, patch_ping, set_env
):
    set_server_responses(mock_responses)
    service = ConnectorService(CONFIG)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.poll()


@pytest.mark.asyncio
async def test_connector_service_poll_unknown_service(
    mock_responses, patch_logger, patch_ping, set_env
):

    set_server_responses(mock_responses, FAKE_CONFIG_UNKNOWN_SERVICE)
    service = ConnectorService(CONFIG)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.poll()
    assert "Can't handle source of type UNKNOWN" in patch_logger.logs


@pytest.mark.asyncio
async def test_connector_service_poll_buggy_service(
    mock_responses, patch_logger, patch_ping, set_env
):

    set_server_responses(mock_responses, FAKE_CONFIG_BUGGY_SERVICE)
    service = ConnectorService(CONFIG)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.poll()
    for log in patch_logger.logs:
        if isinstance(log, DataSourceError):
            return
    raise AssertionError


def test_connector_service_run(mock_responses, patch_logger, set_env):
    args = mock.MagicMock()
    args.config_file = CONFIG
    args.action = "list"
    assert run(args) == 0
    assert patch_logger.logs == ["Registered connectors:", "- Fakey", "Bye"]


@pytest.mark.asyncio
async def test_ping_fails(mock_responses, patch_logger, set_env):
    from connectors.byoc import BYOIndex

    async def _ping(*args):
        return False

    BYOIndex.ping = _ping

    service = ConnectorService(CONFIG)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.poll()
    assert patch_logger.logs[-1] == "http://nowhere.com:9200 seem down. Bye!"


@pytest.mark.asyncio
async def test_spurious(mock_responses, patch_logger, patch_ping, set_env):
    set_server_responses(mock_responses)

    from connectors.byoc import BYOConnector

    async def _sync(*args):
        raise Exception("me")

    old_sync = BYOConnector.sync
    BYOConnector.sync = _sync

    try:
        service = ConnectorService(CONFIG)
        service.idling = 0
        service.service_config["max_errors"] = 0
        await service.poll()
    except Exception:
        pass
    finally:
        BYOConnector.sync = old_sync

    assert patch_logger.logs[-1].args[0] == "me"


@pytest.mark.asyncio
async def test_spurious_continue(mock_responses, patch_logger, patch_ping, set_env):
    set_server_responses(mock_responses)

    from connectors.byoc import BYOConnector

    async def _sync(*args):
        raise Exception("me")

    old_sync = BYOConnector.sync
    BYOConnector.sync = _sync

    set_server_responses(mock_responses)
    headers = {"X-Elastic-Product": "Elasticsearch"}

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={"hits": {"hits": [{"_id": "1", "_source": FAKE_CONFIG}]}},
        headers=headers,
    )

    try:
        service = ConnectorService(CONFIG)
        asyncio.get_event_loop().call_soon(service.stop)
        await service.poll()
    except Exception:
        pass
    finally:
        BYOConnector.sync = old_sync

    assert isinstance(patch_logger.logs[-3], Exception)
