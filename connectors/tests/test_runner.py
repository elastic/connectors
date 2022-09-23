#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import pytest
import asyncio
from unittest import mock
from functools import partial
import json

from connectors.runner import ConnectorService, run
from connectors.byoc import purge_cache as purge_connectors
from connectors.source import DataSourceError, purge_cache as purge_sources
from connectors.conftest import assert_re


CONFIG = os.path.join(os.path.dirname(__file__), "config.yml")
ES_CONFIG = os.path.join(os.path.dirname(__file__), "entsearch.yml")
CONFIG_2 = os.path.join(os.path.dirname(__file__), "config_2.yml")
CONFIG_KEEP_ALIVE = os.path.join(os.path.dirname(__file__), "config_keep_alive.yml")


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
    "is_native": True,
}

FAKE_CONFIG_NOT_NATIVE = {
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
    "is_native": False,
}

LARGE_FAKE_CONFIG = dict(FAKE_CONFIG)
LARGE_FAKE_CONFIG["service_type"] = "large_fake"


FAKE_CONFIG_NO_SYNC = {
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
    "scheduling": {"enabled": False, "interval": "0 * * * *"},
    "sync_now": False,
}


FAKE_CONFIG_FAIL_SERVICE = {
    "api_key_id": "",
    "configuration": {"fail": {"value": True, "label": ""}},
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
    patch_logger.assert_present(["Registered connectors:", "- Fakey", "- Phatey"])


class FakeSource:
    """Fakey"""

    service_type = "fake"

    def __init__(self, connector):
        self.connector = connector
        if connector.configuration.has_field("raise"):
            raise Exception("I break on init")
        self.fail = connector.configuration.has_field("fail")

    async def changed(self):
        return True

    async def ping(self):
        pass

    async def close(self):
        pass

    async def _dl(self, doc_id, timestamp=None, doit=None):
        if not doit:
            return
        return {"_id": doc_id, "timestamp": timestamp, "text": "xx"}

    async def get_docs(self):
        if self.fail:
            raise Exception("I fail while syncing")
        yield {"_id": 1}, partial(self._dl, 1)

    @classmethod
    def get_default_configuration(cls):
        return []


class LargeFakeSource(FakeSource):
    """Phatey"""

    service_type = "large_fake"

    async def get_docs(self):
        for i in range(10001):
            doc_id = str(i + 1)
            yield {"_id": doc_id}, partial(self._dl, doc_id)


async def set_server_responses(mock_responses, config=FAKE_CONFIG):
    await purge_connectors()
    await purge_sources()

    headers = {"X-Elastic-Product": "Elasticsearch"}

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )
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
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors-sync-jobs/_update/1",
        headers=headers,
        repeat=True,
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

    def update_connector(url, **kw):
        read_only_fields = [
            "is_native",
            "service_type",
            "api_key_id",
            "pipeline",
            "scheduling",
            "configuration",
        ]
        fields = json.loads(kw["data"])["doc"].keys()
        for field in fields:
            assert field not in read_only_fields

    mock_responses.put(
        "http://nowhere.com:9200/.elastic-connectors/_doc/1",
        callback=update_connector,
        headers=headers,
    )
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_update/1",
        headers=headers,
        callback=update_connector,
        repeat=True,
    )
    mock_responses.head(
        "http://nowhere.com:9200/search-airbnb?expand_wildcards=open", headers=headers
    )
    mock_responses.get(
        "http://nowhere.com:9200/search-airbnb/_mapping?expand_wildcards=open",
        payload={"search-airbnb": {"mappings": {}}},
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-airbnb/_mapping?expand_wildcards=open",
        headers=headers,
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
        "http://nowhere.com:9200/_bulk?pipeline=ent-search-generic-ingestion",
        payload={"items": []},
        headers=headers,
        repeat=True,
    )


@pytest.mark.asyncio
async def test_connector_service_poll(
    mock_responses, patch_logger, patch_ping, set_env
):
    await set_server_responses(mock_responses)
    service = ConnectorService(CONFIG)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.poll()
    patch_logger.assert_present("Sync done: 1 indexed, 0  deleted. (0 seconds)")


@pytest.mark.asyncio
async def test_connector_service_poll_large(
    mock_responses, patch_logger, patch_ping, set_env
):
    await set_server_responses(mock_responses, LARGE_FAKE_CONFIG)
    service = ConnectorService(CONFIG)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.poll()
    assert_re(r"Sync done: 10001 indexed, 0  deleted", patch_logger.logs)


@pytest.mark.asyncio
async def test_connector_service_poll_not_native(
    mock_responses, patch_logger, patch_ping, set_env
):
    await set_server_responses(mock_responses, FAKE_CONFIG_NOT_NATIVE)
    service = ConnectorService(CONFIG_2)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.poll()
    patch_logger.assert_present("Connector 1 of type fake not supported, ignoring")


@pytest.mark.asyncio
async def test_connector_service_poll_with_entsearch(
    mock_responses, patch_logger, patch_ping, set_env
):
    with mock.patch.dict(os.environ, {"ENT_SEARCH_CONFIG_PATH": ES_CONFIG}):
        await set_server_responses(mock_responses)
        service = ConnectorService(CONFIG)
        asyncio.get_event_loop().call_soon(service.stop)
        await service.poll()
        for log in patch_logger.logs:
            print(log)
        patch_logger.assert_present("Sync done: 1 indexed, 0  deleted. (0 seconds)")


@pytest.mark.asyncio
async def test_connector_service_poll_sync_now(
    mock_responses, patch_logger, patch_ping, set_env
):
    await set_server_responses(mock_responses, FAKE_CONFIG_NO_SYNC)
    service = ConnectorService(CONFIG)
    # one_sync means it won't loop forever
    await service.poll(sync_now=True, one_sync=True)
    patch_logger.assert_present("Sync done: 1 indexed, 0  deleted. (0 seconds)")


@pytest.mark.asyncio
async def test_connector_service_poll_sync_fails(
    mock_responses, patch_logger, patch_ping, set_env
):

    await set_server_responses(mock_responses, FAKE_CONFIG_FAIL_SERVICE)
    service = ConnectorService(CONFIG)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.poll()
    patch_logger.assert_present("The document fetcher failed")


@pytest.mark.asyncio
async def test_connector_service_poll_unknown_service(
    mock_responses, patch_logger, patch_ping, set_env
):

    await set_server_responses(mock_responses, FAKE_CONFIG_UNKNOWN_SERVICE)
    service = ConnectorService(CONFIG)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.poll()
    patch_logger.assert_present("Can't handle source of type UNKNOWN")


@pytest.mark.asyncio
async def test_connector_service_poll_buggy_service(
    mock_responses, patch_logger, patch_ping, set_env
):

    await set_server_responses(mock_responses, FAKE_CONFIG_BUGGY_SERVICE)
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
    patch_logger.assert_present(
        ["Registered connectors:", "- Fakey", "- Phatey", "Bye"]
    )


@pytest.mark.asyncio
async def test_ping_fails(mock_responses, patch_logger, set_env):
    from connectors.byoc import BYOIndex

    async def _ping(*args):
        return False

    BYOIndex.ping = _ping

    service = ConnectorService(CONFIG)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.poll()
    patch_logger.assert_present("http://nowhere.com:9200 seem down. Bye!")


@pytest.mark.asyncio
async def test_spurious(mock_responses, patch_logger, patch_ping, set_env):
    await set_server_responses(mock_responses)

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

    patch_logger.assert_check(
        lambda log: isinstance(log, Exception) and log.args[0] == "me"
    )


@pytest.mark.asyncio
async def test_spurious_continue(mock_responses, patch_logger, patch_ping, set_env):
    await set_server_responses(mock_responses)

    from connectors.byoc import BYOConnector

    async def _sync(*args):
        raise Exception("me")

    old_sync = BYOConnector.sync
    BYOConnector.sync = _sync

    await set_server_responses(mock_responses)
    headers = {"X-Elastic-Product": "Elasticsearch"}

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={"hits": {"hits": [{"_id": "1", "_source": FAKE_CONFIG}]}},
        headers=headers,
    )

    try:
        service = ConnectorService(CONFIG_KEEP_ALIVE)
        asyncio.get_event_loop().call_soon(service.stop)
        await service.poll()
    except Exception:
        pass
    finally:
        BYOConnector.sync = old_sync

    patch_logger.assert_instance(Exception)
