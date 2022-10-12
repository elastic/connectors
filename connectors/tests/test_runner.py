#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import copy
import os
import pytest
import asyncio
import json
from unittest import mock
from functools import partial
from aioresponses import CallbackResult

from connectors.runner import ConnectorService, run
from connectors.source import DataSourceError
from connectors.conftest import assert_re


CONFIG = os.path.join(os.path.dirname(__file__), "config.yml")
ES_CONFIG = os.path.join(os.path.dirname(__file__), "entsearch.yml")
CONFIG_2 = os.path.join(os.path.dirname(__file__), "config_2.yml")
CONFIG_HTTPS = os.path.join(os.path.dirname(__file__), "config_https.yml")


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
    "pipeline": {
        "extract_binary_content": True,
        "reduce_whitespace": True,
        "run_ml_inference": True,
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

FAKE_CONFIG_PIPELINE_CHANGED = copy.deepcopy(FAKE_CONFIG)
FAKE_CONFIG_PIPELINE_CHANGED["pipeline"] = {
    "extract_binary_content": False,
    "reduce_whitespace": False,
    "run_ml_inference": False,
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

LARGE_FAKE_CONFIG = copy.deepcopy(FAKE_CONFIG)
LARGE_FAKE_CONFIG["service_type"] = "large_fake"
FAIL_ONCE_CONFIG = dict(FAKE_CONFIG)
FAIL_ONCE_CONFIG["service_type"] = "fail_once"


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


class FailsThenWork(FakeSource):
    """Buggy"""

    service_type = "fail_once"
    fail = True

    async def get_docs(self):
        if FailsThenWork.fail:
            FailsThenWork.fail = False
            raise Exception("I fail while syncing")
        yield {"_id": 1}, partial(self._dl, 1)


class LargeFakeSource(FakeSource):
    """Phatey"""

    service_type = "large_fake"

    async def get_docs(self):
        for i in range(10001):
            doc_id = str(i + 1)
            yield {"_id": doc_id}, partial(self._dl, doc_id)


async def set_server_responses(
    mock_responses,
    config=FAKE_CONFIG,
    connectors_read=None,
    connectors_update=None,
    host="http://nowhere.com:9200",
    jobs_update=None,
    bulk_call=None,
):
    headers = {"X-Elastic-Product": "Elasticsearch"}

    mock_responses.head(f"{host}/.elastic-connectors", headers=headers, repeat=True)
    mock_responses.head(
        f"{host}/.elastic-connectors-sync-jobs", headers=headers, repeat=True
    )

    mock_responses.get(
        f"{host}/_ingest/pipeline/ent-search-generic-ingestion",
        headers=headers,
        repeat=True,
    )
    mock_responses.post(
        f"{host}/.elastic-connectors/_refresh", headers=headers, repeat=True
    )

    def _connectors_read(url, **kw):
        return CallbackResult(
            status=200, payload={"hits": {"hits": [{"_id": "1", "_source": config}]}}
        )

    if connectors_read is None:
        connectors_read = _connectors_read

    mock_responses.post(
        f"{host}/.elastic-connectors/_search?expand_wildcards=hidden",
        headers=headers,
        callback=connectors_read,
        repeat=True,
    )
    mock_responses.post(
        f"{host}/.elastic-connectors-sync-jobs/_doc",
        payload={"_id": "1"},
        headers=headers,
        repeat=True,
    )

    mock_responses.post(
        f"{host}/.elastic-connectors-sync-jobs/_update/1",
        headers=headers,
        callback=jobs_update,
        repeat=True,
    )
    mock_responses.put(
        f"{host}/.elastic-connectors-sync-jobs/_doc/1",
        payload={"_id": "1"},
        callback=jobs_update,
        headers=headers,
    )

    mock_responses.put(
        f"{host}/.elastic-connectors/_doc/1",
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

    if connectors_update is None:
        connectors_update = update_connector

    mock_responses.put(
        f"{host}/.elastic-connectors/_doc/1",
        callback=connectors_update,
        headers=headers,
    )
    mock_responses.post(
        f"{host}/.elastic-connectors/_update/1",
        headers=headers,
        callback=connectors_update,
        repeat=True,
    )
    mock_responses.head(
        f"{host}/search-airbnb?expand_wildcards=open",
        headers=headers,
        repeat=True,
    )
    mock_responses.get(
        f"{host}/search-airbnb/_mapping?expand_wildcards=open",
        payload={"search-airbnb": {"mappings": {}}},
        headers=headers,
        repeat=True,
    )
    mock_responses.put(
        f"{host}/search-airbnb/_mapping?expand_wildcards=open",
        headers=headers,
        repeat=True,
    )
    mock_responses.get(
        f"{host}/search-airbnb",
        payload={"hits": {"hits": [{"_id": "1", "_source": config}]}},
        headers=headers,
        repeat=True,
    )
    mock_responses.get(
        f"{host}/search-airbnb/_search?scroll=5m",
        payload={"hits": {"hits": [{"_id": "1", "_source": config}]}},
        headers=headers,
        repeat=True,
    )
    mock_responses.post(
        f"{host}/search-airbnb/_search?scroll=5m",
        payload={"_id": "1"},
        headers=headers,
        repeat=True,
    )
    mock_responses.put(
        f"{host}/search-airbnb/_search?scroll=5m",
        payload={"_id": "1"},
        headers=headers,
        repeat=True,
    )

    def _bulk_call(url, **kw):
        return CallbackResult(status=200, payload={"items": []})

    if bulk_call is None:
        bulk_call = _bulk_call

    mock_responses.put(
        f"{host}/_bulk?pipeline=ent-search-generic-ingestion",
        callback=bulk_call,
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
async def test_connector_service_poll_https(
    mock_responses, patch_logger, patch_ping, set_env
):
    await set_server_responses(mock_responses, host="https://safenowhere.com:443")
    service = ConnectorService(CONFIG_HTTPS)
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
async def test_connector_service_poll_clear_error(
    mock_responses, patch_logger, patch_ping, set_env
):
    service = ConnectorService(CONFIG)
    calls = []

    def upd(url, **kw):
        doc = json.loads(kw["data"])["doc"]
        calls.append("connector:" + str(doc.get("error", "NOT THERE")))

    def jobs_update(url, **kw):
        doc = json.loads(kw["data"])["doc"]
        calls.append("job:" + str(doc.get("error", "NOT THERE")))

    await set_server_responses(
        mock_responses, FAIL_ONCE_CONFIG, connectors_update=upd, jobs_update=jobs_update
    )
    await service.poll(one_sync=True)  # fails
    await service.poll(one_sync=True)  # works
    assert calls == [
        "connector:NOT THERE",  # from is_ready()
        "job:I fail while syncing",  # first sync
        "connector:I fail while syncing",  # first sync
        "connector:NOT THERE",  # heartbeat
        "job:None",  # second sync
        "connector:None",  # second sync
    ]


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
    mock_responses, patch_logger, patch_ping, set_env, catch_stdout
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
    def connectors_update(url, **kw):
        doc = json.loads(kw["data"])["doc"]
        assert doc["error"] == "Could not instantiate test_runner:FakeSource for fake"
        return CallbackResult(status=200)

    await set_server_responses(
        mock_responses, FAKE_CONFIG_BUGGY_SERVICE, connectors_update=connectors_update
    )
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
        service = ConnectorService(CONFIG)
        asyncio.get_event_loop().call_soon(service.stop)
        await service.poll()
    except Exception:
        pass
    finally:
        BYOConnector.sync = old_sync

    patch_logger.assert_instance(Exception)


@pytest.mark.asyncio
async def test_connector_settings_change(
    mock_responses, patch_logger, patch_ping, set_env
):
    service = ConnectorService(CONFIG)

    configs = [FAKE_CONFIG, FAKE_CONFIG_PIPELINE_CHANGED]
    current = [-1]

    # we want to simulate a settings change between two calls
    # to make sure the pipeline settings get updated
    def connectors_read(url, **kw):
        current[0] += 1
        source = configs[current[0]]
        return CallbackResult(
            status=200, payload={"hits": {"hits": [{"_id": "1", "_source": source}]}}
        )

    indexed = []

    def bulk_call(url, **kw):
        queries = [json.loads(call.strip()) for call in kw["data"].split(b"\n") if call]
        indexed.append(queries[1])
        return CallbackResult(status=200, payload={"items": []})

    await set_server_responses(
        mock_responses,
        FAKE_CONFIG,
        connectors_read=connectors_read,
        bulk_call=bulk_call,
    )

    # two polls, the second one gets a different pipeline
    await service.poll(one_sync=True)
    await service.poll(one_sync=True)

    service.stop()

    # the first doc and second doc don't get the same pipeline
    assert indexed[0]["_extract_binary_content"]
    assert indexed[0]["_reduce_whitespace"]
    assert indexed[0]["_run_ml_inference"]
    assert not indexed[1]["_extract_binary_content"]
    assert not indexed[1]["_reduce_whitespace"]
    assert not indexed[1]["_run_ml_inference"]
