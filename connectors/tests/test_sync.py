#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import copy
import json
import os

import pytest
from aioresponses import CallbackResult

from connectors.byoc import DataSourceError
from connectors.config import load_config
from connectors.conftest import assert_re
from connectors.filtering.validation import InvalidFilteringError
from connectors.services.sync import SyncService
from connectors.tests.fake_sources import FakeSourceTS

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.yml")
CONFIG_FILE_2 = os.path.join(os.path.dirname(__file__), "config_2.yml")
CONFIG_HTTPS_FILE = os.path.join(os.path.dirname(__file__), "config_https.yml")
CONFIG_MEM_FILE = os.path.join(os.path.dirname(__file__), "config_mem.yml")
MEM_CONFIG_FILE = os.path.join(os.path.dirname(__file__), "memconfig.yml")


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
    "language": "en",
    "last_sync_status": "null",
    "last_sync_error": "",
    "last_synced": "",
    "last_seen": "",
    "created_at": "",
    "updated_at": "",
    "scheduling": {"enabled": True, "interval": "0 * * * * *"},
    "sync_now": True,
    "is_native": True,
}
FAKE_CONFIG_CRON_BROKEN = copy.deepcopy(FAKE_CONFIG)
FAKE_CONFIG_CRON_BROKEN["sync_now"] = False
FAKE_CONFIG_CRON_BROKEN["scheduling"]["interval"] = "0 * * * *"
FAKE_CONFIG_CREATED = copy.deepcopy(FAKE_CONFIG)
FAKE_CONFIG_CREATED["status"] = "created"
FAKE_CONFIG_NEEDS_CONFIG = copy.deepcopy(FAKE_CONFIG)
FAKE_CONFIG_NEEDS_CONFIG["status"] = "needs_configuration"
FAKE_CONFIG_NO_SYNC = copy.deepcopy(FAKE_CONFIG)
FAKE_CONFIG_NO_SYNC["sync_now"] = False
FAKE_CONFIG_NO_SYNC["scheduling"]["enabled"] = False
FAKE_CONFIG_PIPELINE_CHANGED = copy.deepcopy(FAKE_CONFIG)
FAKE_CONFIG_PIPELINE_CHANGED["pipeline"] = {
    "extract_binary_content": False,
    "reduce_whitespace": False,
    "run_ml_inference": False,
}
FAKE_CONFIG_TS = copy.deepcopy(FAKE_CONFIG)
FAKE_CONFIG_TS["service_type"] = "fake_ts"


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
    "language": "en",
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

FAKE_FILTERING_VALID_CONFIG = copy.deepcopy(FAKE_CONFIG)
FAKE_FILTERING_VALID_CONFIG["service_type"] = "filtering_state_valid"

FAIL_FILTERING_INVALID_CONFIG = copy.deepcopy(FAKE_CONFIG)
FAIL_FILTERING_INVALID_CONFIG["service_type"] = "filtering_state_invalid"

FAIL_FILTERING_EDITED_CONFIG = copy.deepcopy(FAKE_CONFIG)
FAIL_FILTERING_EDITED_CONFIG["service_type"] = "filtering_state_edited"

FAIL_FILTERING_ERRORS_PRESENT_CONFIG = copy.deepcopy(FAKE_CONFIG)
FAIL_FILTERING_ERRORS_PRESENT_CONFIG["service_type"] = "filtering_errors_present"

FAKE_CONFIG_FAIL_SERVICE = {
    "api_key_id": "",
    "configuration": {"fail": {"value": True, "label": ""}},
    "index_name": "search-airbnb",
    "service_type": "fake",
    "status": "configured",
    "language": "en",
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
    "language": "en",
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
    "language": "en",
    "last_sync_status": "null",
    "last_sync_error": "",
    "last_synced": "",
    "last_seen": "",
    "created_at": "",
    "updated_at": "",
    "scheduling": {"enabled": True, "interval": "0 * * * *"},
    "sync_now": True,
}


class Args:
    def __init__(self, **options):
        self.one_sync = options.get("one_sync", False)
        self.sync_now = options.get("sync_now", False)


def create_service(config_file, **options):
    config = load_config(config_file)
    return SyncService(config, Args(**options))


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
            status=200,
            payload={
                "hits": {
                    "hits": [{"_id": "1", "_source": config}],
                    "total": {"value": 1},
                }
            },
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
            "api_key_id",
            "pipeline",
            "scheduling",
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
async def test_connector_service_poll(mock_responses, patch_logger, set_env):
    await set_server_responses(mock_responses)
    service = create_service(CONFIG_FILE)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.run()
    patch_logger.assert_present("Sync done: 1 indexed, 0  deleted. (0 seconds)")
    # we want to make sure we DON'T get memory usage report
    patch_logger.assert_not_present("===> Largest memory usage:")


@pytest.mark.asyncio
async def test_connector_service_poll_unconfigured(
    mock_responses, patch_logger, set_env
):
    # we should not sync a connector that is not configured
    # but still send out a heartbeat

    await set_server_responses(mock_responses, FAKE_CONFIG_NEEDS_CONFIG)
    service = create_service(CONFIG_FILE)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.run()

    patch_logger.assert_present("*** Connector 1 HEARTBEAT")
    patch_logger.assert_present("Can't sync with status `needs_configuration`")
    patch_logger.assert_not_present("Sync done")


@pytest.mark.asyncio
async def test_connector_service_poll_no_sync_but_status_updated(
    mock_responses, patch_logger, set_env
):
    calls = []

    def upd(url, **kw):
        doc = json.loads(kw["data"])["doc"]
        calls.append(doc)

    # if a connector is correctly configured but we don't sync (not scheduled)
    # we still want to tell kibana we are connected
    await set_server_responses(
        mock_responses, FAKE_CONFIG_NO_SYNC, connectors_update=upd
    )
    service = create_service(CONFIG_FILE, sync_now=False)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.run()

    patch_logger.assert_present("*** Connector 1 HEARTBEAT")
    patch_logger.assert_present("Scheduling is disabled")
    patch_logger.assert_not_present("Sync done")
    assert calls[-1]["status"] == "connected"


@pytest.mark.asyncio
async def test_connector_service_poll_cron_broken(
    mock_responses, patch_logger, set_env
):
    calls = []

    def upd(url, **kw):
        doc = json.loads(kw["data"])["doc"]
        calls.append(doc)

    # if a connector is correctly configured but we don't sync because the cron
    # is broken
    # we still want to tell kibana we are connected
    await set_server_responses(
        mock_responses, FAKE_CONFIG_CRON_BROKEN, connectors_update=upd
    )
    service = create_service(CONFIG_FILE, sync_now=False)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.run()
    patch_logger.assert_not_present("Sync done")
    assert calls[0]["status"] == "error"


@pytest.mark.asyncio
async def test_connector_service_poll_just_created(
    mock_responses, patch_logger, set_env
):
    # we should not sync a connector that is not configured
    # but still send out an heartbeat
    await set_server_responses(mock_responses, FAKE_CONFIG_CREATED)
    service = create_service(CONFIG_FILE)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.run()

    patch_logger.assert_present("*** Connector 1 HEARTBEAT")
    patch_logger.assert_present("Can't sync with status `created`")
    patch_logger.assert_not_present("Sync done")


@pytest.mark.asyncio
async def test_connector_service_poll_https(mock_responses, patch_logger, set_env):
    await set_server_responses(mock_responses, host="https://safenowhere.com:443")
    service = create_service(CONFIG_HTTPS_FILE)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.run()
    patch_logger.assert_present("Sync done: 1 indexed, 0  deleted. (0 seconds)")


@pytest.mark.asyncio
async def test_connector_service_poll_large(mock_responses, patch_logger, set_env):
    await set_server_responses(mock_responses, LARGE_FAKE_CONFIG)
    service = create_service(MEM_CONFIG_FILE)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.run()

    # let's make sure we are seeing bulk batches of various sizes
    assert_re(".*Sending a batch.*", patch_logger.logs)
    assert_re(".*0.48MiB", patch_logger.logs)
    assert_re(".*0.17MiB", patch_logger.logs)
    assert_re("Sync done: 1001 indexed, 0  deleted", patch_logger.logs)


@pytest.mark.asyncio
async def test_connector_service_poll_clear_error(
    mock_responses, patch_logger, set_env
):
    service = create_service(CONFIG_FILE, one_sync=True)
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
    await service.run()  # fails
    await service.run()  # works

    assert calls == [
        "connector:NOT THERE",  # from is_ready()
        "job:I fail while syncing",  # first sync
        "connector:I fail while syncing",  # first sync
        "connector:NOT THERE",  # heartbeat
        "job:None",  # second sync
        "connector:None",  # second sync
    ]


@pytest.mark.asyncio
async def test_connector_service_poll_sync_now(mock_responses, patch_logger, set_env):
    await set_server_responses(mock_responses, FAKE_CONFIG_NO_SYNC)
    service = create_service(CONFIG_FILE, sync_now=True, one_sync=True)
    # one_sync means it won't loop forever
    await service.run()
    patch_logger.assert_present("Sync done: 1 indexed, 0  deleted. (0 seconds)")


@pytest.mark.asyncio
async def test_connector_service_poll_sync_ts(mock_responses, patch_logger, set_env):
    indexed = []

    def bulk_call(url, **kw):
        queries = [json.loads(call.strip()) for call in kw["data"].split(b"\n") if call]
        indexed.append(queries[1])
        return CallbackResult(status=200, payload={"items": []})

    await set_server_responses(mock_responses, FAKE_CONFIG_TS, bulk_call=bulk_call)
    service = create_service(CONFIG_FILE, sync_now=True, one_sync=True)
    await service.run()
    patch_logger.assert_present("Sync done: 1 indexed, 0  deleted. (0 seconds)")

    # make sure we kept the original ts
    assert indexed[0]["_timestamp"] == FakeSourceTS.ts


@pytest.mark.asyncio
async def test_connector_service_poll_sync_fails(mock_responses, patch_logger, set_env):
    await set_server_responses(mock_responses, FAKE_CONFIG_FAIL_SERVICE)
    service = create_service(CONFIG_FILE)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.run()
    patch_logger.assert_present("The document fetcher failed")


@pytest.mark.asyncio
async def test_connector_service_poll_unknown_service(
    mock_responses, patch_logger, set_env
):
    await set_server_responses(mock_responses, FAKE_CONFIG_UNKNOWN_SERVICE)
    service = create_service(CONFIG_FILE)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.run()


async def service_with_max_errors(mock_responses, config, max_errors):
    await set_server_responses(mock_responses, config)
    service = create_service(CONFIG_FILE)
    service.service_config["max_errors"] = max_errors
    asyncio.get_event_loop().call_soon(service.stop)

    return service


@pytest.mark.parametrize(
    "config, should_raise_filtering_error",
    [
        (FAIL_FILTERING_INVALID_CONFIG, True),
        (FAIL_FILTERING_EDITED_CONFIG, True),
        (FAIL_FILTERING_ERRORS_PRESENT_CONFIG, True),
        (FAKE_FILTERING_VALID_CONFIG, False),
    ],
)
@pytest.mark.asyncio
async def test_connector_service_filtering(
    config,
    should_raise_filtering_error,
    mock_responses,
    patch_logger,
    set_env,
):
    service = await service_with_max_errors(mock_responses, config, 0)

    if should_raise_filtering_error:
        with pytest.raises(InvalidFilteringError):
            await service.run()
    else:
        try:
            await service.run()
        except Exception as e:
            # mark test as failed
            assert False, f"Unexpected exception of type {type(e)} raised."


@pytest.mark.asyncio
async def test_connector_service_poll_buggy_service(
    mock_responses, patch_logger, set_env
):
    def connectors_update(url, **kw):
        doc = json.loads(kw["data"])["doc"]
        assert (
            doc["error"]
            == "Could not instantiate <class 'fake_sources.FakeSource'> for fake"
        )
        return CallbackResult(status=200)

    await set_server_responses(
        mock_responses, FAKE_CONFIG_BUGGY_SERVICE, connectors_update=connectors_update
    )
    service = create_service(CONFIG_FILE)
    asyncio.get_event_loop().call_soon(service.stop)
    await service.run()

    for log in patch_logger.logs:
        if isinstance(log, DataSourceError):
            return

    raise AssertionError


@pytest.mark.asyncio
async def test_spurious(mock_responses, patch_logger, set_env):
    await set_server_responses(mock_responses)

    from connectors.byoc import Connector

    async def _sync(*args):
        raise Exception("me")

    old_sync = Connector.sync
    Connector.sync = _sync

    try:
        service = create_service(CONFIG_FILE)
        service.idling = 0
        service.service_config["max_errors"] = 0
        await service.run()
    except Exception:
        await asyncio.sleep(0.1)
    finally:
        Connector.sync = old_sync

    patch_logger.assert_check(
        lambda log: isinstance(log, Exception) and log.args[0] == "me"
    )


@pytest.mark.asyncio
async def test_spurious_continue(mock_responses, patch_logger, set_env):
    await set_server_responses(mock_responses)

    from connectors.byoc import Connector

    async def _sync(*args):
        raise Exception("me")

    old_sync = Connector.sync
    Connector.sync = _sync

    await set_server_responses(mock_responses)
    headers = {"X-Elastic-Product": "Elasticsearch"}

    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_search?expand_wildcards=hidden",
        payload={
            "hits": {
                "hits": [{"_id": "1", "_source": FAKE_CONFIG}],
                "total": {"value": 1},
            }
        },
        headers=headers,
    )

    try:
        service = create_service(CONFIG_FILE)
        asyncio.get_event_loop().call_soon(service.stop)
        await service.run()
    except Exception:
        await asyncio.sleep(0.1)
    finally:
        Connector.sync = old_sync

    patch_logger.assert_instance(Exception)


@pytest.mark.asyncio
async def test_connector_settings_change(mock_responses, patch_logger, set_env):
    service = create_service(CONFIG_FILE, one_sync=True)

    configs = [FAKE_CONFIG, FAKE_CONFIG_PIPELINE_CHANGED]
    current = [-1]

    # we want to simulate a settings change between two calls
    # to make sure the pipeline settings get updated
    def connectors_read(url, **kw):
        current[0] += 1
        source = configs[current[0]]
        return CallbackResult(
            status=200,
            payload={
                "hits": {
                    "hits": [{"_id": "1", "_source": source}],
                    "total": {"value": 1},
                }
            },
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
    await service.run()
    await service.run()

    service.stop()

    # the first doc and second doc don't get the same pipeline
    assert indexed[0]["_extract_binary_content"]
    assert indexed[0]["_reduce_whitespace"]
    assert indexed[0]["_run_ml_inference"]
    assert not indexed[1]["_extract_binary_content"]
    assert not indexed[1]["_reduce_whitespace"]
    assert not indexed[1]["_run_ml_inference"]
