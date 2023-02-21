#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import copy
import json
import os
from unittest import mock
from unittest.mock import AsyncMock

import pytest
from aioresponses import CallbackResult

from connectors.byoc import DataSourceError, JobStatus
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
    "features": {
        "sync_rules": {"advanced": {"enabled": True}, "basic": {"enabled": True}}
    },
    "index_name": "search-airbnb",
    "service_type": "fake",
    "status": "configured",
    "language": "en",
    "last_sync_status": None,
    "last_sync_error": None,
    "last_synced": None,
    "last_seen": None,
    "created_at": None,
    "updated_at": None,
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
FAKE_CONFIG_LAST_JOB_SUSPENDED = copy.deepcopy(FAKE_CONFIG)
FAKE_CONFIG_LAST_JOB_SUSPENDED["sync_now"] = False
FAKE_CONFIG_LAST_JOB_SUSPENDED["last_sync_status"] = JobStatus.SUSPENDED.value
FAKE_CONFIG_LAST_JOB_SUSPENDED_SCHEDULING_DISABLED = copy.deepcopy(
    FAKE_CONFIG_LAST_JOB_SUSPENDED
)
FAKE_CONFIG_LAST_JOB_SUSPENDED_SCHEDULING_DISABLED["scheduling"]["enabled"] = False
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
    "last_sync_status": None,
    "last_sync_error": None,
    "last_synced": None,
    "last_seen": None,
    "created_at": None,
    "updated_at": None,
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

ALL_SYNC_RULES_FEATURES_DISABLED = {
    "sync_rules": {"advanced": {"enabled": False}, "basic": {"enabled": False}},
    "filtering_advanced_config": False,
    "filtering_rules": False,
}

NO_FEATURES_PRESENT = {}

FAKE_CONFIG_FAIL_SERVICE = {
    "api_key_id": "",
    "configuration": {"fail": {"value": True, "label": ""}},
    "index_name": "search-airbnb",
    "service_type": "fake",
    "status": "configured",
    "language": "en",
    "last_sync_status": None,
    "last_sync_error": None,
    "last_synced": None,
    "last_seen": None,
    "created_at": None,
    "updated_at": None,
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
    "last_sync_status": None,
    "last_sync_error": None,
    "last_synced": None,
    "last_seen": None,
    "created_at": None,
    "updated_at": None,
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
    "last_sync_status": None,
    "last_sync_error": None,
    "last_synced": None,
    "last_seen": None,
    "created_at": None,
    "updated_at": None,
    "scheduling": {"enabled": True, "interval": "0 * * * *"},
    "sync_now": True,
}

JOB_DOC_SOURCE = {
    "_id": "1",
    "_source": {
        "status": "completed",
        "connector": {
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
        },
    },
}


@pytest.fixture(autouse=True)
def patch_validate_filtering_in_sync():
    with mock.patch(
        "connectors.byoc.Connector.validate_filtering", return_value=AsyncMock()
    ) as validate_filtering_mock:
        yield validate_filtering_mock


def create_service(config_file):
    config = load_config(config_file)
    service = SyncService(config)
    service.idling = 0

    return service


async def run_service_with_stop_after(service, stop_after=0):
    def _stop_running_service_without_cancelling():
        service.running = False

    async def _terminate():
        if stop_after == 0:
            # so we actually want the service
            # to run current loop without interruption
            asyncio.get_event_loop().call_soon(_stop_running_service_without_cancelling)
        else:
            # but if stop_after is provided we want to
            # interrupt the service after the timeout
            await asyncio.sleep(stop_after)
            service.stop()

        await asyncio.sleep(0)

    await asyncio.gather(service.run(), _terminate())


async def create_and_run_service(config_file, stop_after=0):
    service = create_service(config_file)
    await run_service_with_stop_after(service, stop_after)


async def set_server_responses(
    mock_responses,
    configs=None,
    connectors_read=None,
    connectors_update=None,
    host="http://nowhere.com:9200",
    jobs_update=None,
    bulk_call=None,
):
    if configs is None:
        configs = [FAKE_CONFIG]
    headers = {"X-Elastic-Product": "Elasticsearch"}

    mock_responses.get(
        f"{host}/_ingest/pipeline/ent-search-generic-ingestion",
        headers=headers,
        repeat=True,
    )
    mock_responses.post(
        f"{host}/.elastic-connectors/_refresh", headers=headers, repeat=True
    )
    mock_responses.post(
        f"{host}/.elastic-connectors-sync-jobs/_refresh", headers=headers, repeat=True
    )
    mock_responses.post(
        f"{host}/search-airbnb/_refresh?ignore_unavailable=true",
        headers=headers,
        repeat=True,
    )
    mock_responses.post(
        f"{host}/search-airbnb/_count?ignore_unavailable=true",
        payload={"count": 100},
        headers=headers,
        repeat=True,
    )

    hits = []
    for index, config in enumerate(configs):
        hits.append({"_id": str(index + 1), "_source": config})

    def _connectors_read(url, **kw):
        nonlocal hits

        return CallbackResult(
            status=200,
            payload={
                "hits": {
                    "hits": hits,
                    "total": {"value": len(hits)},
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
    for id_ in range(len(configs)):
        mock_responses.post(
            f"{host}/.elastic-connectors-sync-jobs/_doc",
            payload={"_id": f"{id_+1}"},
            headers=headers,
        )
        mock_responses.post(
            f"{host}/.elastic-connectors-sync-jobs/_update/{id_+1}",
            headers=headers,
            callback=jobs_update,
            repeat=True,
        )
        mock_responses.put(
            f"{host}/.elastic-connectors-sync-jobs/_doc/{id_+1}",
            payload={"_id": f"{id_+1}"},
            callback=jobs_update,
            headers=headers,
        )
        mock_responses.get(
            f"{host}/.elastic-connectors-sync-jobs/_doc/{id_+1}",
            payload=JOB_DOC_SOURCE | {"_id": f"{id_+1}"},
            headers=headers,
            repeat=True,
        )

    mock_responses.get(
        f"{host}/.elastic-connectors-sync-jobs/_doc/1",
        payload=JOB_DOC_SOURCE,
        headers=headers,
        repeat=True,
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

    for id_ in range(len(configs)):
        mock_responses.post(
            f"{host}/.elastic-connectors/_update/{id_ + 1}",
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
        payload={"hits": {"hits": hits}},
        headers=headers,
        repeat=True,
    )
    mock_responses.get(
        f"{host}/search-airbnb/_search?scroll=5m",
        payload={"hits": {"hits": hits}},
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
    await create_and_run_service(CONFIG_FILE)
    patch_logger.assert_present("Sync done: 1 indexed, 0  deleted. (0 seconds)")
    # we want to make sure we DON'T get memory usage report
    patch_logger.assert_not_present("===> Largest memory usage:")


@pytest.mark.asyncio
async def test_connector_service_poll_unconfigured(
    mock_responses, patch_logger, set_env
):
    # we should not sync a connector that is not configured
    # but still send out a heartbeat

    await set_server_responses(mock_responses, [FAKE_CONFIG_NEEDS_CONFIG])
    await create_and_run_service(CONFIG_FILE)

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
        mock_responses, [FAKE_CONFIG_NO_SYNC], connectors_update=upd
    )
    await create_and_run_service(CONFIG_FILE)

    patch_logger.assert_present("*** Connector 1 HEARTBEAT")
    patch_logger.assert_present("Scheduling is disabled")
    patch_logger.assert_not_present("Sync done")


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
        mock_responses, [FAKE_CONFIG_CRON_BROKEN], connectors_update=upd
    )
    await create_and_run_service(CONFIG_FILE)
    patch_logger.assert_not_present("Sync done")
    assert calls[-1]["status"] == "error"


@pytest.mark.asyncio
async def test_connector_service_poll_suspended_restarts_sync(
    mock_responses, patch_logger, set_env
):
    await set_server_responses(mock_responses, [FAKE_CONFIG_LAST_JOB_SUSPENDED])
    await create_and_run_service(CONFIG_FILE)
    patch_logger.assert_present("Restarting sync after suspension")


@pytest.mark.asyncio
async def test_connector_service_poll_suspended_does_not_restart_when_scheduling_disabled(
    mock_responses, patch_logger, set_env
):
    await set_server_responses(
        mock_responses, [FAKE_CONFIG_LAST_JOB_SUSPENDED_SCHEDULING_DISABLED]
    )
    await create_and_run_service(CONFIG_FILE)
    patch_logger.assert_present("Scheduling is disabled")


@pytest.mark.asyncio
async def test_connector_service_poll_just_created(
    mock_responses, patch_logger, set_env
):
    # we should not sync a connector that is not configured
    # but still send out a heartbeat
    await set_server_responses(mock_responses, [FAKE_CONFIG_CREATED])
    await create_and_run_service(CONFIG_FILE)

    patch_logger.assert_present("*** Connector 1 HEARTBEAT")
    patch_logger.assert_present("Can't sync with status `created`")
    patch_logger.assert_not_present("Sync done")


@pytest.mark.asyncio
async def test_connector_service_poll_https(mock_responses, patch_logger, set_env):
    await set_server_responses(mock_responses, host="https://safenowhere.com:443")
    await create_and_run_service(CONFIG_HTTPS_FILE)
    patch_logger.assert_present("Sync done: 1 indexed, 0  deleted. (0 seconds)")


@pytest.mark.asyncio
async def test_connector_service_poll_large(mock_responses, patch_logger, set_env):
    await set_server_responses(mock_responses, [LARGE_FAKE_CONFIG])
    await create_and_run_service(MEM_CONFIG_FILE)

    # let's make sure we are seeing bulk batches of various sizes
    assert_re(".*Sending a batch.*", patch_logger.logs)
    assert_re(".*0.48MiB", patch_logger.logs)
    assert_re(".*0.17MiB", patch_logger.logs)
    assert_re(".*Sync done: 1001 indexed, 0  deleted", patch_logger.logs)


@pytest.mark.asyncio
async def test_connector_service_poll_suspended_suspends_job(
    mock_responses, patch_logger, set_env
):
    # Service is having a large payload, but we terminate it ASAP
    # This way it should suspend existing running jobs
    await set_server_responses(mock_responses, [LARGE_FAKE_CONFIG])
    await create_and_run_service(MEM_CONFIG_FILE, stop_after=0.1)

    # For now just let's make sure that message is displayed
    # that the running job was suspended
    assert_re(".*suspended.*Job id: 1", patch_logger.logs)


@pytest.mark.asyncio
async def test_connector_service_poll_sync_ts(mock_responses, patch_logger, set_env):
    indexed = []

    def bulk_call(url, **kw):
        queries = [json.loads(call.strip()) for call in kw["data"].split(b"\n") if call]
        indexed.append(queries[1])
        return CallbackResult(status=200, payload={"items": []})

    await set_server_responses(mock_responses, [FAKE_CONFIG_TS], bulk_call=bulk_call)
    await create_and_run_service(CONFIG_FILE)
    patch_logger.assert_present("Sync done: 1 indexed, 0  deleted. (0 seconds)")

    # make sure we kept the original ts
    assert indexed[0]["_timestamp"] == FakeSourceTS.ts


@pytest.mark.asyncio
async def test_connector_service_poll_sync_fails(mock_responses, patch_logger, set_env):
    await set_server_responses(mock_responses, [FAKE_CONFIG_FAIL_SERVICE])
    await create_and_run_service(CONFIG_FILE)
    patch_logger.assert_present("The document fetcher failed")


@pytest.mark.asyncio
async def test_connector_service_poll_unknown_service(
    mock_responses, patch_logger, set_env
):
    await set_server_responses(mock_responses, [FAKE_CONFIG_UNKNOWN_SERVICE])
    await create_and_run_service(CONFIG_FILE)


@pytest.mark.parametrize(
    "config, should_raise_filtering_error",
    [
        (FAIL_FILTERING_INVALID_CONFIG, True),
        (FAIL_FILTERING_EDITED_CONFIG, True),
        (FAIL_FILTERING_ERRORS_PRESENT_CONFIG, True),
        (FAKE_FILTERING_VALID_CONFIG, False),
        (FAIL_FILTERING_INVALID_CONFIG | ALL_SYNC_RULES_FEATURES_DISABLED, False),
        (FAIL_FILTERING_EDITED_CONFIG | ALL_SYNC_RULES_FEATURES_DISABLED, False),
        (
            FAIL_FILTERING_ERRORS_PRESENT_CONFIG | ALL_SYNC_RULES_FEATURES_DISABLED,
            False,
        ),
        (FAKE_FILTERING_VALID_CONFIG | ALL_SYNC_RULES_FEATURES_DISABLED, False),
        (FAIL_FILTERING_INVALID_CONFIG | NO_FEATURES_PRESENT, False),
        (FAIL_FILTERING_EDITED_CONFIG | NO_FEATURES_PRESENT, False),
        (FAIL_FILTERING_ERRORS_PRESENT_CONFIG | NO_FEATURES_PRESENT, False),
        (FAKE_FILTERING_VALID_CONFIG | NO_FEATURES_PRESENT, False),
    ],
)
@pytest.mark.asyncio
async def test_connector_service_filtering(
    config,
    should_raise_filtering_error,
    mock_responses,
    patch_logger,
    set_env,
    patch_validate_filtering_in_sync,
):
    await set_server_responses(mock_responses, [config])

    patch_validate_filtering_in_sync.side_effect = (
        [InvalidFilteringError] if should_raise_filtering_error else None
    )

    if should_raise_filtering_error:
        await create_and_run_service(CONFIG_FILE)
        patch_logger.assert_check(lambda log: isinstance(log, InvalidFilteringError))
    else:
        try:
            await create_and_run_service(CONFIG_FILE)
        except Exception as e:
            # mark test as failed
            raise AssertionError(f"Unexpected exception of type {type(e)} raised.")


@pytest.mark.asyncio
async def test_connector_service_poll_buggy_service(
    mock_responses, patch_logger, set_env
):
    def connectors_update(url, **kw):
        doc = json.loads(kw["data"])["doc"]
        if "error" in doc:  # one time there will be update with no error
            assert (
                doc["error"]
                == "Could not instantiate <class 'fake_sources.FakeSource'> for fake"
            )
        return CallbackResult(status=200)

    await set_server_responses(
        mock_responses, [FAKE_CONFIG_BUGGY_SERVICE], connectors_update=connectors_update
    )

    await create_and_run_service(CONFIG_FILE)

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
        await create_and_run_service(CONFIG_FILE)
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
        await create_and_run_service(CONFIG_FILE)
    except Exception:
        await asyncio.sleep(0.1)
    finally:
        Connector.sync = old_sync

    patch_logger.assert_instance(Exception)


@pytest.mark.asyncio
async def test_concurrent_syncs(mock_responses, patch_logger, set_env):
    await set_server_responses(mock_responses, [FAKE_CONFIG, FAKE_CONFIG, FAKE_CONFIG])
    await create_and_run_service(CONFIG_FILE)

    # make sure we synced the three connectors
    patch_logger.assert_present("[1] Sync done: 1 indexed, 0  deleted. (0 seconds)")
    patch_logger.assert_present("[2] Sync done: 1 indexed, 0  deleted. (0 seconds)")
    patch_logger.assert_present("[3] Sync done: 1 indexed, 0  deleted. (0 seconds)")
