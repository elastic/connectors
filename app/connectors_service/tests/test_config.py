#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os
from unittest import mock

import pytest

from connectors.config import _nest_configs, add_defaults, load_config

HERE = os.path.dirname(__file__)
FIXTURES_DIR = os.path.abspath(os.path.join(HERE, "fixtures"))

CONFIG_FILE = os.path.join(FIXTURES_DIR, "config.yml")


def test_bad_config_file():
    with pytest.raises(FileNotFoundError):
        load_config("BEEUUUAH")


def test_config(set_env):
    config = load_config(CONFIG_FILE)
    assert isinstance(config, dict)
    assert config["elasticsearch"]["host"] == "http://nowhere.com:9200"
    assert config["elasticsearch"]["user"] == "elastic"


def test_default_max_text_document_size(set_env):
    config = load_config(CONFIG_FILE)
    assert config["elasticsearch"]["bulk"]["max_text_document_size"] == 3


def test_config_with_invalid_log_level(set_env):
    with mock.patch.dict(
        os.environ, {"ENT_SEARCH_CONFIG_PATH": ES_CONFIG_INVALID_LOG_LEVEL_FILE}
    ):
        with pytest.raises(ValueError) as e:
            _ = load_config(CONFIG_FILE)

        assert e.match("Unexpected log level.*")


def test_nest_config_when_nested_field_does_not_exist():
    config = {}

    _nest_configs(config, "test.nested.property", 50)

    assert config["test"]["nested"]["property"] == 50


def test_nest_config_when_nested_field_exists():
    config = {"test": {"nested": {"property": 25}}}

    _nest_configs(config, "test.nested.property", 50)

    assert config["test"]["nested"]["property"] == 50


def test_nest_config_when_root_field_does_not_exist():
    config = {}

    _nest_configs(config, "test", 50)

    assert config["test"] == 50


def test_nest_config_when_root_field_does_exists():
    config = {"test": 10}

    _nest_configs(config, "test", 50)

    assert config["test"] == 50


def test_bulk_queue_refresh_timeout_outlasts_worst_case_retry_budget():
    """The mem-queue refresh timeout must outlive a fully retried bulk request.

    `ESManagementClient.bulk_insert` retries through
    `TransientElasticsearchRetrier.execute_with_retry`, which uses LINEAR_BACKOFF on the
    top-level `elasticsearch.max_retries` and `elasticsearch.retry_interval`. The
    worst-case duration of a single bulk send is therefore `max_retries` request
    attempts (each capped by `request_timeout`) plus the linear backoff sleeps between
    them. If `queue_refresh_timeout` is shorter than that budget, the queue gives up on
    a request the retrier is still legitimately working on, and we lose data.
    """
    es = add_defaults({})["elasticsearch"]
    max_retries = es["max_retries"]
    retry_interval = es["retry_interval"]
    request_timeout = es["request_timeout"]
    queue_refresh_timeout = es["bulk"]["queue_refresh_timeout"]

    max_request_time = max_retries * request_timeout
    backoff_gaps = sum(retry_interval * i for i in range(1, max_retries))
    worst_case_retry_budget = max_request_time + backoff_gaps

    assert queue_refresh_timeout >= worst_case_retry_budget, (
        f"queue_refresh_timeout={queue_refresh_timeout}s is shorter than the "
        f"worst-case bulk retry budget of {worst_case_retry_budget}s"
    )


def test_bulk_chunk_sizes_are_sane():
    """Chunk size and concurrency knobs need to be in a workable range.

    Tiny chunks make bulk ingest pathologically chatty; huge chunks blow past
    Elasticsearch's bulk request limits. Memory caps need to leave room for at least one
    chunk, and concurrency must be positive for any work to happen.
    """
    bulk = add_defaults({})["elasticsearch"]["bulk"]

    assert 250 <= bulk["chunk_size"] <= 10_000
    assert 1 <= bulk["chunk_max_mem_size"] <= bulk["queue_max_mem_size"]
    assert bulk["max_concurrency"] >= 1
    assert bulk["queue_max_size"] >= bulk["max_concurrency"]


def test_retry_budgets_do_not_blow_up_into_hours():
    """No single retried operation should be allowed to stall for more than an hour.

    `TransientElasticsearchRetrier` (used by `bulk_insert`) applies LINEAR_BACKOFF on
    `elasticsearch.max_retries` and `elasticsearch.retry_interval`, so the worst-case
    bulk has the shape `attempts * request_timeout + sum(backoffs)`. The ES preflight
    wait loop has its own `max_wait_duration` cap. Setting either high enough to
    produce hour-plus stalls would silently freeze syncs; we cap the worst case at 1
    hour per operation as a sanity ceiling.
    """
    es = add_defaults({})["elasticsearch"]
    max_retries = es["max_retries"]
    retry_interval = es["retry_interval"]
    request_timeout = es["request_timeout"]
    max_wait_duration = es["max_wait_duration"]
    one_hour = 60 * 60

    max_request_time = max_retries * request_timeout
    backoff_gaps = sum(retry_interval * i for i in range(1, max_retries))
    bulk_worst_case = max_request_time + backoff_gaps

    assert (
        bulk_worst_case <= one_hour
    ), f"bulk retry worst-case {bulk_worst_case}s exceeds 1h ceiling"
    assert (
        max_wait_duration <= one_hour
    ), f"ES preflight max_wait_duration {max_wait_duration}s exceeds 1h ceiling"


def test_error_monitor_window_can_observe_threshold():
    """The sliding error window must be able to actually contain the failure threshold.

    `max_consecutive_errors` and `max_error_rate` are evaluated against an error window
    of size `error_window_size`. If the window is smaller than the consecutive threshold,
    the consecutive trigger would fire before the window is even full, defeating the
    rate-based check. The error rate must also be a valid probability.
    """
    error_monitor = add_defaults({})["elasticsearch"]["bulk"]["error_monitor"]

    assert error_monitor["error_window_size"] > error_monitor["max_consecutive_errors"]
    assert error_monitor["error_queue_size"] <= error_monitor["error_window_size"]
    assert 0 < error_monitor["max_error_rate"] <= 1
    assert error_monitor["max_total_errors"] >= error_monitor["max_consecutive_errors"]


def test_service_lifecycle_intervals_are_consistent():
    """Polling / heartbeat / cleanup intervals must respect their natural ordering.

    A worker polls every `idling` seconds, so neither the heartbeat nor the periodic
    cleanup can be expected to fire faster than that. Preflight has to give Elasticsearch
    a real chance to come up (more than one attempt, with a non-trivial idle between
    attempts), and the configured download cap has to be a positive byte count we
    actually want to permit.
    """
    service = add_defaults({})["service"]

    assert service["heartbeat"] > service["idling"]
    assert service["job_cleanup_interval"] > service["idling"]
    assert service["preflight_max_attempts"] >= 2
    assert service["preflight_idle"] >= 1
    assert service["max_concurrent_content_syncs"] >= 1
    assert 0 < service["max_file_download_size"] <= 1024**3
