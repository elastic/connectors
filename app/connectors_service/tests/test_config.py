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
ES_CONFIG_FILE = os.path.join(FIXTURES_DIR, "entsearch.yml")
ES_CONFIG_INVALID_LOG_LEVEL_FILE = os.path.join(
    FIXTURES_DIR, "entsearch_invalid_log_level.yml"
)


def test_bad_config_file():
    with pytest.raises(FileNotFoundError):
        load_config("BEEUUUAH")


def test_config(set_env):
    config = load_config(CONFIG_FILE)
    assert isinstance(config, dict)
    assert config["elasticsearch"]["host"] == "http://nowhere.com:9200"
    assert config["elasticsearch"]["user"] == "elastic"


def test_config_with_ent_search(set_env):
    with mock.patch.dict(os.environ, {"ENT_SEARCH_CONFIG_PATH": ES_CONFIG_FILE}):
        config = load_config(CONFIG_FILE)
        assert config["elasticsearch"]["headers"]["X-Elastic-Auth"] == "SomeYeahValue"
        assert config["service"]["log_level"] == "DEBUG"


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


def test_default_config_protects_critical_values():
    """Snapshot of critical default values.

    These defaults shape production behavior (timeouts, retries, concurrency, file
    size limits). Update this test deliberately if a default is intentionally changed.
    """
    defaults = add_defaults({})

    bulk = defaults["elasticsearch"]["bulk"]
    assert bulk["queue_max_size"] == 1024
    assert bulk["queue_max_mem_size"] == 25
    assert bulk["queue_refresh_interval"] == 1
    assert bulk["queue_refresh_timeout"] == 700
    assert bulk["display_every"] == 100
    assert bulk["chunk_size"] == 1000
    assert bulk["max_concurrency"] == 5
    assert bulk["chunk_max_mem_size"] == 5
    assert bulk["max_retries"] == 5
    assert bulk["retry_interval"] == 10
    assert bulk["concurrent_downloads"] == 10
    assert bulk["enable_operations_logging"] is False

    error_monitor = bulk["error_monitor"]
    assert error_monitor["enabled"] is True
    assert error_monitor["max_total_errors"] == 1000
    assert error_monitor["max_consecutive_errors"] == 10
    assert error_monitor["max_error_rate"] == 0.15
    assert error_monitor["error_window_size"] == 100
    assert error_monitor["error_queue_size"] == 10

    es = defaults["elasticsearch"]
    assert es["max_retries"] == 5
    assert es["retry_interval"] == 10
    assert es["retry_on_timeout"] is True
    assert es["request_timeout"] == 120
    assert es["max_wait_duration"] == 120
    assert es["initial_backoff_duration"] == 1
    assert es["backoff_multiplier"] == 2

    service = defaults["service"]
    assert service["idling"] == 30
    assert service["heartbeat"] == 300
    assert service["preflight_max_attempts"] == 10
    assert service["preflight_idle"] == 30
    assert service["max_errors"] == 20
    assert service["max_errors_span"] == 600
    assert service["max_concurrent_content_syncs"] == 1
    assert service["max_concurrent_access_control_syncs"] == 1
    assert service["max_concurrent_scheduling_tasks"] == 4
    assert service["max_file_download_size"] == 10485760
    assert service["job_cleanup_interval"] == 300
