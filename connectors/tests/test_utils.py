#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import base64
import functools
import time

import pytest
from pympler import asizeof

from connectors.utils import (
    ConcurrentTasks,
    ESClient,
    InvalidIndexNameError,
    MemQueue,
    get_base64_value,
    next_run,
    validate_index_name,
)


def test_next_run():
    # can run within two minutes
    assert next_run("1 * * * * *") < 120
    assert next_run("* * * * * *") == 0

    # this should get parsed
    next_run("0/5 14,18,52 * ? JAN,MAR,SEP MON-FRI 2010-2030")


def test_invalid_names():
    for name in (
        "index?name",
        "index#name",
        "_indexname",
        "-indexname",
        "+indexname",
        "INDEXNAME",
        "..",
        ".",
    ):
        with pytest.raises(InvalidIndexNameError):
            validate_index_name(name)


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
    }
    es_client = ESClient(config)
    assert not await es_client.ping()
    await es_client.close()


@pytest.mark.asyncio
async def test_mem_queue(patch_logger):

    queue = MemQueue(maxmemsize=1024, refresh_interval=0, refresh_timeout=2)
    await queue.put("small stuff")

    assert not queue.mem_full()
    assert queue.qmemsize() == asizeof.asizeof("small stuff")

    # let's pile up until it can't accept anymore stuff
    while True:
        try:
            await queue.put("x" * 100)
        except asyncio.QueueFull:
            break

    when = []
    # then next put will block until we release some memory

    async def add_data():
        when.append(time.time())
        # this call gets throttled for at the most 2s before it breaks
        await queue.put("DATA" * 10)
        when.append(time.time())

    async def remove_data():
        await asyncio.sleep(0.1)
        size, item = await queue.get()
        assert (size, item) == (64, "small stuff")
        await asyncio.sleep(0)
        await queue.get()  # removes the 2kb
        assert not queue.mem_full()

    await asyncio.gather(remove_data(), add_data())
    assert when[1] - when[0] > 0.1


def test_get_base64_value():
    """This test verify get_base64_value method and convert encoded data into base64"""
    expected_result = get_base64_value("dummy".encode("utf-8"))
    assert expected_result == "ZHVtbXk="


@pytest.mark.asyncio
async def test_concurrent_runner(patch_logger):
    results = []

    def _results_callback(result):
        results.append(result)

    async def coroutine(i):
        await asyncio.sleep(0.1)
        return i

    runner = ConcurrentTasks(results_callback=_results_callback)
    for i in range(10):
        await runner.put(functools.partial(coroutine, i))

    await runner.join()
    assert results == list(range(10))


@pytest.mark.asyncio
async def test_concurrent_runner_fails(patch_logger):
    results = []

    def _results_callback(result):
        results.append(result)

    async def coroutine(i):
        await asyncio.sleep(0.1)
        if i == 5:
            raise Exception("I FAILED")
        return i

    runner = ConcurrentTasks(results_callback=_results_callback)
    for i in range(10):
        await runner.put(functools.partial(coroutine, i))

    with pytest.raises(Exception):
        await runner.join()

    assert 5 not in results


@pytest.mark.asyncio
async def test_concurrent_runner_high_concurrency(patch_logger):
    results = []

    def _results_callback(result):
        results.append(result)

    async def coroutine(i):
        await asyncio.sleep(0)
        return i

    second_results = []

    def _second_callback(result):
        second_results.append(result)

    runner = ConcurrentTasks(results_callback=_results_callback)
    for i in range(1000):
        if i == 3:
            callback = _second_callback
        else:
            callback = None
        await runner.put(functools.partial(coroutine, i), result_callback=callback)

    await runner.join()
    assert results == list(range(1000))
    assert second_results == [3]
