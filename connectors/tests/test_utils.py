#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import base64
import binascii
import contextlib
import functools
import os
import random
import tempfile
import time
import timeit
from unittest.mock import Mock

import pytest
from freezegun import freeze_time
from pympler import asizeof

from connectors import utils
from connectors.utils import (
    ConcurrentTasks,
    InvalidIndexNameError,
    MemQueue,
    RetryStrategy,
    convert_to_b64,
    get_base64_value,
    get_pem_format,
    get_size,
    next_run,
    retryable,
    validate_index_name,
)


@freeze_time("2023-01-18 17:18:56.814003", tick=True)
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


def test_mem_queue_speed(patch_logger):
    def mem_queue():
        import asyncio

        from connectors.utils import MemQueue

        queue = MemQueue(
            maxmemsize=1024 * 1024, refresh_interval=0.1, refresh_timeout=2
        )

        async def run():
            for _ in range(1000):
                await queue.put("x" * 100)

        asyncio.run(run())

    mem_queue_duration = min(timeit.repeat(mem_queue, number=1, repeat=3))

    def vanilla_queue():
        import asyncio

        queue = asyncio.Queue()

        async def run():
            for _ in range(1000):
                await queue.put("x" * 100)

        asyncio.run(run())

    queue_duration = min(timeit.repeat(vanilla_queue, number=1, repeat=3))

    # mem queue should be 30 times slower at the most
    quotient, _ = divmod(mem_queue_duration, queue_duration)
    assert quotient < 30


@pytest.mark.asyncio
async def test_mem_queue_race(patch_logger):
    item = "small stuff"
    queue = MemQueue(
        maxmemsize=get_size(item) * 2 + 1, refresh_interval=0.01, refresh_timeout=1
    )
    max_size = 0

    async def put_one():
        nonlocal max_size
        await queue.put(item)
        qsize = queue.qmemsize()
        await asyncio.sleep(0)
        if max_size < qsize:
            max_size = qsize

    async def remove_one():
        await queue.get()

    tasks = []
    for _ in range(1000):
        tasks.append(put_one())
        tasks.append(remove_one())

    random.shuffle(tasks)
    tasks = [asyncio.create_task(t) for t in tasks]

    await asyncio.gather(*tasks)
    assert max_size <= get_size(item) * 2


@pytest.mark.asyncio
async def test_mem_queue(patch_logger):
    queue = MemQueue(maxmemsize=1024, refresh_interval=0, refresh_timeout=0.1)
    await queue.put("small stuff")

    assert not queue.full()
    assert queue.qmemsize() == asizeof.asizeof("small stuff")

    # let's pile up until it can't accept anymore stuff
    while True:
        try:
            await queue.put("x" * 100)
        except asyncio.QueueFull:
            break

    when = []

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
        assert not queue.full()

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


@contextlib.contextmanager
def temp_file(converter):
    if converter == "system":
        assert utils._BASE64 is not None
        _SAVED = None
    else:
        _SAVED = utils._BASE64
        utils._BASE64 = None
    try:
        content = binascii.hexlify(os.urandom(32)).strip()
        with tempfile.NamedTemporaryFile() as fp:
            fp.write(content)
            fp.flush()
            yield fp.name, content
    finally:
        if _SAVED is not None:
            utils._BASE64 = _SAVED


@pytest.mark.parametrize("converter", ["system", "py"])
def test_convert_to_b64_inplace(converter):
    with temp_file(converter) as (source, content):
        # convert in-place
        result = convert_to_b64(source)

        assert result == source
        with open(result, "rb") as f:
            assert f.read().strip() == base64.b64encode(content).strip()


@pytest.mark.parametrize("converter", ["system", "py"])
def test_convert_to_b64_target(converter):
    with temp_file(converter) as (source, content):
        # convert to a specific file
        try:
            target = f"{source}.here"
            result = convert_to_b64(source, target=target)
            with open(result, "rb") as f:
                assert f.read().strip() == base64.b64encode(content).strip()
        finally:
            if os.path.exists(target):
                os.remove(target)


@pytest.mark.parametrize("converter", ["system", "py"])
def test_convert_to_b64_no_overwrite(converter):
    with temp_file(converter) as (source, content):
        # check overwrite
        try:
            target = f"{source}.here"
            with open(target, "w") as f:
                f.write("some")

            # if the file exists we should raise an error..
            with pytest.raises(IOError):
                convert_to_b64(source, target=target)

            # ..unless we use `overwrite`
            result = convert_to_b64(source, target=target, overwrite=True)
            with open(result, "rb") as f:
                assert f.read().strip() == base64.b64encode(content)
        finally:
            if os.path.exists(target):
                os.remove(target)


class CustomException(Exception):
    pass


@pytest.mark.fail_slow(1)
@pytest.mark.asyncio
async def test_exponential_backoff_retry():
    mock_func = Mock()
    num_retries = 10

    @retryable(
        retries=num_retries,
        interval=0,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def raises():
        mock_func()
        raise CustomException()

    with pytest.raises(CustomException):
        await raises()

        # retried 10 times
        assert mock_func.call_count == num_retries

    # would lead to roughly ~ 50 seconds of retrying
    @retryable(retries=10, interval=5, strategy=RetryStrategy.LINEAR_BACKOFF)
    async def does_not_raise():
        pass

    # would fail, if retried once (retry_interval = 5 seconds). Explicit time boundary for this test: 1 second
    await does_not_raise()


def test_get_pem_format():
    """This function tests prepare private key and certificate with dummy values"""
    # Setup
    expected_formated_pem_key = """-----BEGIN PRIVATE KEY-----
PrivateKey
-----END PRIVATE KEY-----"""
    private_key = "-----BEGIN PRIVATE KEY----- PrivateKey -----END PRIVATE KEY-----"

    # Execute
    formated_privat_key = get_pem_format(key=private_key, max_split=2)
    assert formated_privat_key == expected_formated_pem_key

    # Setup
    expected_formated_certificate = """-----BEGIN CERTIFICATE-----
Certificate1
Certificate2
-----END CERTIFICATE-----"""
    certificate = "-----BEGIN CERTIFICATE----- Certificate1 Certificate2 -----END CERTIFICATE-----"

    # Execute
    formated_certificate = get_pem_format(key=certificate, max_split=1)
    assert formated_certificate == expected_formated_certificate
