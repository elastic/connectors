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
import ssl
import string
import tempfile
import time
import timeit
from datetime import datetime
from unittest.mock import Mock, mock_open, patch

import pytest
import pytest_asyncio
from aioresponses import aioresponses
from freezegun import freeze_time
from pympler import asizeof

from connectors import utils
from connectors.utils import (
    ConcurrentTasks,
    ExtractionService,
    InvalidIndexNameError,
    MemQueue,
    RetryStrategy,
    base64url_to_base64,
    convert_to_b64,
    decode_base64_value,
    deep_merge_dicts,
    evaluate_timedelta,
    filter_nested_dict_by_keys,
    get_base64_value,
    get_pem_format,
    get_size,
    has_duplicates,
    hash_id,
    html_to_text,
    is_expired,
    iterable_batches_generator,
    next_run,
    retryable,
    ssl_context,
    truncate_id,
    url_encode,
    validate_index_name,
)


def test_next_run():
    now = datetime(2023, 1, 18, 17, 18, 56, 814)
    # can run within two minutes
    assert (
        next_run("1 * * * * *", now).isoformat(" ", "seconds") == "2023-01-18 17:19:01"
    )
    assert (
        next_run("* * * * * *", now).isoformat(" ", "seconds") == "2023-01-18 17:18:57"
    )

    # this should get parsed
    next_run("0/5 14,18,52 * ? JAN,MAR,SEP MON-FRI 2010-2030", now)


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


def test_mem_queue_speed():
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
async def test_mem_queue_race():
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
async def test_mem_queue():
    queue = MemQueue(maxmemsize=1024, refresh_interval=0, refresh_timeout=0.15)
    await queue.put("small stuff")

    assert not queue.full()
    assert queue.qmemsize() == asizeof.asizeof("small stuff")

    # let's pile up until it can't accept anymore stuff

    with pytest.raises(asyncio.QueueFull):
        while True:
            await queue.put("x" * 100)

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


@pytest.mark.asyncio
async def test_mem_queue_too_large_item():
    """
    When an item is added to the queue that is larger than the queue capacity then the item is discarded

    Imagine that maximum queue size is 5MB and an item of 6MB is added inside.
    Before the logic was to declare the queue full in this case, but it becomes deadlocked - queue is
    trying to free some space up to fit a 6MB item, but it's not possible.

    After a fix the item is discarded with a log line stating that the document is skipped.
    """
    queue = MemQueue(maxmemsize=1, refresh_interval=0.1, refresh_timeout=0.5)

    # Does not raise an error - it'll be full after the item is added inside
    await queue.put("lala" * 1000)

    with pytest.raises(asyncio.QueueFull) as e:
        await queue.put("x")

    assert e is not None


@pytest.mark.asyncio
async def test_mem_queue_put_nowait():
    queue = MemQueue(
        maxsize=5, maxmemsize=1000, refresh_interval=0.1, refresh_timeout=0.5
    )
    # make queue full by size
    for i in range(5):
        queue.put_nowait(i)

    with pytest.raises(asyncio.QueueFull) as e:
        await queue.put_nowait("x")

    assert e is not None


def test_get_base64_value():
    """This test verify get_base64_value method and convert encoded data into base64"""
    expected_result = get_base64_value("dummy".encode("utf-8"))
    assert expected_result == "ZHVtbXk="


def test_decode_base64_value():
    """This test verify decode_base64_value and decodes base64 encoded data"""
    expected_result = decode_base64_value("ZHVtbXk=".encode("utf-8"))
    assert expected_result == b"dummy"


@pytest.mark.asyncio
async def test_concurrent_runner():
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
async def test_concurrent_runner_fails():
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

    await runner.join()
    assert 5 not in results


@pytest.mark.asyncio
async def test_concurrent_runner_high_concurrency():
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


class CustomGeneratorException(Exception):
    pass


@pytest.mark.fail_slow(1)
@pytest.mark.asyncio
async def test_exponential_backoff_retry_async_generator():
    mock_gen = Mock()
    num_retries = 10

    @retryable(
        retries=num_retries,
        interval=0,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def raises_async_generator():
        for _ in range(3):
            mock_gen()
            raise CustomGeneratorException()
            yield 1

    with pytest.raises(CustomGeneratorException):
        async for _ in raises_async_generator():
            pass

    # retried 10 times
    assert mock_gen.call_count == num_retries

    # would lead to roughly ~ 50 seconds of retrying
    @retryable(retries=10, interval=5, strategy=RetryStrategy.LINEAR_BACKOFF)
    async def does_not_raise_async_generator():
        for _ in range(3):
            yield 1

    # would fail, if retried once (retry_interval = 5 seconds). Explicit time boundary for this test: 1 second
    items = []
    async for item in does_not_raise_async_generator():
        items.append(item)

    assert items == [1, 1, 1]


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


@pytest.mark.parametrize(
    "skipped_exceptions",
    [CustomGeneratorException, [CustomGeneratorException, RuntimeError]],
)
@pytest.mark.asyncio
async def test_skipped_exceptions_retry_async_generator(skipped_exceptions):
    mock_gen = Mock()
    num_retries = 10

    @retryable(
        retries=num_retries,
        skipped_exceptions=skipped_exceptions,
    )
    async def raises_async_generator():
        for _ in range(3):
            mock_gen()
            raise CustomGeneratorException()
            yield 1

    with pytest.raises(CustomGeneratorException):
        async for _ in raises_async_generator():
            pass

    assert mock_gen.call_count == 1


@pytest.mark.parametrize(
    "skipped_exceptions", [CustomException, [CustomException, RuntimeError]]
)
@pytest.mark.asyncio
async def test_skipped_exceptions_retry(skipped_exceptions):
    mock_func = Mock()
    num_retries = 10

    @retryable(
        retries=num_retries,
        skipped_exceptions=skipped_exceptions,
    )
    async def raises():
        mock_func()
        raise CustomException()

    with pytest.raises(CustomException):
        await raises()

        # retried 10 times
        assert mock_func.call_count == 1


class MockSSL:
    """This class contains methods which returns dummy ssl context"""

    def load_verify_locations(self, cadata):
        """This method verify locations"""
        pass


def test_ssl_context():
    """This function test ssl_context with dummy certificate"""
    # Setup
    certificate = "-----BEGIN CERTIFICATE----- Certificate -----END CERTIFICATE-----"

    # Execute
    with patch.object(ssl, "create_default_context", return_value=MockSSL()):
        ssl_context(certificate=certificate)


def test_url_encode():
    """Test the url_encode method by passing a string"""
    # Execute
    encode_response = url_encode("http://ascii.cl?parameter='Click on URL Decode!'")
    # Assert
    assert (
        encode_response
        == "http%3A%2F%2Fascii.cl%3Fparameter%3D'Click%20on%20URL%20Decode%21'"
    )


def test_is_expired():
    """This method checks whether token expires or not"""
    # Execute
    expires_at = datetime.fromisoformat("2023-02-10T09:02:23.629821")
    actual_response = is_expired(expires_at=expires_at)
    # Assert
    assert actual_response is True


@freeze_time("2023-02-18 14:25:26.158843", tz_offset=-4)
def test_evaluate_timedelta():
    """This method tests adding seconds to the current utc time"""
    # Execute
    expected_response = evaluate_timedelta(seconds=86399, time_skew=20)

    # Assert
    assert expected_response == "2023-02-19T14:25:05.158843"


def test_get_pem_format_with_postfix():
    expected_formatted_pem_key = """-----BEGIN PRIVATE KEY-----
PrivateKey
-----END PRIVATE KEY-----"""
    private_key = "-----BEGIN PRIVATE KEY----- PrivateKey -----END PRIVATE KEY-----"

    formatted_private_key = get_pem_format(
        key=private_key, postfix="-----END PRIVATE KEY-----"
    )
    assert formatted_private_key == expected_formatted_pem_key


def test_get_pem_format_multiline():
    expected_formatted_certificate = """-----BEGIN CERTIFICATE-----
Certificate1
Certificate2
-----END CERTIFICATE-----"""
    certificate = "-----BEGIN CERTIFICATE----- Certificate1 Certificate2 -----END CERTIFICATE-----"

    formatted_certificate = get_pem_format(key=certificate)
    assert formatted_certificate == expected_formatted_certificate


def test_get_pem_format_multiple_certificates():
    expected_formatted_multiple_certificates = """-----BEGIN CERTIFICATE-----
Certificate1
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
Certificate2
-----END CERTIFICATE-----
"""
    multiple_certificates = "-----BEGIN CERTIFICATE----- Certificate1 -----END CERTIFICATE----- -----BEGIN CERTIFICATE----- Certificate2 -----END CERTIFICATE-----"

    formatted_multi_certificate = get_pem_format(key=multiple_certificates)
    assert formatted_multi_certificate == expected_formatted_multiple_certificates


def test_hash_id():
    limit = 512
    random_id_too_long = "".join(
        random.choices(string.ascii_letters + string.digits, k=1000)
    )

    assert len(hash_id(random_id_too_long).encode("UTF-8")) < limit


def test_truncate_id():
    long_id = "something-12341361361-21905128510263"
    truncated_id = truncate_id(long_id)

    assert len(truncated_id) < len(long_id)


@pytest.mark.parametrize(
    "_list, should_have_duplicate",
    [([], False), (["abc"], False), (["abc", "def"], False), (["abc", "abc"], True)],
)
def test_has_duplicates(_list, should_have_duplicate):
    assert has_duplicates(_list) == should_have_duplicate


@pytest.mark.parametrize(
    "key_list, source_dict, expected_dict",
    [
        (["one", "two"], {"foo": {"one": 1, "two": 2}}, {}),
        (
            ["one", "two"],
            {"foo": {"one": 1, "two": 2}, "bar": {"one": 1, "three": 3}},
            {"bar": {"one": 1, "three": 3}},
        ),
        (
            ["one", "two"],
            {"foo": {}},
            {"foo": {}},
        ),
        (
            [],
            {"foo": {"one": 1, "two": 2}, "bar": {"one": 1, "three": 3}},
            {},
        ),
    ],
)
def test_filter_nested_dict_by_keys(key_list, source_dict, expected_dict):
    assert filter_nested_dict_by_keys(key_list, source_dict) == expected_dict


@pytest.mark.parametrize(
    "base_dict, new_dict, expected_dict",
    [
        (
            {"foo": {"one": 1, "two": 2}, "bar": {"one": 1, "two": 2}},
            {"foo": {"one": 10}, "bar": {"three": 30}},
            {"foo": {"one": 10, "two": 2}, "bar": {"one": 1, "two": 2, "three": 30}},
        ),
        (
            {"foo": {}, "bar": {}},
            {"foo": {"one": 1}, "bar": {"three": 3}},
            {"foo": {"one": 1}, "bar": {"three": 3}},
        ),
        (
            {
                "foo": {"level2": {"level3": "fafa"}},
                "bar": {"level2": {"level3": "fafa"}},
            },
            {
                "foo": {"level2": {"level3": "foofoo"}},
                "bar": {"level2": {"level4": "haha"}},
            },
            {
                "foo": {"level2": {"level3": "foofoo"}},
                "bar": {"level2": {"level3": "fafa", "level4": "haha"}},
            },
        ),
    ],
)
def test_deep_merge_dicts(base_dict, new_dict, expected_dict):
    assert deep_merge_dicts(base_dict, new_dict) == expected_dict


def test_html_to_text_with_html_with_unclosed_tag():
    invalid_html = "<div>Hello, world!</div><div>Next Line"

    assert html_to_text(invalid_html) == "Hello, world!\nNext Line"


def test_html_to_text_without_html():
    invalid_html = "just text"

    assert html_to_text(invalid_html) == "just text"


def test_html_to_text_with_weird_html():
    invalid_html = "<div/>just</div> text"

    text = html_to_text(invalid_html)

    assert "just" in text
    assert "text" in text


def batch_size(value):
    """Used for readability purposes in parametrized tests."""
    return value


@pytest.mark.parametrize(
    "iterable, batch_size_, expected_batches",
    [
        ([1, 2, 3], batch_size(1), [[1], [2], [3]]),
        ([1, 2, 3], batch_size(2), [[1, 2], [3]]),
        (
            [1, 2, 3],
            batch_size(3),
            [
                [1, 2, 3],
            ],
        ),
        (
            [1, 2, 3],
            batch_size(1000),
            [
                [1, 2, 3],
            ],
        ),
    ],
)
def test_iterable_batches_generator(iterable, batch_size_, expected_batches):
    actual_batches = []

    for batch in iterable_batches_generator(iterable, batch_size_):
        actual_batches.append(batch)

    assert actual_batches == expected_batches


class TestExtractionService:
    @pytest_asyncio.fixture
    async def mock_responses(self):
        with aioresponses() as m:
            yield m

    @pytest.mark.parametrize(
        "mock_config, expected_result",
        [
            (
                {
                    "extraction_service": {
                        "host": "http://localhost:8090",
                    }
                },
                True,
            ),
            ({"something_else": "???"}, False),
            ({"extraction_service": {"not_a_host": "!!!m"}}, False),
        ],
    )
    def test_check_configured(self, mock_config, expected_result):
        with patch(
            "connectors.utils.ExtractionService.get_extraction_config",
            return_value=mock_config.get("extraction_service", None),
        ):
            extraction_service = ExtractionService()
            assert extraction_service._check_configured() is expected_result

    @pytest.mark.asyncio
    async def test_extract_text(self, mock_responses, patch_logger):
        filepath = "tmp/notreal.txt"
        url = "http://localhost:8090/extract_text/"
        payload = {"extracted_text": "I've been extracted!"}

        with patch("builtins.open", mock_open(read_data=b"data")), patch(
            "connectors.utils.ExtractionService.get_extraction_config",
            return_value={"host": "http://localhost:8090"},
        ):
            mock_responses.put(url, status=200, payload=payload)

            extraction_service = ExtractionService()
            extraction_service._begin_session()

            response = await extraction_service.extract_text(filepath, "notreal.txt")
            await extraction_service._end_session()

            assert response == "I've been extracted!"
            patch_logger.assert_present(
                "Text extraction is successful for 'notreal.txt'."
            )

    @pytest.mark.asyncio
    async def test_extract_text_with_file_pointer(self, mock_responses, patch_logger):
        filepath = "/tmp/notreal.txt"
        url = "http://localhost:8090/extract_text/?local_file_path=/tmp/notreal.txt"
        payload = {"extracted_text": "I've been extracted from a local file!"}

        with patch("builtins.open", mock_open(read_data=b"data")), patch(
            "connectors.utils.ExtractionService.get_extraction_config",
            return_value={
                "host": "http://localhost:8090",
                "use_file_pointers": True,
                "shared_volume_dir": "/tmp",
            },
        ):
            mock_responses.put(url, status=200, payload=payload)

            extraction_service = ExtractionService()
            extraction_service._begin_session()

            response = await extraction_service.extract_text(filepath, "notreal.txt")
            await extraction_service._end_session()

            assert response == "I've been extracted from a local file!"
            patch_logger.assert_present(
                "Text extraction is successful for 'notreal.txt'."
            )

    @pytest.mark.asyncio
    async def test_extract_text_when_response_isnt_200_logs_warning(
        self, mock_responses, patch_logger
    ):
        filepath = "tmp/notreal.txt"
        url = "http://localhost:8090/extract_text/"

        with patch("builtins.open", mock_open(read_data=b"data")), patch(
            "connectors.utils.ExtractionService.get_extraction_config",
            return_value={"host": "http://localhost:8090"},
        ):
            mock_responses.put(
                url,
                status=422,
                payload={
                    "error": "Unprocessable Entity",
                    "message": "Could not process file.",
                },
            )

            extraction_service = ExtractionService()
            extraction_service._begin_session()

            response = await extraction_service.extract_text(filepath, "notreal.txt")
            await extraction_service._end_session()
            assert response == ""

            patch_logger.assert_present(
                "Extraction service could not parse `notreal.txt'. Status: [422]; Unprocessable Entity: Could not process file."
            )

    @pytest.mark.asyncio
    async def test_extract_text_when_response_is_200_with_error_logs_warning(
        self, mock_responses, patch_logger
    ):
        filepath = "tmp/notreal.txt"
        url = "http://localhost:8090/extract_text/"

        with patch("builtins.open", mock_open(read_data=b"data")), patch(
            "connectors.utils.ExtractionService.get_extraction_config",
            return_value={"host": "http://localhost:8090"},
        ):
            mock_responses.put(
                url,
                status=200,
                payload={"error": "oh no!", "message": "I'm all messed up..."},
            )

            extraction_service = ExtractionService()
            extraction_service._begin_session()

            response = await extraction_service.extract_text(filepath, "notreal.txt")
            await extraction_service._end_session()
            assert response == ""

            patch_logger.assert_present(
                "Extraction service could not parse `notreal.txt'. Status: [200]; oh no!: I'm all messed up..."
            )


@pytest.mark.parametrize(
    "base64url_encoded_value, base64_expected_value",
    [("YQ-_", "YQ+/"), ("", ""), (None, None)],
)
def test_base64url_to_base64(base64url_encoded_value, base64_expected_value):
    assert base64url_to_base64(base64url_encoded_value) == base64_expected_value
