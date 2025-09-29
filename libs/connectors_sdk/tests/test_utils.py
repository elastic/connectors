#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64
import binascii
import contextlib
import os
import random
import string
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from connectors_sdk import utils
from connectors_sdk.utils import (
    convert_to_b64,
    hash_id,
    nested_get_from_dict,
    shorten_str,
    with_utc_tz,
)


def test_hash_id():
    limit = 512
    random_id_too_long = "".join(
        random.choices(string.ascii_letters + string.digits, k=1000)
    )

    assert len(hash_id(random_id_too_long).encode("UTF-8")) < limit


@pytest.fixture
def patch_file_ops():
    with patch("connectors_sdk.utils.open"):
        with patch("os.remove"):
            with patch("os.rename"):
                yield


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


@pytest.mark.parametrize(
    "dictionary, default, expected",
    [
        (None, None, None),
        ({}, None, None),
        ({}, "default", "default"),
        ({"foo": {}}, None, None),
        ({"foo": {"bar": {}}}, None, None),
        ({"foo": {"bar": {"baz": "result"}}}, None, "result"),
    ],
)
def test_nested_get_from_dict(dictionary, default, expected):
    keys = ["foo", "bar", "baz"]

    assert nested_get_from_dict(dictionary, keys, default=default) == expected


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


@pytest.mark.parametrize(
    "system,mac_ver,cmd_template",
    [
        ("Darwin", "13.0", "/usr/bin/base64 -i {source} -o {target}"),
        ("Darwin", "12.0", "/usr/bin/base64 {source} > {target}"),
        ("Ubuntu", None, "/usr/bin/base64 -w 0 {source} > {target}"),
    ],
)
def test_convert_to_b64_newer_macos(system, mac_ver, cmd_template, patch_file_ops):
    with (
        patch("platform.system", return_value=system),
        patch("platform.mac_ver", return_value=[mac_ver]),
        patch("subprocess.check_call") as subprocess_mock,
    ):
        with temp_file("system") as (source, _):
            target = f"{source}.b64"

            convert_to_b64(source, target, overwrite=False)

            expected_cmd = cmd_template.format(source=source, target=target)
            subprocess_mock.assert_called_with(expected_cmd, shell=True)


@pytest.mark.parametrize(
    "original, shorten_by, shortened",
    [
        ("", 0, ""),
        ("", 1000, ""),
        (None, 0, ""),
        (None, 1000, ""),
        # introducing '...' would increase the string length -> no shortening
        ("abcdefgh", 0, "abcdefgh"),
        ("abcdefgh", 1, "abcdefgh"),
        ("abcdefgh", 2, "abcdefgh"),
        # valid shortening
        ("abcdefgh", 4, "ab...gh"),
        ("abcdefgh", 5, "ab...h"),
        ("abcdefg", 4, "ab...g"),
        ("abcdefg", 5, "a...g"),
        # shortens to the max, if shorten_by is bigger than the actual string
        ("abcdefgh", 1000, "a...h"),
    ],
)
def test_shorten_str(original, shorten_by, shortened):
    assert shorten_str(original, shorten_by) == shortened


def test_with_utc_tz_naive_timestamp():
    ts_naive = datetime(2024, 5, 27, 12, 0, 0)
    ts_utc = with_utc_tz(ts_naive)
    assert ts_utc.tzinfo == timezone.utc
    assert ts_utc.year == 2024
    assert ts_utc.month == 5
    assert ts_utc.day == 27
    assert ts_utc.hour == 12
    assert ts_utc.minute == 0
    assert ts_utc.second == 0


def test_with_utc_tz_aware_timestamp():
    ts_aware = datetime(
        2024, 5, 27, 12, 0, 0, tzinfo=timezone(timedelta(hours=5))
    )  # Timezone aware timestamp with +5 offset
    ts_utc = with_utc_tz(ts_aware)
    assert ts_utc.tzinfo == timezone.utc
    assert ts_utc.year == 2024
    assert ts_utc.month == 5
    assert ts_utc.day == 27
    assert ts_utc.hour == 7  # Converted to UTC
    assert ts_utc.minute == 0
    assert ts_utc.second == 0


def test_with_utc_tz_timestamp_in_utc():
    ts_aware = datetime(2024, 5, 27, 12, 0, 0, tzinfo=timezone.utc)
    ts_utc = with_utc_tz(ts_aware)
    assert ts_utc.tzinfo == timezone.utc
    assert ts_utc.year == 2024
    assert ts_utc.month == 5
    assert ts_utc.day == 27
    assert ts_utc.hour == 12
    assert ts_utc.minute == 0
    assert ts_utc.second == 0
