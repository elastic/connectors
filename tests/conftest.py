#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import io
import os
import re
import sys
import traceback
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from aioresponses import aioresponses


class Logger:
    def __init__(self, silent=True):
        self.logs = []
        self.silent = silent

    def debug(self, msg, exc_info=False):
        if not self.silent:
            print(msg)
        self.logs.append(msg)
        if exc_info:
            self.logs.append(traceback.format_exc())

    def assert_instance(self, instance):
        for log in self.logs:
            if isinstance(log, instance):
                return
        raise AssertionError(f"Could not find an instance of {instance}")

    def assert_not_present(self, lines):
        if isinstance(lines, str):
            lines = [lines]
        for msg in lines:
            for log in self.logs:
                if isinstance(log, str) and msg in log:
                    raise AssertionError(f"'{msg}' found in {self.logs}")

    def assert_present(self, lines):
        if isinstance(lines, str):
            lines = [lines]
        for msg in lines:
            found = False
            for log in self.logs:
                if isinstance(log, str) and msg in log:
                    found = True
                    break
            if not found:
                raise AssertionError(f"'{msg}' not found in {self.logs}")

    error = exception = critical = info = debug


@pytest.fixture
def set_env():
    old = os.environ.get("elasticsearch.password")
    os.environ["elasticsearch.password"] = "password"
    try:
        yield
    finally:
        if old:
            os.environ["elasticsearch.password"] = old


@pytest.fixture
def catch_stdout():
    old = sys.stdout
    new = sys.stdout = io.StringIO()
    try:
        yield new
    finally:
        sys.stdout = old


@pytest.fixture
def patch_logger(silent=True):
    new_logger = Logger(silent)

    from connectors.logger import logger

    methods = ("exception", "error", "critical", "info", "debug", "warning")
    for method in methods:
        setattr(logger, f"_old_{method}", getattr(logger, method))
        setattr(logger, method, new_logger.info)

    try:
        yield new_logger
    finally:
        for method in methods:
            setattr(logger, method, getattr(logger, f"_old_{method}"))
            delattr(logger, f"_old_{method}")


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_responses():
    with aioresponses() as m:
        yield m


@pytest_asyncio.fixture
async def patch_cancellable_sleeps():
    with patch(
        "connectors.utils.CancellableSleeps.sleep", return_value=AsyncMock()
    ) as new_mock:
        yield new_mock


@pytest_asyncio.fixture
async def patch_sleep():
    with patch("asyncio.sleep", return_value=AsyncMock) as patch_sleep:
        # To avoid actually sleeping
        yield patch_sleep


@pytest.fixture
def mock_aws():
    if "AWS_ACCESS_KEY_ID" in os.environ:
        old_key = os.environ["AWS_ACCESS_KEY_ID"]
        os.environ["AWS_ACCESS_KEY_ID"] = "xxx"
    else:
        old_key = None

    if "AWS_SECRET_ACCESS_KEY" in os.environ:
        old_secret = os.environ["AWS_SECRET_ACCESS_KEY"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = "xxx"
    else:
        old_secret = None

    try:
        yield
    finally:
        if old_secret is not None:
            os.environ["AWS_SECRET_ACCESS_KEY"] = old_secret
        if old_key is not None:
            os.environ["AWS_ACCESS_KEY_ID"] = old_key


def assert_re(expr, items):
    expr = re.compile(expr)

    for item in reversed(items):
        if isinstance(item, str) and expr.match(item):
            return

    raise AssertionError(f"{expr} not found in {items}")
