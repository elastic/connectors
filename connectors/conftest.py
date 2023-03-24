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

import pytest
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

    def assert_check(self, callable):
        for log in self.logs:
            if callable(log):
                return
        raise AssertionError(f"{callable} returned False for {self.logs}")

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


def pytest_itemcollected(item):
    """
    This code is here to format the output of verbose run of pytest.

    What it does is - it receives a object representing a test that will be ran in {item} argument.
    Test can be located either just in the module, or inside of the class,
    or inside of the class inside of the class inside of the class, etc.

    So the idea of the code inside is to collect the name/docstring of current node
    and then repeat it recursively until an instance of _pytest.python.Module is met.

    It gives the following results:
    <module>.<class>.<test> or <module>.<test> or <module>.<class>.<class>.<test>

    It tries to use node's docstring if there's one, otherwise its name (or class name for modules)
    """

    def _collect_recursively(n):
        from _pytest.python import Module

        if isinstance(n, Module):
            return n.obj.__doc__.strip() if n.obj.__doc__ else n.obj.__class__.__name__

        parent = n.parent

        parent_description = _collect_recursively(parent) if parent else ""
        node_description = n.obj.__doc__.strip() if n.obj.__doc__ else n.obj.__name__

        return ".".join((parent_description, node_description))

    item._nodeid = _collect_recursively(item)
