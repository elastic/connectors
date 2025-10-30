#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import traceback

import pytest
from aioresponses import aioresponses


class Logger:
    def __init__(self, silent=True):
        self.logs = []
        self.silent = silent

    def debug(self, msg, exc_info=False):
        if not self.silent:
            print(msg)  # noqa: T201
        self.logs.append(msg)
        if exc_info:
            self.logs.append(traceback.format_exc())

    def assert_instance(self, instance):
        for log in self.logs:
            if isinstance(log, instance):
                return
        msg = f"Could not find an instance of {instance}"
        raise AssertionError(msg)

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
def patch_logger(silent=True):
    class PatchedLogger(Logger):
        def info(self, msg, *args, prefix=None, extra=None, exc_info=None):
            super(PatchedLogger, self).info(msg, *args)

    from connectors_sdk.logger import logger

    new_logger = PatchedLogger(silent)

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


@pytest.fixture
def mock_responses():
    with aioresponses() as m:
        yield m
