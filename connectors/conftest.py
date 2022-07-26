#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import io
import sys
import pytest
import asyncio
from aioresponses import aioresponses


class Logger:
    def __init__(self, silent=True):
        self.logs = []
        self.silent = silent

    def debug(self, msg, exc_info=True):
        if not self.silent:
            print(msg)
        self.logs.append(msg)

    exception = critical = info = debug


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
    logger = Logger(silent)

    from connectors import runner
    from connectors import byoc
    from connectors import byoei

    old_logger = runner.logger
    byoei.logger = runner.logger = byoc.logger = logger
    try:
        yield logger
    finally:
        byoei.logger = runner.logger = byoc.logger = old_logger


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_responses():
    with aioresponses() as m:
        yield m
