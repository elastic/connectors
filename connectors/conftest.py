#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import io
import sys
import os
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
def set_env():
    old = os.environ.get("elasticsearch.password")
    os.environ["elasticsearch.password"] = "password"
    try:
        yield
    finally:
        if old:
            os.environ["elasticsearch.password"] = old


@pytest.fixture
def patch_ping():
    from connectors.byoc import BYOIndex

    async def _ping(*args):
        return True

    old = BYOIndex.ping
    BYOIndex.ping = _ping
    try:
        yield
    finally:
        BYOIndex.ping = old


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

    logger._old_exception = logger.exception
    logger._old_critical = logger.critical
    logger._old_info = logger.info
    logger._old_debug = logger.debug

    logger.exception = new_logger.debug
    logger.critical = new_logger.debug
    logger.info = new_logger.debug
    logger.debug = new_logger.debug

    try:
        yield new_logger
    finally:
        logger.exception = logger._old_exception
        logger.critical = logger._old_critical
        logger.info = logger._old_info
        logger.debug = logger._old_debug


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
