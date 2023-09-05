#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import json
import logging
import time
from contextlib import contextmanager

import pytest

import connectors.logger
from connectors.logger import ColorFormatter, logger, set_logger, tracer


@contextmanager
def unset_logger():
    old = connectors.logger.logger
    connectors.logger.logger = None
    try:
        yield
    finally:
        if old is not None:
            connectors.logger.logger = logger


def test_logger():
    with unset_logger():
        logger = set_logger(logging.DEBUG)
        assert logger.level == logging.DEBUG


def test_logger_filebeat():
    with unset_logger():
        logger = set_logger(logging.DEBUG, filebeat=True)
        logs = []

        def _w(msg):
            logs.append(msg)

        logger.handlers[0].stream.write = _w
        logger.debug("filbeat")
        ecs_log = logs[0]

        # make sure it's JSON and we have service.type
        data = json.loads(ecs_log)
        assert data["service"]["type"] == "connectors-python"


def test_tracer():
    with unset_logger():
        logger = set_logger(logging.DEBUG, filebeat=True)
        logs = []

        def _w(msg):
            logs.append(msg)

        logger.handlers[0].stream.write = _w

        @tracer.start_as_current_span("trace me")
        def traceable():
            time.sleep(0.1)

        traceable()
        ecs_log = logs[0]

        # make sure it's JSON and we have service.type
        data = json.loads(ecs_log)

        assert data["message"].startswith("[trace me] traceable took 0.1")


@pytest.mark.asyncio
async def test_async_tracer():
    with unset_logger():
        logger = set_logger(logging.DEBUG, filebeat=True)
        logs = []

        def _w(msg):
            logs.append(msg)

        logger.handlers[0].stream.write = _w

        @tracer.start_as_current_span("trace me", "special")
        async def traceable():
            time.sleep(0.1)

        await traceable()
        ecs_log = logs[0]

        # make sure it's JSON and we have service.type
        data = json.loads(ecs_log)
        assert data["message"].startswith("[trace me] special took 0.1")


@pytest.mark.asyncio
async def test_async_tracer_slow():
    with unset_logger():
        logger = set_logger(logging.DEBUG, filebeat=True)
        logs = []

        def _w(msg):
            logs.append(msg)

        logger.handlers[0].stream.write = _w

        @tracer.start_as_current_span("trace me", "special", slow_log=10)
        async def traceable():
            time.sleep(0.1)

        await traceable()
        assert (len(logs)) == 0

        @tracer.start_as_current_span("trace me", "special", slow_log=0.01)
        async def traceable_slow():
            time.sleep(0.1)

        await traceable_slow()
        assert (len(logs)) == 1


@pytest.mark.asyncio
async def test_trace_async_gen():
    with unset_logger():
        logger = set_logger(logging.DEBUG, filebeat=True)
        logs = []

        def _w(msg):
            logs.append(msg)

        logger.handlers[0].stream.write = _w

        @tracer.start_as_current_span("trace me", "special")
        async def gen():
            yield "1"
            yield "2"
            yield "3"

        async for _ in gen():
            pass

        assert len(logs) == 4

        for i, log in enumerate(logs):
            data = json.loads(log)
            assert data["message"].startswith(f"[trace me] {i}-special took")


@pytest.mark.parametrize(
    "log_level, color",
    [
        ("debug", ColorFormatter.GREY),
        ("info", ColorFormatter.GREEN),
        ("warning", ColorFormatter.YELLOW),
        ("error", ColorFormatter.RED),
        ("critical", ColorFormatter.BOLD_RED),
    ],
)
def test_colored_logging(log_level, color):
    with unset_logger():
        logger = set_logger(logging.DEBUG, filebeat=False)
        logs = []

        def _w(msg):
            logs.append(msg)

        logger.handlers[0].stream.write = _w

        getattr(logger, log_level)("foobar")

        assert len(logs) == 1
        assert logs[0].startswith(color)


def test_colored_logging_with_filebeat():
    with unset_logger():
        logger = set_logger(logging.DEBUG, filebeat=True)
        logs = []

        def _w(msg):
            logs.append(msg)

        logger.handlers[0].stream.write = _w

        logger.debug("foobar")
        logger.info("foobar")
        logger.warning("foobar")
        logger.error("foobar")
        logger.critical("foobar")

        assert len(logs) == 5
        for log in logs:
            assert not log.startswith("\x1b")
