#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import json
import logging
import time
from contextlib import contextmanager

import pytest

import connectors.logger
from connectors.logger import TracedAsyncGenerator, logger, set_logger, tracer


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
            time.sleep(.1)

        traceable()
        ecs_log = logs[0]

        # make sure it's JSON and we have service.type
        data = json.loads(ecs_log)

        assert data['message'].startswith(
        'trace me traceable took 0.1'
        )



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
            time.sleep(.1)

        await traceable()
        ecs_log = logs[0]

        # make sure it's JSON and we have service.type
        data = json.loads(ecs_log)
        assert data['message'].startswith(
        'trace me special took 0.1'
        )


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
            time.sleep(.1)

        await traceable()
        assert(len(logs)) == 0

        @tracer.start_as_current_span("trace me", "special", slow_log=0.01)
        async def traceable_slow():
            time.sleep(.1)

        await traceable_slow()
        assert(len(logs)) == 1


@pytest.mark.asyncio
async def test_trace_async_gen():
    with unset_logger():
        logger = set_logger(logging.DEBUG, filebeat=True)
        logs = []

        def _w(msg):
            logs.append(msg)

        logger.handlers[0].stream.write = _w

        async def gen():
            yield '1'
            yield '2'
            yield '3'

        gen = TracedAsyncGenerator(gen(), 'one')
        async for i in gen:
            pass

        assert len(logs) == 4

        ecs_log = logs[0]

        # make sure it's JSON and we have service.type
        data = json.loads(ecs_log)
        assert data['message'].startswith(
        'one took '
        )
