#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Logger -- sets the logging and provides a `logger` global object.
"""
import contextlib
import inspect
import logging
import time
from datetime import datetime
from functools import wraps
from typing import AsyncGenerator

import ecs_logging

from connectors import __version__

logger = None


def _formatter(prefix):
    return logging.Formatter(
        fmt="[" + prefix + "][%(asctime)s][%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


class ExtraLogger(logging.Logger):
    def _log(self, level, msg, args, exc_info=None, extra=None):
        if extra is None:
            extra = {}
        extra.update(
            {
                "service.type": "connectors-python",
                "service.version": __version__,
                "labels.index_date": datetime.now().strftime("%Y.%m.%d"),
            }
        )
        super(ExtraLogger, self)._log(level, msg, args, exc_info, extra)


def set_logger(log_level=logging.INFO, filebeat=False):
    global logger
    if filebeat:
        formatter = ecs_logging.StdlibFormatter()
    else:
        formatter = _formatter("FMWK")

    if logger is None:
        logging.setLoggerClass(ExtraLogger)
        logger = logging.getLogger("connectors")
        logger.handlers.clear()
        handler = logging.StreamHandler()
        logger.addHandler(handler)

    logger.propagate = False
    logger.setLevel(log_level)
    logger.handlers[0].setLevel(log_level)
    logger.handlers[0].setFormatter(formatter)
    logger.filebeat = filebeat  # pyright: ignore
    return logger


def set_extra_logger(logger, log_level=logging.INFO, prefix="BYOC", filebeat=False):
    if isinstance(logger, str):
        logger = logging.getLogger(logger)
    handler = logging.StreamHandler()
    if filebeat:
        handler.setFormatter(ecs_logging.StdlibFormatter())
    else:
        formatter = _formatter(prefix)
        handler.setFormatter(formatter)
    handler.setLevel(log_level)
    logger.addHandler(handler)
    logger.setLevel(log_level)


#
# metrics APIs that follow the open-telemetry APIs
#
# For now everything is dropped into the logger, but later on
# we will use the otel API and exporter


@contextlib.contextmanager
def timed_execution(name, func_name, slow_log=None, canceled=None):
    """Context manager to log time execution in DEBUG

    - name: prefix used for the log message
    - func_name: additional prefix for the function name
    - slow_log: if given a treshold time in seconds. if it runs faster, no log
      is emited
    - canceled: if provided a callable to cancel the timer. Used in nested
      calls.
    """
    start = time.time()
    try:
        yield
    finally:
        do_not_track = canceled is not None and canceled()
        if not do_not_track:
            delta = time.time() - start
            if slow_log is None or (slow_log is not None and delta > slow_log):
                logger.debug(  # pyright: ignore
                    f"[{name}] {func_name} took {delta} seconds."
                )


class _TracedAsyncGenerator:
    def __init__(self, generator, name, func_name, slow_log=None):
        self.gen = generator
        self.name = name
        self.slow_log = slow_log
        self.func_name = func_name
        self.counter = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            with timed_execution(
                self.name, f"{self.counter}-{self.func_name}", self.slow_log
            ):
                return await self.gen.__anext__()
        finally:
            self.counter += 1


class CustomTracer:
    # spans as decorators, https://opentelemetry.io/docs/instrumentation/python/manual/#creating-spans-with-decorators
    def start_as_current_span(self, name, func_name=None, slow_log=None):
        def _wrapped(func):
            if inspect.iscoroutinefunction(func):

                @wraps(func)
                async def _awrapped(*args, **kw):
                    nonlocal func_name
                    nonlocal slow_log

                    if func_name is None:
                        func_name = func.__name__

                    with timed_execution(name, func_name, slow_log):
                        return await func(*args, **kw)

                return _awrapped

            else:

                @wraps(func)
                def __wrapped(*args, **kw):
                    nonlocal func_name

                    if func_name is None:
                        func_name = func.__name__

                    _canceled = False

                    def canceled():
                        return _canceled

                    with timed_execution(name, func_name, slow_log, canceled):
                        res = func(*args, **kw)

                        # if it's an async generator we return it wrapped
                        if isinstance(res, AsyncGenerator):
                            _canceled = True
                            return _TracedAsyncGenerator(res, name, func_name, slow_log)
                        return res

                return __wrapped

        return _wrapped


tracer = CustomTracer()
set_logger()
