#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Logger -- sets the logging and provides a `logger` global object.
"""
import inspect
import logging
import time
from datetime import datetime
from functools import wraps

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
class CustomTracer:
    # spans as decorators, https://opentelemetry.io/docs/instrumentation/python/manual/#creating-spans-with-decorators
    def start_as_current_span(self, name, func_name=None, slow_log=None):
        def _wrapped(func):
            if inspect.iscoroutinefunction(func):

                @wraps(func)
                async def _awrapped(*args, **kw):
                    nonlocal func_name
                    nonlocal slow_log

                    start = time.time()
                    try:
                        return await func(*args, **kw)
                    finally:
                        delta = time.time() - start

                        if slow_log is None or (
                            slow_log is not None and delta > slow_log
                        ):
                            if func_name is None:
                                func_name = func.__name__
                            logger.debug(f"{name} {func_name} took {delta} seconds.")

                return _awrapped

            else:

                @wraps(func)
                def __wrapped(*args, **kw):
                    nonlocal func_name

                    start = time.time()
                    try:
                        return func(*args, **kw)
                    finally:
                        delta = time.time() - start
                        if slow_log is None or (
                            slow_log is not None and delta > slow_log
                        ):
                            if func_name is None:
                                func_name = func.__name__
                            logger.debug(f"{name} {func_name} took {delta} seconds.")

                return __wrapped

        return _wrapped


tracer = CustomTracer()


class TracedAsyncGenerator:
    def __init__(self, generator, name):
        self.gen = generator
        self.name = name

    def __aiter__(self):
        return self

    async def __anext__(self):
        start = time.time()
        try:
            return await self.gen.__anext__()
        finally:
            delta = time.time() - start
            logger.debug(f"{self.name} took {delta} seconds.")


set_logger()
