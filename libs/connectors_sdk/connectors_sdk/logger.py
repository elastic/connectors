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
import sys
import time
from datetime import datetime, timezone
from functools import wraps
from typing import AsyncGenerator

import ecs_logging
from dateutil.tz import tzlocal

from connectors_sdk import __version__

logger = None


class MaxLevelFilter(logging.Filter):
    """Only lets through records strictly below ``level``.

    Used to route non-error logs to stdout while errors (and above) are
    emitted on stderr, so that redirecting stdout (e.g. ``>> logs.txt``)
    captures the regular service output.
    """

    def __init__(self, level):
        super().__init__()
        self.level = level

    def filter(self, record):
        return record.levelno < self.level


class ColorFormatter(logging.Formatter):
    GREY = "\x1b[38;20m"
    GREEN = "\x1b[32;20m"
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    BOLD_RED = "\x1b[31;1m"
    RESET = "\x1b[0m"
    COLORS = {
        logging.DEBUG: GREY,
        logging.INFO: GREEN,
        logging.WARNING: YELLOW,
        logging.ERROR: RED,
        logging.CRITICAL: BOLD_RED,
    }

    DATE_FMT = "%H:%M:%S"

    def __init__(self, prefix):
        self.custom_format = "[" + prefix + "][%(asctime)s][%(levelname)s] %(message)s"
        super().__init__(datefmt=self.DATE_FMT)
        self.local_tz = tzlocal()

    def converter(self, timestamp):
        dt = datetime.fromtimestamp(timestamp, self.local_tz)
        return dt.astimezone(timezone.utc)

    # override logging.Formatter to use an aware datetime object
    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec="milliseconds")
            except TypeError:
                s = dt.isoformat()
        return s

    def format(self, record):  # noqa: A003
        self._style._fmt = self.COLORS[record.levelno] + self.custom_format + self.RESET
        return super().format(record)


class DocumentLogger:
    def __init__(self, prefix, extra):
        self._prefix = prefix
        self._extra = extra

    def isEnabledFor(self, level):
        return logger.isEnabledFor(level)

    def debug(self, msg, *args, **kwargs):
        logger.debug(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            **kwargs,
        )

    def info(self, msg, *args, **kwargs):
        logger.info(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            **kwargs,
        )

    def warning(self, msg, *args, **kwargs):
        logger.warning(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            **kwargs,
        )

    def error(self, msg, *args, **kwargs):
        logger.error(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            **kwargs,
        )

    def exception(self, msg, *args, exc_info=True, **kwargs):
        logger.exception(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            exc_info=exc_info,
            **kwargs,
        )

    def critical(self, msg, *args, **kwargs):
        logger.critical(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            **kwargs,
        )

    def fatal(self, msg, *args, **kwargs):
        logger.fatal(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            **kwargs,
        )


class ExtraLogger(logging.Logger):
    def _log(self, level, msg, args, exc_info=None, prefix=None, extra=None):
        if (
            not (hasattr(self, "filebeat") and self.filebeat)  # pyright: ignore
            and prefix
        ):
            msg = f"{prefix} {msg}"

        if extra is None:
            extra = {}
        extra.update(
            {
                "service.type": "connectors-python",
                "service.version": __version__,
            }
        )
        super(ExtraLogger, self)._log(level, msg, args, exc_info, extra)


def _build_formatter(prefix, filebeat):
    if filebeat:
        return ecs_logging.StdlibFormatter()
    return ColorFormatter(prefix)


def _stdout_handler():
    """Handler for non-error logs, so they can be piped via stdout redirection."""
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(MaxLevelFilter(logging.ERROR))
    return handler


def _stderr_handler():
    """Handler for error logs (ERROR and above), kept on stderr."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.ERROR)
    return handler


def set_logger(log_level=logging.INFO, filebeat=False):
    global logger
    formatter = _build_formatter("FMWK", filebeat)

    if logger is None:
        logging.setLoggerClass(ExtraLogger)
        logger = logging.getLogger("connectors")
        logger.handlers.clear()
        logger.addHandler(_stdout_handler())
        logger.addHandler(_stderr_handler())

    logger.propagate = False
    logger.setLevel(log_level)
    # Regular logs (below ERROR) follow the configured level and go to stdout;
    # the stderr handler stays pinned at ERROR so errors are always visible there.
    stdout_handler, stderr_handler = logger.handlers[0], logger.handlers[1]
    stdout_handler.setLevel(log_level)
    stdout_handler.setFormatter(formatter)
    stderr_handler.setFormatter(formatter)
    logger.filebeat = filebeat  # pyright: ignore
    return logger


def set_extra_logger(logger, log_level=logging.INFO, prefix="BYOC", filebeat=False):
    if isinstance(logger, str):
        logger = logging.getLogger(logger)
    formatter = _build_formatter(prefix, filebeat)

    stdout_handler = _stdout_handler()
    stdout_handler.setFormatter(formatter)
    stdout_handler.setLevel(log_level)

    stderr_handler = _stderr_handler()
    stderr_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)
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
