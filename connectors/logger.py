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
from datetime import datetime, timezone
from functools import wraps
from types import TracebackType
from typing import AsyncGenerator, Mapping, Optional, Tuple, Type, Union

import ecs_logging
from dateutil.tz import tzlocal

from connectors import __version__

logger: logging.Logger = logging.getLogger("connectors")
logger_initialized = False


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

    def __init__(self, prefix) -> None:
        self.custom_format = "[" + prefix + "][%(asctime)s][%(levelname)s] %(message)s"
        super().__init__(datefmt=self.DATE_FMT)
        self.local_tz = tzlocal()

    def converter(self, timestamp: float) -> datetime:
        dt = datetime.fromtimestamp(timestamp, self.local_tz)
        return dt.astimezone(timezone.utc)

    # override logging.Formatter to use an aware datetime object
    def formatTime(
        self, record: logging.LogRecord, datefmt: Optional[str] = None
    ) -> str:
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec="milliseconds")
            except TypeError:
                s = dt.isoformat()
        return s

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        self._style._fmt = self.COLORS[record.levelno] + self.custom_format + self.RESET
        return super().format(record)


class DocumentLogger:
    def __init__(self, prefix, extra) -> None:
        self._prefix = prefix
        self._extra = extra

    def isEnabledFor(self, level):
        return logger.isEnabledFor(level)

    def debug(self, msg, *args, **kwargs) -> None:
        logger.debug(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            **kwargs,
        )

    def info(self, msg, *args, **kwargs) -> None:
        logger.info(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            **kwargs,
        )

    def warning(self, msg, *args, **kwargs) -> None:
        logger.warning(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            **kwargs,
        )

    def error(self, msg, *args, **kwargs) -> None:
        logger.error(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            **kwargs,
        )

    def exception(self, msg, *args, exc_info: bool = True, **kwargs) -> None:
        logger.exception(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            exc_info=exc_info,
            **kwargs,
        )

    def critical(self, msg, *args, **kwargs) -> None:
        logger.critical(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            **kwargs,
        )

    def fatal(self, msg, *args, **kwargs) -> None:
        logger.fatal(
            msg,
            *args,
            prefix=self._prefix,  # pyright: ignore
            extra=self._extra,
            **kwargs,
        )


class ExtraLogger(logging.Logger):
    def _log(
        self,
        level: int,
        msg: str,
        args: Union[Mapping[str, object], Tuple[object, ...]],
        exc_info: Union[
            None,
            BaseException,
            bool,
            Tuple[Type[BaseException], BaseException, Optional[TracebackType]],
            Tuple[None, ...],
        ] = None,
        prefix=None,
        extra: Optional[Mapping[str, object]] = None,
    ) -> None:
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


def set_logger(log_level: int = logging.INFO, filebeat: bool = False):
    global logger
    global logger_initialized
    if filebeat:
        formatter = ecs_logging.StdlibFormatter()
    else:
        formatter = ColorFormatter("FMWK")

    if not logger_initialized:
        logging.setLoggerClass(ExtraLogger)
        logger = logging.getLogger("connectors")
        logger.handlers.clear()
        handler = logging.StreamHandler()
        logger.addHandler(handler)
        logger_initialized = True

    logger.propagate = False
    logger.setLevel(log_level)
    logger.handlers[0].setLevel(log_level)
    logger.handlers[0].setFormatter(formatter)
    logger.filebeat = filebeat  # pyright: ignore
    return logger


def set_extra_logger(
    logger: Union[str, logging.Logger],
    log_level: Union[int, str] = logging.INFO,
    prefix: str = "BYOC",
    filebeat: bool = False,
) -> None:
    if isinstance(logger, str):
        logger = logging.getLogger(logger)
    handler = logging.StreamHandler()
    if filebeat:
        handler.setFormatter(ecs_logging.StdlibFormatter())
    else:
        formatter = ColorFormatter(prefix)
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
    def __init__(self, generator, name, func_name, slow_log=None) -> None:
        self.gen = generator
        self.name = name
        self.slow_log = slow_log
        self.func_name = func_name
        self.counter = 0

    def __aiter__(self) -> "_TracedAsyncGenerator":
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
