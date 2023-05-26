#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Logger -- sets the logging and provides a `logger` global object.
"""
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
    def start_as_current_span(self, name):
        def _wrapped(func):
            @wraps(func)
            def __wrapped(*args, **kw):
                start = time.time()
                try:
                    return func(*args, **kw)
                finally:
                    delta = time.time() - start
                    logger.debug(f"{name} {func.__name__} took {delta} seconds.")

            return __wrapped

        return _wrapped


tracer = CustomTracer()


set_logger()
