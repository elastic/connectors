#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Logger -- sets the logging and provides a `logger` global object.
"""
import logging
import ecs_logging

logger = None


def _formatter(prefix):
    return logging.Formatter(
        fmt="[" + prefix + "][%(asctime)s][%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def set_logger(log_level=logging.INFO, filebeat=False):
    global logger
    if filebeat:
        formatter = ecs_logging.StdlibFormatter()
    else:
        formatter = _formatter("FMWK")

    if logger is None:
        logger = logging.getLogger("connectors")
        handler = logging.StreamHandler()
        logger.addHandler(handler)

    logger.propagate = False
    logger.setLevel(log_level)
    logger.handlers[0].setLevel(log_level)
    logger.handlers[0].setFormatter(formatter)
    logger.filebeat = filebeat
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


set_logger()
