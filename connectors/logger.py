#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Logger -- sets the logging and provides a `logger` global object.
"""
import logging

logger = None


def set_logger(log_level=logging.INFO):
    global logger
    if logger is None:
        logger = logging.getLogger("connectors")
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="[%(asctime)s][%(levelname)s] %(message)s", datefmt="%H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.propagate = False
    logger.setLevel(log_level)
    logger.handlers[0].setLevel(log_level)
    return logger


set_logger()
