#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import logging
import sys

import ecs_logging


class _MaxLevelFilter(logging.Filter):
    """Only lets through records strictly below ``level``."""

    def __init__(self, level):
        super().__init__()
        self.level = level

    def filter(self, record):
        return record.levelno < self.level


_formatter = ecs_logging.StdlibFormatter()

# Regular logs go to stdout (so they can be piped via stdout redirection),
# while errors (ERROR and above) are emitted on stderr.
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(_formatter)
stdout_handler.addFilter(_MaxLevelFilter(logging.ERROR))

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setFormatter(_formatter)
stderr_handler.setLevel(logging.ERROR)

root_logger = logging.getLogger("agent_component")
root_logger.addHandler(stdout_handler)
root_logger.addHandler(stderr_handler)
root_logger.setLevel(logging.INFO)


def get_logger(module):
    logger = root_logger.getChild(module)

    if logger.hasHandlers():
        return logger

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)

    return logger


def update_logger_level(log_level):
    root_logger.setLevel(log_level)
