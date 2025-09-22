#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import logging

import ecs_logging
from typing import Union, TextIO

root_logger: logging.Logger = logging.getLogger("agent_component")
handler: logging.StreamHandler[TextIO] = logging.StreamHandler()
handler.setFormatter(ecs_logging.StdlibFormatter())
root_logger.addHandler(handler)
root_logger.setLevel(logging.INFO)


def get_logger(module: str) -> logging.Logger:
    logger = root_logger.getChild(module)

    if logger.hasHandlers():
        return logger

    logger.addHandler(handler)

    return logger


def update_logger_level(log_level: Union[int, str]) -> None:
    root_logger.setLevel(log_level)
