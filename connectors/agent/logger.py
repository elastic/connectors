#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import logging

import ecs_logging


def get_logger(module, log_level=logging.INFO):
    logger = logging.getLogger(module)

    if logger.hasHandlers():
        return logger

    handler = logging.StreamHandler()
    handler.setFormatter(ecs_logging.StdlibFormatter())

    handler.setLevel(log_level)
    logger.addHandler(handler)
    logger.setLevel(log_level)

    return logger
