#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import json
import logging
from contextlib import contextmanager

import connectors.logger
from connectors.logger import logger, set_logger


@contextmanager
def unset_logger():
    old = connectors.logger.logger
    connectors.logger.logger = None
    try:
        yield
    finally:
        if old is not None:
            connectors.logger.logger = logger


def test_logger():
    with unset_logger():
        logger = set_logger(logging.DEBUG)
        assert logger.level == logging.DEBUG


def test_logger_filebeat():
    with unset_logger():
        logger = set_logger(logging.DEBUG, filebeat=True)
        logs = []

        def _w(msg):
            logs.append(msg)

        logger.handlers[0].stream.write = _w
        logger.debug("filbeat")
        ecs_log = logs[0]

        # make sure it's JSON and we have service.type
        data = json.loads(ecs_log)
        assert data["service"]["type"] == "connectors-python"
