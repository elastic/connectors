#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import logging
import json

import connectors.logger
from connectors.logger import logger, set_logger


def test_logger():
    connectors.logger.logger = None
    set_logger(logging.DEBUG)
    assert logger.level == logging.DEBUG


def test_logger_filebeat():
    connectors.logger.logger = None
    logger = set_logger(logging.DEBUG, filebeat=True)
    logs = []

    def _w(msg):
        logs.append(msg)

    logger.handlers[0].stream.write = _w
    logger.debug("filbeat")
    ecs_log = logs[0]

    # make sure it's JSON and we have service.name
    data = json.loads(ecs_log)
    assert data["service"]["name"] == "connectors-python"
