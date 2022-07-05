#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import logging
from connectors.logger import logger, set_logger


def test_logger():
    set_logger(logging.DEBUG)
    assert logger.level == logging.DEBUG
