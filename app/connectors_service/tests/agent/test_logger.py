#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import logging

import pytest

from connectors.agent.logger import root_logger, update_logger_level


@pytest.mark.parametrize(
    "log_level, expected",
    [
        ("debug", logging.DEBUG),
        ("Warning", logging.WARNING),
        ("ERROR", logging.ERROR),
        (logging.CRITICAL, logging.CRITICAL),
    ],
)
def test_update_logger_level_accepts_case_insensitive_level(log_level, expected):
    original_level = root_logger.level
    try:
        update_logger_level(log_level)
        assert root_logger.level == expected
    finally:
        root_logger.setLevel(original_level)
