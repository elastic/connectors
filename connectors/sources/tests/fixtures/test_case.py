#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from abc import ABC, abstractmethod
from collections import UserDict
from enum import Enum

from connectors.logger import logger
from connectors.sources.tests.support import ESIndexAssertionHelper


class TestState(Enum):
    SUCCESSFUL = "successful"
    FAILED = "failed"
    UNKNOWN = None


class TestResult(UserDict):
    def __init__(self, state=TestState.UNKNOWN, errors=None):
        if errors is None:
            errors = []

        super().__init__({"state": state, "errors": errors})

        self.errors = errors
        self.state = state


class TestCaseFailed(Exception):
    pass


class TestCase(ABC):
    def __init__(self, index_name, size):
        self.test_result = TestResult()
        self.size = size
        self.es_index = ESIndexAssertionHelper(index_name)

    @abstractmethod
    async def load(self):
        raise NotImplementedError

    @abstractmethod
    async def verify_load(self):
        raise NotImplementedError

    @abstractmethod
    async def remove(self):
        raise NotImplementedError

    @abstractmethod
    async def verify_remove(self):
        raise NotImplementedError

    @abstractmethod
    async def update(self):
        raise NotImplementedError

    @abstractmethod
    async def verify_update(self):
        raise NotImplementedError

    async def teardown(self):
        logger.info("Stopping test case...")
        await self.es_index.close()
        logger.info("Test case stopped successfully.")

    async def run_verifications_concurrently(self, verifications):
        try:
            results = await asyncio.gather(*verifications)
            successful, errors = map(list, zip(*results))

            self.test_result = (
                TestResult(state=TestState.SUCCESSFUL)
                if all(successful) and self.test_result.state == TestState.SUCCESSFUL and len(errors) == 0
                else TestResult(state=TestState.FAILED, errors=errors)
            )
            self.test_result.errors.extend(errors)
        except Exception as e:
            logger.error("Unknown error encountered", e)
            self.test_result.state = TestState.FAILED
            self.test_result.errors.append(e)
        finally:
            if self.test_result.state == TestState.FAILED:
                raise TestCaseFailed
