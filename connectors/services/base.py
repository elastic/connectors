#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import time

from connectors.logger import logger
from connectors.utils import CancellableSleeps


class ServiceAlreadyRunningError(Exception):
    pass


class BaseService:
    def __init__(self, config):
        self.config = config
        self.service_config = self.config["service"]
        self.es_config = self.config["elasticsearch"]
        self.running = False
        self._sleeps = CancellableSleeps()
        self.errors = [0, time.time()]

    def stop(self):
        self.running = False
        self._sleeps.cancel()

    async def _run(self):
        raise NotImplementedError()

    async def run(self):
        if self.running:
            raise ServiceAlreadyRunningError(
                f"{self.__class__.__name__} is already running."
            )

        self.running = True
        try:
            await self._run()
        except Exception as e:
            logger.critical(e, exc_info=True)
            self.raise_if_spurious(e)
        finally:
            self.stop()

    def raise_if_spurious(self, exception):
        errors, first = self.errors
        errors += 1

        # if we piled up too many errors we raise and quit
        if errors > self.service_config["max_errors"]:
            raise exception

        # we re-init every ten minutes
        if time.time() - first > self.service_config["max_errors_span"]:
            first = time.time()
            errors = 0

        self.errors[0] = errors
        self.errors[1] = first
