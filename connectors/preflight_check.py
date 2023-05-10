#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from connectors.es import ESClient
from connectors.logger import logger
from connectors.protocol import CONNECTORS_INDEX, JOBS_INDEX
from connectors.utils import CancellableSleeps


class PreflightCheck:
    def __init__(self, config):
        self.elastic_config = config["elasticsearch"]
        self.service_config = config["service"]
        self.es_client = ESClient(self.elastic_config)
        self.preflight_max_attempts = int(
            self.service_config.get("preflight_max_attempts", 10)
        )
        self.preflight_idle = int(self.service_config.get("preflight_idle", 30))
        self._sleeps = CancellableSleeps()
        self.running = False

    def stop(self):
        self.running = False
        self._sleeps.cancel()
        if self.es_client is not None:
            self.es_client.stop_waiting()

    def shutdown(self, sig):
        logger.info(f"Caught {sig.name}. Graceful shutdown.")
        self.stop()

    async def run(self):
        try:
            logger.info("Preflight checks...")
            self.running = True
            if not (await self.es_client.wait()):
                logger.critical(f"{self.elastic_config['host']} seem down. Bye!")
                return False

            return await self._check_system_indices_with_retries()
        finally:
            self.stop()
            if self.es_client is not None:
                await self.es_client.close()

    async def _check_system_indices_with_retries(self):
        attempts = 0
        while self.running:
            try:
                # Checking the indices/pipeline in the loop to be less strict about the boot ordering
                await self.es_client.check_exists(
                    indices=[CONNECTORS_INDEX, JOBS_INDEX]
                )
                return True
            except Exception as e:
                if attempts >= self.preflight_max_attempts:
                    logger.critical(
                        f"Could not perform preflight check after {self.preflight_max_attempts} retries."
                    )
                    return False
                else:
                    logger.warning(
                        f"Attempt {attempts + 1}/{self.preflight_max_attempts} failed. Retrying..."
                    )
                    logger.warning(str(e))
                    attempts += 1
                    await self._sleeps.sleep(self.preflight_idle)
        return False
