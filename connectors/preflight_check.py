#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio

from connectors.byoc import CONNECTORS_INDEX, JOBS_INDEX
from connectors.es import ESClient
from connectors.logger import logger


async def pre_flight_check(config):
    elastic_config = config["elasticsearch"]
    es_host = elastic_config["host"]

    es_client = ESClient(elastic_config)

    if not (await es_client.wait()):
        logger.critical(f"{es_host} seem down. Bye!")
        return -1

    service_config = config["service"]
    preflight_max_attempts = int(service_config.get("preflight_max_attempts", 10))
    preflight_idle = int(service_config.get("preflight_idle", 30))

    try:
        attempts = 0
        while True:
            logger.info("Preflight checks...")
            try:
                # Checking the indices/pipeline in the loop to be less strict about the boot ordering
                await es_client.check_exists(indices=[CONNECTORS_INDEX, JOBS_INDEX])
                break
            except Exception as e:
                if attempts > preflight_max_attempts:
                    raise
                else:
                    logger.warn(
                        f"Attempt {attempts + 1}/{preflight_max_attempts} failed. Retrying..."
                    )
                    logger.warn(str(e))
                    attempts += 1
                    await asyncio.sleep(preflight_idle)
    finally:
        if es_client is not None:
            es_client.stop_waiting()
            await es_client.close()
