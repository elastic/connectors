#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.logger import logger


class BaseService:
    def __init__(self, config):
        self.config = config

    def stop(self):
        raise NotImplementedError()

    async def run(self):
        raise NotImplementedError()

    def shutdown(self, sig):
        logger.info(f"Caught {sig.name}. Graceful shutdown.")
        self.stop()
