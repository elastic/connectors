#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Event loop

- polls for work by calling Elasticsearch on a regular basis
- instanciates connector plugins
- mirrors an Elasticsearch index with a collection of documents
"""
import asyncio
import os
import time

from connectors.byoc import (
    SyncJobIndex
)
from connectors.logger import logger
from connectors.services.base import BaseService
from connectors.utils import CancellableSleeps, trace_mem


class JobService(BaseService):
    def __init__(self, args):
        super().__init__(args)
        self.running = False
        self._sleeps = CancellableSleeps()
        self.jobs = SyncJobIndex(self.config["elasticsearch"])

    async def run(self):
        self.running = True

        try:
            while self.running:
                if "connector_id" in self.config:
                    connectors_ids = [self.config.get("connector_id")]
                else:
                    connectors_ids = []

                query = self.jobs.build_docs_query(connectors_ids)
                async for job in self.jobs.get_all_docs(query=query):
                    print(f"Fetched a job {job}")

                await self._sleeps.sleep(10)
        finally:
            self.stop()
        return 0

    def stop(self):
        logger.debug("Shutting down consumers")
        self.running = False
        self._sleeps.cancel()
