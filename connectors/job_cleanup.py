#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
A task periodically clean up orphaned and stuck jobs.
"""
import asyncio
from connectors.logger import logger


class JobCleanUp:
    def __init__(
        self,
        connectors,
        native_service_types=[],
        connector_ids=[],
        interval=300,
        stuck_threshold=60,
    ):
        self.connectors = connectors
        self.native_service_types = native_service_types.copy()
        self.connector_ids = connector_ids
        self.interval = interval
        self.stuck_threshold = stuck_threshold
        self.running = False
        self.task = None

    def start(self):
        if self.running:
            return
        self.running = True
        self.task = asyncio.create_task(self._cleanup())
        logger.info("Successfully started Job cleanup task...")

    def shutdown(self):
        if not self.running:
            return
        self.running = False
        if self.task is not None:
            self.task.cancel()
        logger.info("Successfully shut down Job cleanup task...")

    async def _cleanup(self):
        while self.running:
            logger.debug(
                f"Start job clean up for native connectors {self.native_service_types} and connector IDs {self.connector_ids}..."
            )
            await self._process_orphaned_jobs()
            await self._process_stuck_jobs()
            await asyncio.sleep(self.interval)

    async def _process_orphaned_jobs(self):
        logger.debug('Start cleaning up orphaned jobs...')
        orphaned_jobs = [job async for job in self.connectors.orphaned_jobs(self.native_service_types, self.connector_ids)]
        if len(orphaned_jobs) == 0:
            logger.debug('No orphaned jobs found. Skipping...')
            return

        # delete content indices in case they are re-created by sync job
        content_indices = list(map(lambda x: x.index_name, orphaned_jobs))
        if len(content_indices) > 0:
            self.connectors.indices.delete(content_indices)
        result = ConnectorJob.delete_jobs(orphaned_jobs)
        if len(result["failures"]) > 0:
            logger.error(f"Error found when deleting jobs: {result['failures']}")
        logger.info(f"Successfully deleted {result['deleted']} out of {result['total']} orphaned jobs.")

    async def _process_stuck_jobs(self):
        pass
