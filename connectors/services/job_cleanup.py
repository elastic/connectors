#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
A task periodically clean up orphaned and idle jobs.
"""

from connectors.es.index import DocumentNotFoundError
from connectors.es.management_client import ESManagementClient
from connectors.logger import logger
from connectors.protocol import ConnectorIndex, SyncJobIndex
from connectors.services.base import BaseService

IDLE_JOB_ERROR = "The job has not seen any update for some time."


class JobCleanUpService(BaseService):
    name = "cleanup"

    def __init__(self, config):
        super().__init__(config)
        self.idling = int(self.service_config.get("job_cleanup_interval", 60 * 5))
        self.native_service_types = self.config.get("native_service_types", []) or []
        self.connector_ids = list(self.connectors.keys())

    async def _run(self):
        logger.debug("Successfully started Job cleanup task...")
        self.connector_index = ConnectorIndex(self.es_config)
        self.es_management_client = ESManagementClient(self.es_config)
        self.sync_job_index = SyncJobIndex(self.es_config)

        try:
            while self.running:
                await self._process_orphaned_idle_jobs()
                await self._process_idle_jobs()
                await self._sleeps.sleep(self.idling)
        finally:
            if self.connector_index is not None:
                self.connector_index.stop_waiting()
                await self.connector_index.close()
            if self.sync_job_index is not None:
                self.sync_job_index.stop_waiting()
                await self.sync_job_index.close()
        return 0

    async def _process_orphaned_idle_jobs(self):
        try:
            logger.debug("Cleaning up orphaned idle jobs")
            connector_ids = [
                connector.id
                async for connector in self.connector_index.all_connectors()
            ]

            marked_count = total_count = 0
            async for job in self.sync_job_index.orphaned_idle_jobs(
                connector_ids=connector_ids
            ):
                try:
                    await job.fail(IDLE_JOB_ERROR)
                    marked_count += 1
                except Exception as e:
                    logger.error(
                        f"Failed to mark orphaned idle job #{job.id} as error: {e}"
                    )
                finally:
                    total_count += 1

            if total_count == 0:
                logger.debug("No orphaned idle jobs found. Skipping...")
            else:
                logger.info(
                    f"Successfully marked #{marked_count} out of #{total_count} orphaned idle jobs as error."
                )
        except Exception as e:
            logger.critical(e, exc_info=True)
            self.raise_if_spurious(e)

    async def _process_idle_jobs(self):
        try:
            logger.debug("Start cleaning up idle jobs...")
            connector_ids = [
                connector.id
                async for connector in self.connector_index.supported_connectors(
                    native_service_types=self.native_service_types,
                    connector_ids=self.connector_ids,
                )
            ]

            marked_count = total_count = 0
            async for job in self.sync_job_index.idle_jobs(connector_ids=connector_ids):
                job_id = job.id
                try:
                    connector_id = job.connector_id

                    await job.fail(IDLE_JOB_ERROR)
                    marked_count += 1

                    try:
                        connector = await self.connector_index.fetch_by_id(
                            doc_id=connector_id
                        )
                    except DocumentNotFoundError:
                        logger.warning(
                            f"Could not found connector by id #{connector_id}"
                        )
                        continue

                    try:
                        await job.reload()
                    except DocumentNotFoundError:
                        logger.warning(f"Could not reload sync job #{job_id}")
                        job = None
                    await connector.sync_done(job=job)
                except Exception as e:
                    logger.error(f"Failed to mark idle job #{job_id} as error: {e}")
                finally:
                    total_count += 1

            if total_count == 0:
                logger.debug("No idle jobs found. Skipping...")
            else:
                logger.info(
                    f"Successfully marked #{marked_count} out of #{total_count} idle jobs as error."
                )
        except Exception as e:
            logger.critical(e, exc_info=True)
            self.raise_if_spurious(e)
