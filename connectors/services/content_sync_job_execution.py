#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from functools import cached_property

from connectors.logger import logger
from connectors.protocol import JobStatus, JobType
from connectors.services.job_execution import JobExecutionService

DEFAULT_MAX_CONCURRENT_CONTENT_SYNCS = 1


class ContentSyncJobExecutionService(JobExecutionService):
    name = "sync_content"
    display_name = "content sync job execution"
    JOB_TYPES = [JobType.FULL.value, JobType.INCREMENTAL.value]

    @cached_property
    def max_concurrency(self):
        max_concurrent_content_syncs = self.service_config.get(
            "max_concurrent_content_syncs"
        )

        if max_concurrent_content_syncs is not None:
            return max_concurrent_content_syncs

        if "max_concurrent_syncs" in self.service_config:
            logger.warning(
                "'max_concurrent_syncs' is deprecated. Use 'max_concurrent_content_syncs' in 'config.yml'."
            )

        # keep for backwards compatibility
        return self.service_config.get(
            "max_concurrent_syncs", DEFAULT_MAX_CONCURRENT_CONTENT_SYNCS
        )

    def should_execute(self, connector, sync_job):
        if connector.last_sync_status == JobStatus.IN_PROGRESS:
            sync_job.log_debug("Connector is still syncing content, skip the job...")
            return False

        return True
