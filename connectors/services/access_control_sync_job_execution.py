#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from functools import cached_property

from connectors.protocol import JobStatus, JobType
from connectors.services.job_execution import JobExecutionService


class AccessControlSyncJobExecutionService(JobExecutionService):
    name = "sync_access_control"

    @cached_property
    def display_name(self):
        return "access control sync job execution"

    @cached_property
    def max_concurrency_config(self):
        return "service.max_concurrent_access_control_syncs"

    @cached_property
    def job_types(self):
        return JobType.ACCESS_CONTROL.value

    @cached_property
    def max_concurrency(self):
        return self.service_config.get("max_concurrent_access_control_syncs")

    def should_execute(self, connector, sync_job):
        if not connector.features.document_level_security_enabled():
            sync_job.log_debug("DLS is not enabled for the connector, skip the job...")
            return False

        if connector.last_access_control_sync_status == JobStatus.IN_PROGRESS:
            sync_job.log_debug(
                "Connector is still syncing access control, skip the job..."
            )
            return False

        return True
