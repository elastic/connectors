#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from functools import cached_property

from connectors.protocol import JobStatus, JobType
from connectors.services.job_execution import JobExecutionService
from typing import Dict, List, Union
from unittest.mock import Mock


class ContentSyncJobExecutionService(JobExecutionService):
    name = "sync_content"

    def __init__(self, config: Dict[str, Union[List[Dict[str, str]], str, Dict[str, Union[str, bool, Dict[str, Union[int, bool, Dict[str, Union[bool, int, float]]]], int]], Dict[str, Union[float, int, str]], Dict[str, str]]]) -> None:
        super().__init__(config, "content_sync_job_execution_service")

    @cached_property
    def display_name(self):
        return "content sync job execution"

    @cached_property
    def max_concurrency_config(self):
        return "service.max_concurrent_content_syncs"

    @cached_property
    def job_types(self):
        return [JobType.FULL.value, JobType.INCREMENTAL.value]

    @cached_property
    def max_concurrency(self):
        return self.service_config.get("max_concurrent_content_syncs")

    def should_execute(self, connector: Mock, sync_job: Mock) -> bool:
        if connector.last_sync_status == JobStatus.IN_PROGRESS:
            sync_job.log_debug("Connector is still syncing content, skip the job...")
            return False

        return True
