#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from connectors.services.base import BaseService


class JobExecutionService(BaseService):
    def __init__(self, config):
        super().__init__(config)
        self.idling = self.service_config["idling"]
        self.source_list = config["sources"]
        self.connector_index = None
        self.sync_job_index = None
