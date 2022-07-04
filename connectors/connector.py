#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Connector definition
"""
from crontab import CronTab
from datetime import datetime


class Connector:
    def __init__(self, elastic_server, definition):
        self.definition = definition
        self.service_type = definition["service_type"]
        self.index_name = definition["index_name"]
        self.configuration = {}
        for key, value in definition["configuration"].items():
            self.configuration[key] = value["value"]
        self.scheduling = definition["scheduling"]
        self.elastic_server = elastic_server

    def json(self):
        return self.definition

    async def _write(self):
        await self.elastic_server.save_connector(self.definition)

    def next_sync(self):
        """Returns in seconds when the next sync should happen.

        If the function returns -1, no sync is scheduled.
        """
        if self.definition["sync_now"]:
            return 0
        if not self.scheduling["enabled"]:
            return -1
        return CronTab(self.scheduling["interval"]).next(default_utc=True)

    async def sync_starts(self):
        self.definition["sync_now"] = False
        await self._write()

    async def sync_done(self):
        await self._write()
