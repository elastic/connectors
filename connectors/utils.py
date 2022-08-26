#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime
import logging
import time
import asyncio

from elasticsearch import AsyncElasticsearch

from connectors.logger import set_extra_logger, logger
from connectors.quartz import QuartzCron


class ESClient:
    def __init__(self, config):
        self.host = config["host"]
        self._sleeps = CancellableSleeps()
        options = {
            "hosts": [config["host"]],
            "request_timeout": config.get("request_timeout", 120),
        }
        if "username" in config:
            if "api_key" in config:
                raise KeyError("You can't use basic auth and Api Key at the same time")
            auth = config["username"], config["password"]
            options["basic_auth"] = auth
            logger.debug(
                f"Connecting using Basic Auth (user: {config['username']}, password: {config['password'][:3]}...)"
            )
        elif "api_key" in config:
            logger.debug(f"Connecting with an Api Key ({config['api_key'][:5]}...)")
            options["api_key"] = config["api_key"]

        if config.get("ssl", False):
            options["verify_certs"] = True
            if "ca_certs" in config:
                ca_certs = config["ca_certs"]
                logger.debug(f"Verifying cert with {ca_certs}")
                options["ca_certs"] = ca_certs

        level = config.get("log_level", "INFO").upper()
        es_logger = logging.getLogger("elastic_transport.node")
        set_extra_logger(
            es_logger, log_level=logging.getLevelName(level), filebeat=logger.filebeat
        )
        self.max_wait_duration = config.get("max_wait_duration", 60)
        self.initial_backoff_duration = config.get("initial_backoff_duration", 5)
        self.backoff_multiplier = config.get("backoff_multiplier", 2)
        self.client = AsyncElasticsearch(**options)
        self._keep_waiting = True

    def stop_waiting(self):
        self._keep_waiting = False
        self._sleeps.cancel()

    async def close(self):
        await self.client.close()

    async def ping(self):
        return await self.client.ping()

    async def wait(self):
        backoff = self.initial_backoff_duration
        start = time.time()
        logger.debug(f"Wait for Elasticsearch (max: {self.max_wait_duration})")
        while time.time() - start < self.max_wait_duration:
            if not self._keep_waiting:
                return False
            logger.info(
                f"Waiting for {self.host} (so far: {int(time.time() - start)} secs)"
            )
            if await self.ping():
                return True
            await self._sleeps.sleep(backoff)
            backoff *= self.backoff_multiplier

        await self.close()
        return False


def iso_utc(when=None):
    if when is None:
        when = datetime.utcnow()
    return when.isoformat()


def next_run(quartz_definition):
    """Returns the number of seconds before the next run."""
    cron_obj = QuartzCron(quartz_definition, datetime.utcnow())
    when = cron_obj.next_trigger()
    now = datetime.utcnow()
    secs = (when - now).total_seconds()
    if secs < 1.0:
        secs = 0
    return secs


class CancellableSleeps:
    def __init__(self):
        self._sleeps = set()

    async def sleep(self, delay, result=None, *, loop=None):
        async def _sleep(delay, result=None, *, loop=None):
            coro = asyncio.sleep(delay, result=result)
            task = asyncio.ensure_future(coro)
            self._sleeps.add(task)
            try:
                return await task
            except asyncio.CancelledError:
                logger.debug("Sleep canceled")
                return result
            finally:
                self._sleeps.remove(task)

        await _sleep(delay, result=result, loop=loop)

    def cancel(self):
        for task in self._sleeps:
            task.cancel()
