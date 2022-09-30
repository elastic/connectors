#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime, timezone
import logging
import time
import asyncio

from elasticsearch import (
    AsyncElasticsearch,
    ApiError,
    ConnectionError as ElasticConnectionError,
    NotFoundError,
)
from elastic_transport.client_utils import url_to_node_config

from connectors.logger import set_extra_logger, logger
from connectors.quartz import QuartzCron


class ESClient:
    def __init__(self, config):
        self.config = config
        self.host = url_to_node_config(
            config.get("host", "http://localhost:9200"),
            use_default_ports_for_scheme=True,
        )
        self._sleeps = CancellableSleeps()
        options = {
            "hosts": [self.host],
            "request_timeout": config.get("request_timeout", 120),
            "retry_on_timeout": config.get("retry_on_timeout", True),
        }
        logger.debug(f"Host is {self.host}")

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
        try:
            await self.client.info()
        except ApiError as e:
            logger.error(f"The server returned a {e.status_code} code")
            if e.info is not None and "error" in e.info and "reason" in e.info["error"]:
                logger.error(e.info["error"]["reason"])
            return False
        except ElasticConnectionError as e:
            logger.error("Could not connect to the server")
            if e.message is not None:
                logger.error(e.message)
            return False
        return True

    async def wait(self):
        backoff = self.initial_backoff_duration
        start = time.time()
        logger.debug(f"Wait for Elasticsearch (max: {self.max_wait_duration})")
        while time.time() - start < self.max_wait_duration:
            if not self._keep_waiting:
                await self.close()
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

    async def check_exists(self, indices, pipelines):
        for index in indices:
            logger.debug("Checking for index {index} presence")
            if not await self.client.indices.exists(index=index):
                raise PreflightCheckError(f"Cloud not find index {index}")

        for pipeline in pipelines:
            logger.debug("Checking for pipeline {pipeline} presence")
            try:
                await self.client.ingest.get_pipeline(id=pipeline)
            except NotFoundError:
                raise PreflightCheckError(f"Cloud not find pipeline {pipeline}")


def iso_utc(when=None):
    if when is None:
        when = datetime.now(timezone.utc)
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


INVALID_CHARS = "\\", "/", "*", "?", '"', "<", ">", "|", " ", ",", "#"
INVALID_PREFIX = "_", "-", "+"
INVALID_NAME = "..", "."


class InvalidIndexNameError(ValueError):
    pass


class PreflightCheckError(Exception):
    pass


def validate_index_name(name):
    for char in INVALID_CHARS:
        if char in name:
            raise InvalidIndexNameError(f"Invalid character {char}")

    if name.startswith(INVALID_PREFIX):
        raise InvalidIndexNameError(f"Invalid prefix {name[0]}")

    if not name.islower():
        raise InvalidIndexNameError("Must be lowercase")

    if name in INVALID_NAME:
        raise InvalidIndexNameError("Can't use that name")

    return name


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
