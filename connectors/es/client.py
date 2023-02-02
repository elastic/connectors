#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import logging
import time

from elastic_transport.client_utils import url_to_node_config
from elasticsearch import ApiError, AsyncElasticsearch
from elasticsearch import ConnectionError as ElasticConnectionError
from elasticsearch import NotFoundError

from connectors import __version__
from connectors.logger import logger, set_extra_logger
from connectors.utils import CancellableSleeps


class PreflightCheckError(Exception):
    pass


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
            logger.debug(f"Connecting using Basic Auth (user: {config['username']})")
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
        options["headers"] = config.get("headers", {})
        options["headers"]["user-agent"] = f"elastic-connectors-python-{__version__}"
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

    async def check_exists(self, indices=None, pipelines=None):
        if indices is None:
            indices = []
        if pipelines is None:
            pipelines = []

        for index in indices:
            logger.debug(f"Checking for index {index} presence")
            if not await self.client.indices.exists(index=index):
                raise PreflightCheckError(f"Could not find index {index}")

        for pipeline in pipelines:
            logger.debug(f"Checking for pipeline {pipeline} presence")
            try:
                await self.client.ingest.get_pipeline(id=pipeline)
            except NotFoundError:
                raise PreflightCheckError(f"Could not find pipeline {pipeline}")

    async def delete_indices(self, indices=[]):
        await self.client.indices.delete(index=indices, ignore_unavailable=True)
