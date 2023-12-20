#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import functools
import logging
import os
import time
from enum import Enum

from elastic_transport.client_utils import url_to_node_config
from elasticsearch import ApiError, AsyncElasticsearch, ConflictError
from elasticsearch import ConnectionError as ElasticConnectionError

from connectors import __version__
from connectors.logger import logger, set_extra_logger
from connectors.utils import CancellableSleeps


class License(Enum):
    ENTERPRISE = "enterprise"
    PLATINUM = "platinum"
    GOLD = "gold"
    BASIC = "basic"
    TRIAL = "trial"
    EXPIRED = "expired"
    UNSET = None


class ESClient:
    def __init__(self, config):
        # We don't have a way to ask the server, but it's planned
        # for now we just use an env flag
        self.serverless = "SERVERLESS" in os.environ
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

        if "api_key" in config:
            logger.debug(f"Connecting with an API Key ({config['api_key'][:5]}...)")
            options["api_key"] = config["api_key"]
            if "username" in config or "password" in config:
                msg = "configured API key will be used over configured basic auth"
                if (
                    config.get("username") == "elastic"
                    and config.get("password") == "changeme"
                ):
                    logger.debug(
                        msg
                    )  # don't cause a panic if it's just the default creds
                else:
                    logger.warning(msg)
        elif "username" in config:
            auth = config["username"], config["password"]
            options["basic_auth"] = auth
            logger.debug(f"Connecting using Basic Auth (user: {config['username']})")

        if config.get("ssl", False):
            options["verify_certs"] = True
            if "ca_certs" in config:
                ca_certs = config["ca_certs"]
                logger.debug(f"Verifying cert with {ca_certs}")
                options["ca_certs"] = ca_certs

        level = config.get("log_level", "INFO").upper()
        es_logger = logging.getLogger("elastic_transport.node")
        set_extra_logger(
            es_logger,
            log_level=logging.getLevelName(level),
            filebeat=logger.filebeat,  # pyright: ignore
        )
        self.max_wait_duration = config.get("max_wait_duration", 60)
        self.initial_backoff_duration = config.get("initial_backoff_duration", 5)
        self.backoff_multiplier = config.get("backoff_multiplier", 2)
        options["headers"] = config.get("headers", {})
        options["headers"]["user-agent"] = f"elastic-connectors-{__version__}"
        self.client = AsyncElasticsearch(**options)
        self._keep_waiting = True

    def stop_waiting(self):
        self._keep_waiting = False
        self._sleeps.cancel()

    async def has_active_license_enabled(self, license_):
        """This method checks, whether an active license or a more powerful active license is enabled.

        Returns:
            Tuple: (boolean if `license_` is enabled and not expired, actual license Elasticsearch is using)
        """

        license_response = await self.client.license.get()
        license_info = license_response.get("license", {})
        is_expired = license_info.get("status", "").lower() == "expired"

        if is_expired:
            return False, License.EXPIRED

        actual_license = License(license_info.get("type").lower())

        license_order = [
            License.BASIC,
            License.GOLD,
            License.PLATINUM,
            License.ENTERPRISE,
            License.TRIAL,
        ]

        license_index = license_order.index(actual_license)

        return (
            license_order.index(license_) <= license_index,
            actual_license,
        )

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


class ESManagementClient(ESClient):
    """
    Elasticsearch client with methods to manage connector-related indices.

    Additionally to regular methods of ESClient, this client provides methods to work with arbitrary indices,
    for example allowing to list indices, delete indices, wipe data from indices and such.

    ESClient should be used to provide rich clients that operate on "domains", such as:
        - specific connector
        - specific job

    This client, on the contrary, is used to manage a number of indices outside of connector protocol operations.
    """

    def __init__(self, config):
        logger.debug(f"ESManagementClient connecting to {config['host']}")
        # initialize ESIndex instance
        super().__init__(config)

    async def ensure_exists(self, indices=None):
        if indices is None:
            indices = []

        for index in indices:
            logger.debug(f"Checking index {index}")
            if not await self.client.indices.exists(index=index):
                # 400 is if index already exists
                # It could technically have been created by someone else
                # while this code has been running
                await self.client.indices.create(index=index, ignore=400)
                logger.debug(f"Created index {index}")

    async def delete_indices(self, indices):
        await self.client.indices.delete(index=indices, ignore_unavailable=True)

    async def clean_index(self, index_name):
        return await self.client.delete_by_query(
            index=index_name, body={"query": {"match_all": {}}}, ignore_unavailable=True
        )

    async def list_indices(self):
        return await self.client.indices.stats(index="search-*")

    async def index_exists(self, index_name):
        return await self.client.indices.exists(index=index_name)


def with_concurrency_control(retries=3):
    def wrapper(func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            retry = 1
            while retry <= retries:
                try:
                    return await func(*args, **kwargs)
                except ConflictError as e:
                    logger.debug(
                        f"A conflict error was returned from elasticsearch: {e.message}"
                    )
                    if retry >= retries:
                        raise e
                    retry += 1

        return wrapped

    return wrapper
