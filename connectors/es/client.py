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

from elastic_transport import ConnectionTimeout
from elastic_transport.client_utils import url_to_node_config
from elasticsearch import ApiError, AsyncElasticsearch, ConflictError
from elasticsearch import (
    ConnectionError as ElasticConnectionError,
)

from connectors import __version__
from connectors.config import (
    DEFAULT_ELASTICSEARCH_MAX_RETRIES,
    DEFAULT_ELASTICSEARCH_RETRY_INTERVAL,
)
from connectors.logger import logger, set_extra_logger
from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    time_to_sleep_between_retries,
)


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
        self._retrier = TransientElasticsearchRetrier(
            logger,
            config.get("max_retries", DEFAULT_ELASTICSEARCH_MAX_RETRIES),
            config.get("retry_interval", DEFAULT_ELASTICSEARCH_RETRY_INTERVAL),
        )

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

        license_response = await self._retrier.execute_with_retry(
            self.client.license.get
        )
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
        await self._retrier.close()
        await self.client.close()

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


class RetryInterruptedError(Exception):
    pass


class TransientElasticsearchRetrier:
    def __init__(
        self,
        logger_,
        max_retries,
        retry_interval,
        retry_strategy=RetryStrategy.LINEAR_BACKOFF,
    ):
        self._logger = logger_
        self._sleeps = CancellableSleeps()
        self._keep_retrying = True
        self._error_codes_to_retry = [429, 500, 502, 503, 504]
        self._max_retries = max_retries
        self._retry_interval = retry_interval
        self._retry_strategy = retry_strategy

    async def close(self):
        self._sleeps.cancel()
        self._keep_retrying = False

    async def _sleep(self, retry):
        time_to_sleep = time_to_sleep_between_retries(
            self._retry_strategy, self._retry_interval, retry
        )
        self._logger.debug(f"Attempt {retry}: sleeping for {time_to_sleep}")
        await self._sleeps.sleep(time_to_sleep)

    async def execute_with_retry(self, func):
        retry = 0
        while self._keep_retrying and retry < self._max_retries:
            retry += 1
            try:
                result = await func()

                return result
            except ConnectionTimeout:
                self._logger.debug(f"Attempt {retry}: connection timeout")

                if retry >= self._max_retries:
                    raise
            except ApiError as e:
                self._logger.debug(
                    f"Attempt {retry}: api error with status {e.status_code}"
                )

                if e.status_code not in self._error_codes_to_retry:
                    raise
                if retry >= self._max_retries:
                    raise

            await self._sleep(retry)

        msg = "Retry operation was interrupted"
        raise RetryInterruptedError(msg)


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
