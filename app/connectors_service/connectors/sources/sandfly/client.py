#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import json
import socket
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import aiohttp
from aiohttp.client_exceptions import (
    ClientResponseError,
)
from connectors_sdk.logger import logger

from connectors.utils import (
    CacheWithTimeout,
    CancellableSleeps,
    RetryStrategy,
    retryable,
)

RETRIES = 3
RETRY_INTERVAL = 2
RESULTS_SIZE = 999


class FetchTokenError(Exception):
    pass


class ResourceNotFound(Exception):
    pass


class SandflyAccessToken:
    def __init__(self, http_session, configuration, logger_):
        self._token_cache = CacheWithTimeout()
        self._http_session = http_session
        self._logger = logger_

        self.server_url = configuration["server_url"]
        self.username = configuration["username"]
        self.password = configuration["password"]

    def set_logger(self, logger_):
        self._logger = logger_

    async def get(self, is_cache=True):
        cached_value = self._token_cache.get_value() if is_cache else None

        if cached_value:
            return cached_value

        now = datetime.utcnow()
        access_token, expires_in = await self._fetch_token()
        self._token_cache.set_value(access_token, now + timedelta(seconds=expires_in))

        return access_token

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _fetch_token(self):
        url = f"{self.server_url}/auth/login"
        request_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        data = {
            "username": self.username,
            "password": self.password,
        }

        try:
            async with self._http_session.post(
                url=url, headers=request_headers, data=json.dumps(data)
            ) as response:
                json_response = await response.json()
                return json_response.get("access_token"), 3599
        except Exception as exception:
            msg = f"Error while generating access token. Exception {exception}."
            raise FetchTokenError(msg) from exception


class SandflySession:
    def __init__(self, http_session, token, logger_):
        self._sleeps = CancellableSleeps()
        self._logger = logger_

        self._http_session = http_session
        self._token = token

    def set_logger(self, logger_):
        self._logger = logger_

    def close(self):
        self._sleeps.cancel()

    async def ping(self, server_url):
        try:
            await self._http_session.head(server_url)
            return True
        except ClientResponseError as exception:
            # The 401 Unauthorized means the Sandfly Server is up and running, so return True
            if exception.status == 401:
                return True
            else:
                raise
        except Exception:
            raise

    @asynccontextmanager
    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[ResourceNotFound, FetchTokenError],
    )
    async def _get(self, absolute_url):
        try:
            access_token = await self._token.get()
            headers = {
                "Accept": "application/json",
                "Content-type": "application/json",
                "Authorization": f"Bearer {access_token}",
            }

            async with self._http_session.get(
                url=absolute_url, headers=headers
            ) as response:
                yield response
        except ClientResponseError as exception:
            if exception.status == 401:
                await self._token.get(is_cache=False)
                raise
            elif exception.status == 404:
                msg = "Resource Not Found"
                raise ResourceNotFound(msg) from exception
            else:
                raise
        except FetchTokenError:
            raise
        except Exception:
            raise

    @asynccontextmanager
    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[ResourceNotFound, FetchTokenError],
    )
    async def _post(self, absolute_url, payload):
        try:
            access_token = await self._token.get()
            headers = {
                "Accept": "application/json",
                "Content-type": "application/json",
                "Authorization": f"Bearer {access_token}",
            }

            async with self._http_session.post(
                url=absolute_url, headers=headers, data=json.dumps(payload)
            ) as response:
                yield response
        except ClientResponseError as exception:
            if exception.status == 401:
                await self._token.get(is_cache=False)
                raise
            elif exception.status == 404:
                msg = "Resource Not Found"
                raise ResourceNotFound(msg) from exception
            else:
                raise
        except FetchTokenError:
            raise
        except Exception:
            raise

    async def content_get(self, url):
        try:
            async with self._get(absolute_url=url) as response:
                return await response.text()
        except Exception as exception:
            self._logger.warning(
                f"Content-GET for {url} is being skipped. Error: {exception}."
            )
            raise

    async def content_post(self, url, payload):
        try:
            async with self._post(absolute_url=url, payload=payload) as response:
                return await response.text()
        except Exception as exception:
            self._logger.warning(
                f"Content-POST for {url} is being skipped. Error: {exception}."
            )
            raise


class SandflyClient:
    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self._logger = logger

        self.configuration = configuration

        self.server_url = self.configuration["server_url"]
        self.verify_ssl = self.configuration["verify_ssl"]

        connector = aiohttp.TCPConnector(
            family=socket.AF_INET, verify_ssl=self.verify_ssl
        )
        self.http_session = aiohttp.ClientSession(
            connector=connector, raise_for_status=True
        )

        self.token = SandflyAccessToken(
            http_session=self.http_session,
            configuration=configuration,
            logger_=self._logger,
        )
        self.client = SandflySession(
            http_session=self.http_session,
            token=self.token,
            logger_=self._logger,
        )

    def set_logger(self, logger_):
        self._logger = logger_
        self.token.set_logger(self._logger)
        self.client.set_logger(self._logger)

    async def close(self):
        await self.http_session.close()
        self.client.close()

    async def ping(self):
        try:
            await self.client.ping(self.server_url)
            self._logger.info(
                f"SandflyClient PING : Successfully connected to Sandfly Security [{self.server_url}]"
            )
            return True
        except Exception:
            self._logger.error(
                f"SandflyClient PING : Error while connecting to Sandfly Security [{self.server_url}]"
            )
            raise

    async def get_results_by_id(self, sequence_id, enable_pass):
        results_url = f"{self.server_url}/results"

        payload = None

        if enable_pass:
            payload = {
                "size": RESULTS_SIZE,
                "filter": {
                    "items": [
                        {
                            "columnField": "sequence_id",
                            "operatorValue": ">",
                            "value": sequence_id,
                        }
                    ]
                },
                "sort": [
                    {
                        "Field": "sequence_id",
                        "sort": "asc",
                    }
                ],
            }
        else:
            payload = {
                "size": RESULTS_SIZE,
                "filter": {
                    "items": [
                        {
                            "columnField": "data.status",
                            "operatorValue": "notEquals",
                            "value": "pass",
                        },
                        {
                            "columnField": "sequence_id",
                            "operatorValue": ">",
                            "value": sequence_id,
                        },
                    ],
                    "linkoperator": "and",
                },
                "sort": [
                    {
                        "Field": "sequence_id",
                        "sort": "asc",
                    }
                ],
            }

        content = await self.client.content_post(url=results_url, payload=payload)

        if content is not None:
            content_json = json.loads(content)
            total = content_json["total"]
            more_results = content_json["more_results"]
            self._logger.debug(f"GET_RESULTS_BY_ID : [{total}] : [{more_results}]")

            data_list = content_json["data"]
            for result_item in data_list:
                yield result_item, more_results

    async def get_results_by_time(self, time_since, enable_pass):
        results_url = f"{self.server_url}/results"

        if enable_pass:
            payload = {
                "size": RESULTS_SIZE,
                "time_since": time_since,
                "sort": [
                    {
                        "Field": "sequence_id",
                        "sort": "asc",
                    }
                ],
            }
        else:
            payload = {
                "size": RESULTS_SIZE,
                "time_since": time_since,
                "filter": {
                    "items": [
                        {
                            "columnField": "data.status",
                            "operatorValue": "notEquals",
                            "value": "pass",
                        }
                    ]
                },
                "sort": [
                    {
                        "Field": "sequence_id",
                        "sort": "asc",
                    }
                ],
            }

        content = await self.client.content_post(url=results_url, payload=payload)

        if content is not None:
            content_json = json.loads(content)

            total = content_json["total"]
            more_results = content_json["more_results"]
            self._logger.debug(f"GET_RESULTS_BY_TIME : [{total}] : [{more_results}]")

            data_list = content_json["data"]
            for result_item in data_list:
                yield result_item, more_results

    async def get_ssh_keys(self):
        ssh_url = f"{self.server_url}/sshhunter/summary"
        content = await self.client.content_get(url=ssh_url)

        if content is not None:
            content_json = json.loads(content)

            more_results = content_json["more_results"]
            data_list = content_json["data"]

            for key in data_list:
                key_id = key["id"]
                key_url = f"{self.server_url}/sshhunter/key/{key_id}"

                key_details = await self.client.content_get(url=key_url)
                if key_details is not None:
                    yield json.loads(key_details), more_results

    async def get_hosts(self):
        hosts_url = f"{self.server_url}/hosts"
        content = await self.client.content_get(url=hosts_url)

        if content is not None:
            content_json = json.loads(content)

            data_list = content_json["data"]
            for host in data_list:
                yield host

    async def get_license(self):
        license_url = f"{self.server_url}/license"
        content = await self.client.content_get(url=license_url)

        if content is not None:
            content_json = json.loads(content)
            yield content_json
