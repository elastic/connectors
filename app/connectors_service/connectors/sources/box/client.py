#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import logging
from datetime import datetime, timedelta

import aiohttp
from aiohttp.client_exceptions import ClientResponseError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_unless_exception_type

from connectors_sdk.logger import logger

from connectors.sources.box.constants import (
    BASE_URL,
    BOX_FREE,
    ENDPOINTS,
    FETCH_LIMIT,
    RETRIES,
    RETRY_INTERVAL,
)
from connectors.utils import (
    CacheWithTimeout,
    CancellableSleeps,
)


class TokenError(Exception):
    pass


class NotFound(Exception):
    pass


class AccessToken:
    def __init__(self, configuration, http_session):
        global refresh_token
        self.client_id = configuration["client_id"]
        self.client_secret = configuration["client_secret"]
        self._http_session = http_session
        if refresh_token is None:
            refresh_token = configuration["refresh_token"]
        self._token_cache = CacheWithTimeout()
        self.is_enterprise = configuration["is_enterprise"]
        self.enterprise_id = configuration["enterprise_id"]

    async def get(self):
        if cached_value := self._token_cache.get_value():
            return cached_value
        logger.debug("No token cache found; fetching new token")
        await self._set_access_token()
        return self.access_token

    async def _set_access_token(self):
        logger.debug("Generating an access token")
        try:
            if self.is_enterprise == BOX_FREE:
                global refresh_token
                data = {
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                }
                async with self._http_session.post(
                    url=ENDPOINTS["TOKEN"],
                    data=data,
                ) as response:
                    tokens = await response.json()
                    self.access_token = tokens.get("access_token")
                    refresh_token = tokens.get("refresh_token")
                    self.expired_at = datetime.utcnow() + timedelta(
                        seconds=int(tokens.get("expires_in", 3599))
                    )
                    self._token_cache.set_value(
                        value=self.access_token, expiration_date=self.expired_at
                    )
            else:
                data = {
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "client_credentials",
                    "box_subject_type": "enterprise",
                    "box_subject_id": self.enterprise_id,
                }
                async with self._http_session.post(
                    url=ENDPOINTS["TOKEN"],
                    data=data,
                ) as response:
                    tokens = await response.json()
                    self.access_token = tokens.get("access_token")
                    self.expired_at = datetime.utcnow() + timedelta(
                        seconds=int(tokens.get("expires_in", 3599))
                    )
                    self._token_cache.set_value(
                        value=self.access_token, expiration_date=self.expired_at
                    )
        except Exception as exception:
            msg = f"Error while generating access token. Please verify that provided configurations are correct. Exception {exception}."
            raise TokenError(msg) from exception


class BoxClient:
    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.is_enterprise = configuration["is_enterprise"]
        self._http_session = aiohttp.ClientSession(
            base_url=BASE_URL, raise_for_status=True
        )
        self.token = AccessToken(
            configuration=configuration, http_session=self._http_session
        )

    def set_logger(self, logger_):
        self._logger = logger_

    async def _put_to_sleep(self, retry_after):
        self._logger.debug(
            f"Connector will attempt to retry after {retry_after} seconds."
        )
        await self._sleeps.sleep(retry_after)
        msg = "Rate limit exceeded."
        raise Exception(msg)

    def debug_query_string(self, params):
        if self._logger.isEnabledFor(logging.DEBUG):
            return (
                "&".join(f"{key}={value}" for key, value in params.items())
                if params
                else ""
            )

    async def _handle_client_errors(self, exception):
        match exception.status:
            case 401:
                await self.token._set_access_token()
                raise
            case 429:
                retry_after = int(exception.headers.get("retry-after", 5))
                await self._put_to_sleep(retry_after=retry_after)
            case 404:
                msg = f"Resource Not Found. Error: {exception}"
                raise NotFound(msg)
            case _:
                raise

    @retry(
        stop=stop_after_attempt(RETRIES),
        wait=wait_exponential(multiplier=1, exp_base=RETRY_INTERVAL),
        reraise=True,
        retry=retry_unless_exception_type(NotFound),
    )
    async def get(self, url, headers, params=None):
        self._logger.debug(
            f"Calling GET {url}?{self.debug_query_string(params=params)}"
        )
        try:
            access_token = await self.token.get()
            headers.update({"Authorization": f"Bearer {access_token}"})
            return await self._http_session.get(url=url, headers=headers, params=params)
        except ClientResponseError as exception:
            await self._handle_client_errors(exception=exception)
        except Exception:
            raise

    async def paginated_call(self, url, params, headers):
        try:
            offset = 0
            while True:
                params.update({"offset": offset, "limit": FETCH_LIMIT})
                response = await self.get(url=url, headers=headers, params=params)
                json_response = await response.json()
                total_count = json_response.get("total_count")
                for doc in json_response.get("entries"):
                    yield doc
                if offset >= total_count:
                    break
                offset += FETCH_LIMIT
        except Exception:
            raise

    async def ping(self):
        await self.get(url=ENDPOINTS["PING"], headers={})

    async def close(self):
        self._sleeps.cancel()
        await self._http_session.close()
