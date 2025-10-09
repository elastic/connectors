#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import logging

import aiohttp
from aiohttp.client_exceptions import ClientResponseError
from connectors_sdk.logger import logger

from connectors.sources.box.utils import (
    BASE_URL,
    ENDPOINTS,
    FETCH_LIMIT,
    RETRIES,
    RETRY_INTERVAL,
    AccessToken,
    NotFound,
)
from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    retryable,
)


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

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=NotFound,
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
