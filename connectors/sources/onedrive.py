#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""OneDrive source module responsible to fetch documents from OneDrive.
"""
from datetime import datetime, timedelta
from functools import cached_property
from urllib import parse

import aiohttp
from aiohttp.client_exceptions import ClientResponseError

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    CacheWithTimeout,
    CancellableSleeps,
    RetryStrategy,
    iso_utc,
    retryable,
)

RETRIES = 3
RETRY_INTERVAL = 2
BASE_URL = "https://graph.microsoft.com/v1.0/"
ENDPOINTS = {"PING": "drives"}


class TokenRetrievalError(Exception):
    """Exception class to notify that fetching of access token was not successful."""

    pass


class AccessToken:
    """Class for handling access token for Microsoft Graph APIs"""

    def __init__(self, configuration):
        self.tenant_id = configuration["tenant_id"]
        self.client_id = configuration["client_id"]
        self.client_secret = configuration["client_secret"]
        self._token_cache = CacheWithTimeout()

    async def get(self):
        """Get bearer token required for API call.

        If the token is not present in the cache or expired,
        it calls _set_access_token that sends a POST request
        to Microsoft for generating a new access token.

        Returns:
            str: Access Token
        """
        if cached_value := self._token_cache.get():
            return cached_value
        try:
            await self._set_access_token()
        except ClientResponseError as e:
            match e.status:
                case 400:
                    raise TokenRetrievalError(
                        "Failed to fetch access token. Please verify that provided Tenant ID, Client ID are correct."
                    ) from e
                case 401:
                    raise TokenRetrievalError(
                        "Failed to fetch access token. Please check if Client Secret is valid."
                    ) from e
                case _:
                    raise TokenRetrievalError(
                        f"Failed to fetch access token. Response Status: {e.status}, Message: {e.message}"
                    ) from e
        return self.access_token

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _set_access_token(self):
        """Generate access token with configuration fields and stores it in the cache"""
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": "https://graph.microsoft.com/.default",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        async with aiohttp.request(
            method="POST", url=url, data=data, headers=headers, raise_for_status=True
        ) as response:
            token_reponse = await response.json()
            self.access_token = token_reponse["access_token"]
            self.token_expires_at = datetime.utcnow() + timedelta(
                seconds=int(token_reponse.get("expires_in", 0))
            )
            self._token_cache.set(self.access_token, self.token_expires_at)


class OneDriveClient:
    """Client Class for API calls to OneDrive"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self.retry_count = self.configuration["retry_count"]
        self._logger = logger
        self.token = AccessToken(configuration=configuration)

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def _get_session(self):
        """Generate base client session with configuration fields
        Returns:
            ClientSession: Base client session
        """

        timeout = aiohttp.ClientTimeout(total=300)
        return aiohttp.ClientSession(
            timeout=timeout,
            raise_for_status=True,
        )

    async def close_session(self):
        self._sleeps.cancel()
        await self._get_session.close()
        del self._get_session

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get(self, url_name):
        url = parse.urljoin(BASE_URL, url_name)
        access_token = await self.token.get()
        headers = {"authorization": f"Bearer {access_token}"}
        async with self._get_session.get(url=url, headers=headers) as response:
            yield response


class OneDriveDataSource(BaseDataSource):
    """OneDrive"""

    name = "OneDrive"
    service_type = "onedrive"

    def __init__(self, configuration):
        """Setup the connection to OneDrive

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.configuration = configuration

    @cached_property
    def get_client(self):
        return OneDriveClient(self.configuration)

    def _set_internal_logger(self):
        self.get_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for OneDrive

        Returns:
            dictionary: Default configuration.
        """
        return {
            "client_id": {
                "label": "Azure application Client ID",
                "order": 1,
                "type": "str",
                "value": "client#123",
            },
            "client_secret": {
                "label": "Azure application Client Secret",
                "order": 2,
                "sensitive": True,
                "type": "str",
                "value": "secret#123",
            },
            "tenant_id": {
                "label": "Azure application Tenant ID",
                "order": 3,
                "type": "str",
                "value": "tenant#123",
            },
            "retry_count": {
                "default_value": 3,
                "display": "numeric",
                "label": "Maximum retries per request",
                "order": 4,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": 3,
            },
        }

    async def close(self):
        """Closes unclosed client session"""
        await self.get_client.close_session()

    async def ping(self):
        """Verify the connection with OneDrive"""
        try:
            await anext(self.get_client.get(url_name=ENDPOINTS["PING"]))
            self._logger.info("Successfully connected to OneDrive")
        except Exception:
            self._logger.exception("Error while connecting to OneDrive")
            raise

    async def get_docs(self, filtering=None):
        # yield dummy document to make the chunked PR work, subsequent PR will replace it with actual implementation

        yield {"_id": "123", "timestamp": iso_utc()}, None
