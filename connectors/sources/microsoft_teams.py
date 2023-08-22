#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Microsoft Teams source module responsible to fetch documents from Microsoft Teams.
"""
from datetime import datetime, timedelta
from enum import Enum
from functools import cached_property

import aiohttp
from aiohttp.client_exceptions import ClientResponseError
from msal import ConfidentialClientApplication

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    CacheWithTimeout,
    CancellableSleeps,
    RetryStrategy,
    iso_utc,
    retryable,
)

MAX_FILE_SIZE = 10485760
TOKEN_EXPIRES = 3599
RETRY_COUNT = 3
RETRY_INTERVAL = 2
GRAPH_API_AUTH_URL = "https://login.microsoftonline.com"
BASE_URL = "https://graph.microsoft.com/v1.0"
SCOPE = [
    "User.Read.All",
    "TeamMember.Read.All",
    "ChannelMessage.Read.All",
    "Chat.Read",
    "Chat.ReadBasic",
    "Calendars.Read",
]


class UserEndpointName(Enum):
    PING = "Ping"


URLS = {
    UserEndpointName.PING.value: "{base_url}/me",
}


class NotFound(Exception):
    """Internal exception class to handle 404s from the API that has a meaning, that collection
    for specific object is empty.

    It's not an exception for us, we just want to return [], and this exception class facilitates it.
    """

    pass


class InternalServerError(Exception):
    """Exception class to indicate that something went wrong on the server side."""

    pass


class PermissionsMissing(Exception):
    """Exception class to notify that specific Application Permission is missing for the credentials used.
    See: https://learn.microsoft.com/en-us/graph/permissions-reference
    """

    pass


class TokenFetchFailed(Exception):
    """Exception class to notify that connector was unable to fetch authentication token from either
    Microsoft Teams Graph API.

    Error message will indicate human-readable reason.
    """

    pass


class GraphAPIToken:
    """Class for handling access token for Microsoft Graph APIs"""

    def __init__(self, tenant_id, client_id, client_secret, username, password):
        """Initializer.

        Args:
            tenant_id (str): Azure AD Tenant Id
            client_id (str): Azure App Client Id
            client_secret (str): Azure App Client Secret Value
            username (str): Username of the Azure account to fetch the access_token
            password (str): Password of the Azure account to fetch the access_token"""

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password

        self._token_cache_with_username = CacheWithTimeout()

    async def get_with_username_password(self):
        """Get bearer token for provided credentials.

        If token has been retrieved, it'll be taken from the cache.
        Otherwise, call to `_fetch_token` is made to fetch the token
        from 3rd-party service.

        Returns:
            str: bearer token for one of Microsoft services"""
        cached_value = self._token_cache_with_username.get_value()
        if cached_value:
            return cached_value

        # We measure now before request to be on a pessimistic side
        now = datetime.utcnow()
        access_token, expires_in = await self._fetch_token()

        self._token_cache_with_username.set_value(
            access_token, now + timedelta(seconds=expires_in)
        )

        return access_token

    @retryable(
        retries=RETRY_COUNT,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _fetch_token(self):
        """Generate API token for usage with Graph API

        Returns:
            (str, int) - a tuple containing access token as a string and number of seconds it will be valid for as an integer
        """
        authority = f"{GRAPH_API_AUTH_URL}/{self.tenant_id}"

        auth_context = ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=authority,
        )
        token_metadata = auth_context.acquire_token_by_username_password(
            username=self.username, password=self.password, scopes=SCOPE
        )
        if not token_metadata.get("access_token"):
            raise TokenFetchFailed(
                f"Failed to authorize to Graph API. Please verify, that provided details are valid. Error: {token_metadata.get('error_description')}"
            )

        access_token = token_metadata.get("access_token")
        expires_in = int(token_metadata.get("expires_in", TOKEN_EXPIRES))
        return access_token, expires_in


class MicrosoftTeamsClient:
    """Client Class for API calls to Microsoft Teams"""

    def __init__(self, tenant_id, client_id, client_secret, username, password):
        self._sleeps = CancellableSleeps()
        self._http_session = aiohttp.ClientSession(
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=None),
            raise_for_status=True,
        )
        self._api_token = GraphAPIToken(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
        )

        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

    async def fetch(self, url):
        return await self._get_json(absolute_url=url)

    async def _get_json(self, absolute_url):
        self._logger.debug(f"Fetching url: {absolute_url}")
        async for response in self._get(absolute_url=absolute_url):
            return await response.json()

    @retryable(
        retries=RETRY_COUNT,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[NotFound, PermissionsMissing],
    )
    async def _get(self, absolute_url):
        try:
            token = await self._api_token.get_with_username_password()
            self._logger.debug(f"Calling Microsoft Teams Endpoint: {absolute_url}")
            async with self._http_session.get(
                url=absolute_url,
                headers={"authorization": f"Bearer {token}"},
            ) as resp:
                yield resp
        except aiohttp.client_exceptions.ClientOSError:
            self._logger.error(
                "Graph API dropped the connection. It might indicate, that connector makes too many requests - decrease concurrency settings, otherwise Graph API can block this app."
            )
            raise
        except ClientResponseError as e:
            await self._handle_client_response_error(absolute_url, e)

    async def _handle_client_response_error(self, absolute_url, e):
        if e.status == 403:
            raise PermissionsMissing(
                f"Received Unauthorized response for {absolute_url}.\nVerify that the correct Graph API and Microsoft Teams permissions are granted to the app and admin consent is given. If the permissions and consent are correct, wait for several minutes and try again."
            ) from e
        elif e.status == 404:
            raise NotFound from e
        elif e.status == 500:
            raise InternalServerError(
                f"Received InternalServerError error for {absolute_url}: Exception: {e}"
            ) from e
        else:
            raise

    async def ping(self):
        return await self.fetch(
            url=URLS[UserEndpointName.PING.value].format(base_url=BASE_URL)
        )

    async def close(self):
        self._sleeps.cancel()
        await self._http_session.close()


class MicrosoftTeamsDataSource(BaseDataSource):
    """Microsoft Teams"""

    name = "Microsoft Teams"
    service_type = "microsoft_teams"

    def __init__(self, configuration):
        """Set up the connection to the Microsoft Teams.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)

    def _set_internal_logger(self):
        self.client.set_logger(self._logger)

    @cached_property
    def client(self):
        tenant_id = self.configuration["tenant_id"]
        client_id = self.configuration["client_id"]
        client_secret = self.configuration["secret_value"]
        username = self.configuration["username"]
        password = self.configuration["password"]

        return MicrosoftTeamsClient(
            tenant_id, client_id, client_secret, username, password
        )

    async def validate_config(self):
        await super().validate_config()

        # Check that we can log in into Graph API
        await self.client._api_token.get_with_username_password()

    async def ping(self):
        """Verify the connection with Microsoft Teams"""
        try:
            await self.client.ping()
            self._logger.info("Successfully connected to Microsoft Teams")
        except Exception:
            self._logger.exception("Error while connecting to Microsoft Teams")
            raise

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch Microsoft Teams objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """
        yield {"_id": "123", "_timestamp": iso_utc()}, None

    async def close(self):
        """Closes unclosed client session"""
        await self.client.close()

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Microsoft Teams.

        Returns:
            dictionary: Default configuration.
        """
        return {
            "tenant_id": {
                "label": "Tenant ID",
                "order": 1,
                "type": "str",
                "value": "",
            },
            "client_id": {
                "label": "Client ID",
                "order": 2,
                "type": "str",
                "value": "",
            },
            "secret_value": {
                "label": "Secret value",
                "order": 3,
                "sensitive": True,
                "type": "str",
                "value": "",
            },
            "username": {
                "label": "Username",
                "order": 4,
                "type": "str",
                "value": "",
            },
            "password": {
                "label": "Password",
                "order": 5,
                "sensitive": True,
                "type": "str",
                "value": "",
            },
        }
