#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#


import os
from datetime import datetime, timedelta
from functools import cached_property
from urllib import parse

import aiohttp
from aiohttp.client_exceptions import (
    ClientPayloadError,
    ClientResponseError,
    ServerConnectionError,
)
from connectors_sdk.logger import logger
from wcmatch import glob

from connectors.sources.onedrive.constants import (
    BASE_URL,
    DEFAULT_PARALLEL_CONNECTION_COUNT,
    DEFAULT_RETRY_SECONDS,
    DELTA,
    ENDPOINTS,
    FETCH_SIZE,
    GRAPH_API_AUTH_URL,
    GROUPS,
    ITEM_FIELDS,
    PERMISSIONS,
    REQUEST_TIMEOUT,
    RETRIES,
    RETRY_INTERVAL,
    USERS,
)
from connectors.utils import (
    CacheWithTimeout,
    CancellableSleeps,
    RetryStrategy,
    retryable,
)


class TokenRetrievalError(Exception):
    """Exception class to notify that fetching of access token was not successful."""

    pass


class ThrottledError(Exception):
    """Internal exception class to indicate that request was throttled by the API"""

    pass


class InternalServerError(Exception):
    pass


class NotFound(Exception):
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
        if cached_value := self._token_cache.get_value():
            return cached_value
        try:
            await self._set_access_token()
        except ClientResponseError as e:
            match e.status:
                case 400:
                    msg = "Failed to fetch access token. Please verify that provided Tenant ID, Client ID are correct."
                    raise TokenRetrievalError(msg) from e
                case 401:
                    msg = "Failed to fetch access token. Please check if Client Secret is valid."
                    raise TokenRetrievalError(msg) from e
                case _:
                    msg = f"Failed to fetch access token. Response Status: {e.status}, Message: {e.message}"
                    raise TokenRetrievalError(msg) from e
        return self.access_token

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _set_access_token(self):
        """Generate access token with configuration fields and stores it in the cache"""
        url = f"{GRAPH_API_AUTH_URL}/{self.tenant_id}/oauth2/v2.0/token"
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
            self._token_cache.set_value(self.access_token, self.token_expires_at)


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
    def session(self):
        """Generate base client session with configuration fields
        Returns:
            ClientSession: Base client session
        """
        connector = aiohttp.TCPConnector(limit=DEFAULT_PARALLEL_CONNECTION_COUNT)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        return aiohttp.ClientSession(
            timeout=timeout,
            raise_for_status=True,
            connector=connector,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
        )

    async def close_session(self):
        self._sleeps.cancel()
        await self.session.close()
        del self.session

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=NotFound,
    )
    async def get(self, url, header=None):
        access_token = await self.token.get()
        headers = {"authorization": f"Bearer {access_token}"}
        if header:
            headers |= header
        try:
            async with self.session.get(url=url, headers=headers) as response:
                yield response
        except ServerConnectionError:
            await self.close_session()
            raise
        except ClientResponseError as e:
            await self._handle_client_side_errors(e)

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=NotFound,
    )
    async def post(self, url, payload=None):
        access_token = await self.token.get()
        headers = {
            "authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        try:
            async with self.session.post(url, headers=headers, data=payload) as resp:
                yield resp

        except ClientResponseError as e:
            await self._handle_client_side_errors(e)
        except ClientPayloadError as e:
            retry_seconds = DEFAULT_RETRY_SECONDS
            response_headers = e.headers or {}  # type: ignore[attr-defined]
            if "Retry-After" in response_headers:
                try:
                    retry_seconds = int(response_headers["Retry-After"])
                except (TypeError, ValueError) as exception:
                    self._logger.error(
                        f"Error while reading value of retry-after header {exception}. Using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
                    )
            await self._sleeps.sleep(retry_seconds)
            raise

    async def _handle_client_side_errors(self, e):
        if e.status == 429 or e.status == 503:
            response_headers = e.headers or {}
            retry_seconds = DEFAULT_RETRY_SECONDS
            if "Retry-After" in response_headers:
                try:
                    retry_seconds = int(response_headers["Retry-After"])
                except (TypeError, ValueError) as exception:
                    self._logger.error(
                        f"Error while reading value of retry-after header {exception}. Using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
                    )
            else:
                self._logger.warning(
                    f"Rate Limited but Retry-After header is not found, using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
                )
            self._logger.debug(f"Rate Limit reached: retry in {retry_seconds} seconds")

            await self._sleeps.sleep(retry_seconds)
            raise ThrottledError from e
        elif e.status == 404:
            raise NotFound from e
        elif e.status == 500:
            raise InternalServerError from e
        else:
            raise

    async def paginated_api_call(
        self, url, params=None, fetch_size=FETCH_SIZE, header=None
    ):
        if params is None:
            params = {}
        params["$top"] = fetch_size
        params = "&".join(f"{key}={val}" for key, val in params.items())

        url = f"{url}?{params}"
        while True:
            try:
                async for response in self.get(url=url, header=header):
                    response_json = await response.json()
                    result = response_json["value"]

                    if len(result) == 0:
                        return

                    yield result

                    url = response_json.get("@odata.nextLink")
                    if not url:
                        return
            except Exception as exception:
                self._logger.warning(
                    f"Skipping data for {url}. Exception: {exception}."
                )
                break

    async def list_users(self, include_groups=False):
        header = None
        params = {
            "$filter": "accountEnabled eq true",
            "$select": "userPrincipalName,mail,transitiveMemberOf,id,createdDateTime",
        }
        # This condition is executed only during access control sync where connector will fetch all the users (licensed and unlicensed).
        if include_groups:
            params["$expand"] = "transitiveMemberOf($select=id)"

        # This condtion is executed during content sync where connector fetches only licensed accounts as unlicensed users won't have any files.
        else:
            params["$filter"] += " and assignedLicenses/$count ne 0&$count=true"
            header = {"ConsistencyLevel": "eventual"}
        url = parse.urljoin(BASE_URL, ENDPOINTS[USERS])

        async for response in self.paginated_api_call(url, params, header=header):
            for user_detail in response:
                yield user_detail

    async def list_groups(self, user_id):
        url = parse.urljoin(BASE_URL, ENDPOINTS[GROUPS].format(user_id=user_id))
        async for response in self.paginated_api_call(url):
            for group_detail in response:
                yield group_detail

    async def list_file_permission(self, user_id, file_id):
        url = parse.urljoin(
            BASE_URL, ENDPOINTS[PERMISSIONS].format(user_id=user_id, item_id=file_id)
        )
        async for response in self.paginated_api_call(url):
            for permission_detail in response:
                yield permission_detail

    async def get_owned_files(self, user_id, skipped_extensions=None, pattern=""):
        params = {"$select": ITEM_FIELDS}
        delta_endpoint = ENDPOINTS[DELTA].format(user_id=user_id)

        url = parse.urljoin(BASE_URL, delta_endpoint)
        async for response in self.paginated_api_call(url, params):
            for file in response:
                if file.get("name", "") != "root":
                    parent_path = file.get("parentReference", {}).get("path")
                    is_match = glob.globmatch(parent_path, pattern, flags=glob.GLOBSTAR)
                    has_skipped_extension = os.path.splitext(file["name"])[-1] in (
                        skipped_extensions or []
                    )
                    if has_skipped_extension or (pattern and not is_match):
                        continue
                    else:
                        yield file, file.get("@microsoft.graph.downloadUrl")
