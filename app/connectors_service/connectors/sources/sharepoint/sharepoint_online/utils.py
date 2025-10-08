#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from collections.abc import Iterable, Sized
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import wraps

import aiohttp
from aiohttp.client_exceptions import ClientPayloadError, ClientResponseError
from aiohttp.client_reqrep import RequestInfo
from azure.identity.aio import CertificateCredential

from connectors.access_control import prefix_identity
from connectors_sdk.utils import nested_get_from_dict
from connectors.utils import (
    CacheWithTimeout,
    CancellableSleeps,
    retryable,
)
from connectors.sources.sharepoint.sharepoint_online.constants import (
    GRAPH_API_AUTH_URL,
    DEFAULT_RETRY_COUNT,
    DEFAULT_RETRY_SECONDS,
    REST_API_AUTH_URL,
    FILE_WRITE_CHUNK_SIZE,
    DELTA_NEXT_LINK_KEY,
    DEFAULT_BACKOFF_MULTIPLIER,
    TIMESTAMP_FORMAT
)

class NotFound(Exception):
    """Internal exception class to handle 404s from the API that has a meaning, that collection
    for specific object is empty.

    For example List Items API from Sharepoint REST API returns 404 if list has no items.

    It's not an exception for us, we just want to return [], and this exception class facilitates it.
    """

    pass


class BadRequestError(Exception):
    """Internal exception class to handle 400's from the API.

    Similar to the NotFound exception, this allows us to catch edge-case responses that should
    be translated as empty resutls, and let us return []."""

    pass


class InternalServerError(Exception):
    """Exception class to indicate that something went wrong on the server side."""

    pass


class ThrottledError(Exception):
    """Internal exception class to indicate that request was throttled by the API"""

    pass


class InvalidSharepointTenant(Exception):
    """Exception class to notify that tenant name is invalid or does not match tenant id provided"""

    pass


class TokenFetchFailed(Exception):
    """Exception class to notify that connector was unable to fetch authentication token from either
    Sharepoint REST API or Graph API.

    Error message will indicate human-readable reason.
    """

    pass


class PermissionsMissing(Exception):
    """Exception class to notify that specific Application Permission is missing for the credentials used.
    See: https://learn.microsoft.com/en-us/graph/permissions-reference
    """

    pass


class SyncCursorEmpty(Exception):
    """Exception class to notify that incremental sync can't run because sync_cursor is empty.
    See: https://learn.microsoft.com/en-us/graph/delta-query-overview
    """

    pass


class MicrosoftSecurityToken:
    """Abstract token for connecting to one of Microsoft Azure services.

    This class is an abstract base class for getting auth token.

    It takes care of caching the token and asking for new token once the
    token expires.

    Classes that inherit from this class need to implement `async def _fetch_token(self)` method
    that needs to return a tuple: access_token<str> and expires_in<int>.

    To read more about tenants and authentication, see:
        - https://learn.microsoft.com/en-us/azure/active-directory/develop/quickstart-create-new-tenant
        - https://learn.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app
    """

    def __init__(self, http_session, tenant_id, tenant_name, client_id):
        """Initializer.

        Args:
            http_session (aiohttp.ClientSession): HTTP Client Session
            tenant_id (str): Azure AD Tenant Id
            tenant_name (str): Azure AD Tenant Name
            client_id (str): Azure App Client Id
            client_secret (str): Azure App Client Secret Value"""

        self._http_session = http_session
        self._tenant_id = tenant_id
        self._tenant_name = tenant_name
        self._client_id = client_id

        self._token_cache = CacheWithTimeout()

    async def get(self):
        """Get bearer token for provided credentials.

        If token has been retrieved, it'll be taken from the cache.
        Otherwise, call to `_fetch_token` is made to fetch the token
        from 3rd-party service.

        Returns:
            str: bearer token for one of Microsoft services"""

        cached_value = self._token_cache.get_value()

        if cached_value:
            return cached_value

        try:
            access_token, expires_at = await self._fetch_token()
        except ClientResponseError as e:
            # Both Graph API and REST API return error codes that indicate different problems happening when authenticating.
            # Error Code serves as a good starting point classifying these errors, see the messages below:
            match e.status:
                case 400:
                    msg = "Failed to authorize to Sharepoint REST API. Please verify, that provided Tenant Id, Tenant Name and Client ID are valid."
                    raise TokenFetchFailed(msg) from e
                case 401:
                    msg = "Failed to authorize to Sharepoint REST API. Please verify, that provided Secret Value is valid."
                    raise TokenFetchFailed(msg) from e
                case _:
                    msg = f"Failed to authorize to Sharepoint REST API. Response Status: {e.status}, Message: {e.message}"
                    raise TokenFetchFailed(msg) from e

        self._token_cache.set_value(access_token, expires_at)

        return access_token

    async def _fetch_token(self):
        """Fetch token from Microsoft service.

        This method needs to be implemented in the class that inherits MicrosoftSecurityToken.

        Returns:
            (str, int) - a tuple containing access token as a string and number of seconds it will be valid for as an integer
        """

        raise NotImplementedError


class SecretAPIToken(MicrosoftSecurityToken):
    def __init__(self, http_session, tenant_id, tenant_name, client_id, client_secret):
        super().__init__(http_session, tenant_id, tenant_name, client_id)
        self._client_secret = client_secret

    async def _fetch_token(self):
        return await super()._fetch_token()


class GraphAPIToken(SecretAPIToken):
    """Token to connect to Microsoft Graph API endpoints."""

    @retryable(retries=3)
    async def _fetch_token(self):
        """Fetch API token for usage with Graph API

        Returns:
            (str, int) - a tuple containing access token as a string and number of seconds it will be valid for as an integer
        """

        url = f"{GRAPH_API_AUTH_URL}/{self._tenant_id}/oauth2/v2.0/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = f"client_id={self._client_id}&scope=https://graph.microsoft.com/.default&client_secret={self._client_secret}&grant_type=client_credentials"

        # We measure now before request to be on a pessimistic side
        now = datetime.utcnow()
        async with self._http_session.post(url, headers=headers, data=data) as resp:
            json_response = await resp.json()
            access_token = json_response["access_token"]
            expires_in = int(json_response["expires_in"])

            return access_token, now + timedelta(seconds=expires_in)


class SharepointRestAPIToken(SecretAPIToken):
    """Token to connect to Sharepoint REST API endpoints."""

    @retryable(retries=DEFAULT_RETRY_COUNT)
    async def _fetch_token(self):
        """Fetch API token for usage with Sharepoint REST API

        Returns:
            (str, int) - a tuple containing access token as a string and number of seconds it will be valid for as an integer
        """

        url = f"{REST_API_AUTH_URL}/{self._tenant_id}/tokens/OAuth/2"
        # GUID in resource is always a constant used to create access token
        data = {
            "grant_type": "client_credentials",
            "resource": f"00000003-0000-0ff1-ce00-000000000000/{self._tenant_name}.sharepoint.com@{self._tenant_id}",
            "client_id": f"{self._client_id}@{self._tenant_id}",
            "client_secret": self._client_secret,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        # We measure now before request to be on a pessimistic side
        now = datetime.utcnow()
        async with self._http_session.post(url, headers=headers, data=data) as resp:
            json_response = await resp.json()
            access_token = json_response["access_token"]
            expires_in = int(json_response["expires_in"])

            return access_token, now + timedelta(seconds=expires_in)


class EntraAPIToken(MicrosoftSecurityToken):
    """Token to connect to Microsoft Graph API endpoints."""

    def __init__(
        self,
        http_session,
        tenant_id,
        tenant_name,
        client_id,
        certificate,
        private_key,
        scope,
    ):
        super().__init__(http_session, tenant_id, tenant_name, client_id)
        self._certificate = certificate
        self._private_key = private_key
        self._scope = scope

    @retryable(retries=3)
    async def _fetch_token(self):
        """Fetch API token for usage with Graph API

        Returns:
            (str, int) - a tuple containing access token as a string and number of seconds it will be valid for as an integer
        """

        secrets_concat = self._certificate + "\n" + self._private_key

        credentials = CertificateCredential(
            self._tenant_id, self._client_id, certificate_data=secrets_concat.encode()
        )

        token = await credentials.get_token(self._scope)

        await credentials.close()

        return token.token, datetime.utcfromtimestamp(token.expires_on)


def retryable_aiohttp_call(retries):
    # TODO: improve utils.retryable to allow custom logic
    # that can help choose what to retry
    def wrapper(func):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            retry = 1
            while retry <= retries:
                try:
                    async for item in func(*args, **kwargs, retry_count=retry):
                        yield item
                    break
                except (NotFound, BadRequestError):
                    raise
                except Exception:
                    if retry >= retries:
                        raise
                    retry += 1

        return wrapped

    return wrapper


class MicrosoftAPISession:
    def __init__(self, http_session, api_token, scroll_field, logger_):
        self._http_session = http_session
        self._api_token = api_token

        # Graph API and Sharepoint API scroll over slightly different fields:
        # - odata.nextPage for Sharepoint REST API uses
        # - @odata.nextPage for Graph API uses - notice the @ glyph
        # Therefore for flexibility I made it a field passed in the initializer,
        # but this abstraction can be better.
        self._scroll_field = scroll_field
        self._sleeps = CancellableSleeps()
        self._logger = logger_

    def set_logger(self, logger_):
        self._logger = logger_

    def close(self):
        self._sleeps.cancel()

    async def fetch(self, url):
        return await self._get_json(url)

    async def post(self, url, payload):
        self._logger.debug(f"Post to url: '{url}' with body: {payload}")
        async with self._post(url, payload) as resp:
            return await resp.json()

    async def pipe(self, url, stream):
        async with self._get(url) as resp:
            async for data in resp.content.iter_chunked(FILE_WRITE_CHUNK_SIZE):
                await stream.write(data)

    async def scroll(self, url):
        scroll_url = url

        while True:
            graph_data = await self._get_json(scroll_url)
            # We're yielding the whole page here, not one item
            yield graph_data["value"]

            if self._scroll_field in graph_data:
                scroll_url = graph_data[self._scroll_field]
            else:
                break

    async def scroll_delta_url(self, url):
        scroll_url = url

        while True:
            graph_data = await self._get_json(scroll_url)

            yield graph_data

            if DELTA_NEXT_LINK_KEY in graph_data:
                scroll_url = graph_data[DELTA_NEXT_LINK_KEY]
            else:
                break

    async def _get_json(self, absolute_url):
        self._logger.debug(f"Fetching url: {absolute_url}")
        async with self._get(absolute_url) as resp:
            return await resp.json()

    @asynccontextmanager
    @retryable_aiohttp_call(retries=DEFAULT_RETRY_COUNT)
    async def _post(self, absolute_url, payload=None, retry_count=0):
        try:
            token = await self._api_token.get()
            headers = {"authorization": f"Bearer {token}"}
            self._logger.debug(f"Posting to Sharepoint Endpoint: {absolute_url}.")

            async with self._http_session.post(
                absolute_url, headers=headers, json=payload
            ) as resp:
                if absolute_url.endswith("/$batch"):  # response code of $batch lies
                    await self._check_batch_items_for_errors(absolute_url, resp)
                yield resp
        except aiohttp.client_exceptions.ClientOSError:
            self._logger.warning(
                "The Microsoft Graph API dropped the connection. It might indicate that the connector is making too many requests. Decrease concurrency settings, otherwise the Graph API may block this app."
            )
            raise
        except ClientResponseError as e:
            await self._handle_client_response_error(absolute_url, e, retry_count)
        except ClientPayloadError as e:
            await self._handle_client_payload_error(e, retry_count)

    async def _check_batch_items_for_errors(self, url, batch_resp):
        body = await batch_resp.json()
        responses = body.get("responses", [])
        for response in responses:
            status = response.get("status", 200)
            if status != 200:
                self._logger.warning(f"Batch request item failed with: {response}")
                headers = response.get("headers", {})
                req_info = RequestInfo(url=url, method="POST", headers=headers)
                raise ClientResponseError(
                    request_info=req_info,
                    headers=headers,
                    status=status,
                    message=nested_get_from_dict(
                        response, ["body", "error", "message"]
                    ),
                    history=(batch_resp),
                )

    @asynccontextmanager
    @retryable_aiohttp_call(retries=DEFAULT_RETRY_COUNT)
    async def _get(self, absolute_url, retry_count=0):
        try:
            token = await self._api_token.get()
            headers = {"authorization": f"Bearer {token}"}
            self._logger.debug(f"Calling Sharepoint Endpoint: {absolute_url}")

            async with self._http_session.get(
                absolute_url,
                headers=headers,
            ) as resp:
                yield resp
        except aiohttp.client_exceptions.ClientOSError:
            self._logger.warning(
                "Graph API dropped the connection. It might indicate, that connector makes too many requests - decrease concurrency settings, otherwise Graph API can block this app."
            )
            raise
        except ClientResponseError as e:
            await self._handle_client_response_error(absolute_url, e, retry_count)
        except ClientPayloadError as e:
            await self._handle_client_payload_error(e, retry_count)

    async def _handle_client_payload_error(self, e, retry_count):
        await self._sleeps.sleep(
            self._compute_retry_after(
                DEFAULT_RETRY_SECONDS, retry_count, DEFAULT_BACKOFF_MULTIPLIER
            )
        )

        raise e

    async def _handle_client_response_error(self, absolute_url, e, retry_count):
        if e.status == 429 or e.status == 503:
            response_headers = e.headers or {}

            if "Retry-After" in response_headers:
                retry_after = int(response_headers["Retry-After"])
            else:
                self._logger.warning(
                    f"Response Code from Sharepoint Online is {e.status} but Retry-After header is not found, using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
                )
                retry_after = DEFAULT_RETRY_SECONDS

            retry_seconds = self._compute_retry_after(
                retry_after, retry_count, DEFAULT_BACKOFF_MULTIPLIER
            )

            self._logger.warning(
                f"Rate Limited by Sharepoint: a new attempt will be performed in {retry_seconds} seconds (retry-after header: {retry_after}, retry_count: {retry_count}, backoff: {DEFAULT_BACKOFF_MULTIPLIER})"
            )

            await self._sleeps.sleep(retry_seconds)
            raise ThrottledError from e
        elif (
            e.status == 403 or e.status == 401
        ):  # Might work weird, but Graph returns 403 and REST returns 401
            msg = f"Received Unauthorized response for {absolute_url}.\nVerify that the correct Graph API and Sharepoint permissions are granted to the app and admin consent is given. If the permissions and consent are correct, wait for several minutes and try again."
            raise PermissionsMissing(msg) from e
        elif e.status == 404:
            raise NotFound from e  # We wanna catch it in the code that uses this and ignore in some cases
        elif e.status == 500:
            raise InternalServerError from e
        elif e.status == 400:
            self._logger.warning(f"Received 400 response from {absolute_url}")
            raise BadRequestError from e
        else:
            raise

    def _compute_retry_after(self, retry_after, retry_count, backoff):
        # Wait for what Sharepoint API asks after the first failure.
        # Apply backoff if API is still not available.
        if retry_count <= 1:
            return retry_after
        else:
            return retry_after + retry_count * backoff

class DriveItemsPage(Iterable, Sized):
    """
    Container for Microsoft Graph API DriveItem response

    Parameters:
        items (list<dict>):Represents a list of drive items
        delta_link (str): Microsoft API deltaLink
    """

    def __init__(self, items, delta_link):
        if items:
            self.items = items
        else:
            self.items = []

        if delta_link:
            self._delta_link = delta_link
        else:
            self._delta_link = None

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        for item in self.items:
            yield item

    def delta_link(self):
        return self._delta_link


def _prefix_group(group):
    return prefix_identity("group", group)


def _prefix_user(user):
    return prefix_identity("user", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_email(email):
    return prefix_identity("email", email)


def _get_login_name(raw_login_name):
    if raw_login_name and (
        raw_login_name.startswith("i:0#.f|membership|")
        or raw_login_name.startswith("c:0o.c|federateddirectoryclaimprovider|")
        or raw_login_name.startswith("c:0t.c|tenant|")
    ):
        parts = raw_login_name.split("|")

        if len(parts) > 2:
            return parts[2]

    return None


def _parse_created_date_time(created_date_time):
    if created_date_time is None:
        return None
    return datetime.strptime(created_date_time, TIMESTAMP_FORMAT)
