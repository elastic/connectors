#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os
import re
from collections.abc import Iterable, Sized
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import wraps

import aiohttp
from aiohttp.client_exceptions import ClientPayloadError, ClientResponseError
from aiohttp.client_reqrep import RequestInfo
from azure.identity.aio import CertificateCredential
from tenacity import retry, stop_after_attempt, wait_chain, wait_fixed

from connectors_sdk.logger import logger
from connectors_sdk.utils import nested_get_from_dict

from connectors.sources.sharepoint.sharepoint_online.constants import (
    DEFAULT_BACKOFF_MULTIPLIER,
    DEFAULT_PARALLEL_CONNECTION_COUNT,
    DEFAULT_RETRY_COUNT,
    DEFAULT_RETRY_SECONDS,
    DELTA_LINK_KEY,
    DELTA_NEXT_LINK_KEY,
    DRIVE_ITEMS_FIELDS,
    FILE_WRITE_CHUNK_SIZE,
    GRAPH_API_AUTH_URL,
    GRAPH_API_URL,
    REST_API_AUTH_URL,
    WILDCARD,
)
from connectors.utils import (
    CacheWithTimeout,
    CancellableSleeps,
    url_encode,
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

    @retry(
        stop=stop_after_attempt(3),
        # linear backoff
        wait=wait_chain(*[wait_fixed(1 * i) for i in range(1, 4)]),
        reraise=True,
    )
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

    @retry(
        stop=stop_after_attempt(DEFAULT_RETRY_COUNT),
        # linear backoff
        wait=wait_chain(*[wait_fixed(1 * i) for i in range(1, DEFAULT_RETRY_COUNT + 1)]),
        reraise=True,
    )
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

    @retry(
        stop=stop_after_attempt(3),
        # linear backoff
        wait=wait_chain(*[wait_fixed(1 * i) for i in range(1, 4)]),
        reraise=True,
    )
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


class SharepointOnlineClient:
    def __init__(
        self,
        tenant_id,
        tenant_name,
        client_id,
        client_secret=None,
        certificate=None,
        private_key=None,
    ):
        # Sharepoint / Graph API has quite strict throttling policies
        # If connector is overzealous, it can be banned for not respecting throttling policies
        # However if connector has a low setting for the tcp_connector limit, then it'll just be slow.
        # Change the value at your own risk
        tcp_connector = aiohttp.TCPConnector(limit=DEFAULT_PARALLEL_CONNECTION_COUNT)
        self._http_session = aiohttp.ClientSession(  # TODO: lazy create this
            connector=tcp_connector,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=None),
            raise_for_status=True,
        )

        self._tenant_id = tenant_id
        self._tenant_name = tenant_name
        self._tenant_name_pattern = re.compile(
            "https://(.*).sharepoint.com"
        )  # Used later for url validation

        if client_secret and not certificate and not private_key:
            self.graph_api_token = GraphAPIToken(
                self._http_session, tenant_id, tenant_name, client_id, client_secret
            )
            self.rest_api_token = SharepointRestAPIToken(
                self._http_session, tenant_id, tenant_name, client_id, client_secret
            )
        elif certificate and private_key:
            self.graph_api_token = EntraAPIToken(
                self._http_session,
                tenant_id,
                tenant_name,
                client_id,
                certificate,
                private_key,
                "https://graph.microsoft.com/.default",
            )
            self.rest_api_token = EntraAPIToken(
                self._http_session,
                tenant_id,
                tenant_name,
                client_id,
                certificate,
                private_key,
                f"https://{self._tenant_name}.sharepoint.com/.default",
            )
        else:
            msg = "Unexpected authentication: either a client_secret or certificate+private_key should be provided"
            raise Exception(msg)

        self._logger = logger

        self._graph_api_client = MicrosoftAPISession(
            self._http_session, self.graph_api_token, "@odata.nextLink", self._logger
        )
        self._rest_api_client = MicrosoftAPISession(
            self._http_session, self.rest_api_token, "odata.nextLink", self._logger
        )

    def set_logger(self, logger_):
        self._logger = logger_
        self._graph_api_client.set_logger(self._logger)
        self._rest_api_client.set_logger(self._logger)

    async def groups(self):
        select = ""

        async for page in self._graph_api_client.scroll(
            f"{GRAPH_API_URL}/groups?$select={select}"
        ):
            for group in page:
                yield group

    async def group_sites(self, group_id):
        select = ""

        try:
            async for page in self._graph_api_client.scroll(
                f"{GRAPH_API_URL}/groups/{group_id}/sites?$select={select}"
            ):
                for group_site in page:
                    yield group_site
        except NotFound:
            # We can safely ignore cause Sharepoint can return 404 in case List Item is of specific types that do not support/have attachments
            # Yes, makes no sense to me either.
            return

    async def site_collections(self):
        try:
            filter_ = url_encode("siteCollection/root ne null")
            select = "siteCollection,webUrl"

            async for page in self._graph_api_client.scroll(
                f"{GRAPH_API_URL}/sites/?$filter={filter_}&$select={select}"
            ):
                for site_collection in page:
                    yield site_collection
        except PermissionsMissing:
            self._logger.warning(
                "Looks like 'Sites.Read.All' permission is missing to fetch all root-level site collections, hence fetching only tenant root site"
            )
            yield await self._graph_api_client.fetch(url=f"{GRAPH_API_URL}/sites/root")

    async def site_role_assignments(self, site_web_url):
        self._validate_sharepoint_rest_url(site_web_url)
        expand = "Member/users,RoleDefinitionBindings"

        url = f"{site_web_url}/_api/web/roleassignments?$expand={expand}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for role_assignment in page:
                    yield role_assignment
        except NotFound:
            self._logger.debug(f"No role assignments found for site: '{site_web_url}'")
            return

    async def site_admins(self, site_web_url):
        self._validate_sharepoint_rest_url(site_web_url)
        filter_param = url_encode("isSiteAdmin eq true")
        url = f"{site_web_url}/_api/web/SiteUsers?$filter={filter_param}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for member in page:
                    yield member
        except NotFound:
            self._logger.debug(f"No site admins found for site: '${site_web_url}'")
            return

    async def site_groups_users(self, site_web_url, site_group_id):
        self._validate_sharepoint_rest_url(site_web_url)

        select_ = "Email,Id,UserPrincipalName,LoginName,Title"
        url = f"{site_web_url}/_api/web/sitegroups/getbyid({site_group_id})/users?$select={select_}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for site_group_user in page:
                    yield site_group_user
        except NotFound:
            self._logger.warning(
                f"NotFound error when fetching users for sitegroup '{site_group_id}' at '{site_web_url}'."
            )
            return

    async def active_users_with_groups(self):
        expand = "transitiveMemberOf($select=id)"
        top = 999  # this is accepted, but does not get taken literally. Response size seems to max out at 100
        filter_ = "accountEnabled eq true"
        select = "UserName,userPrincipalName,Email,mail,transitiveMemberOf,id,createdDateTime"
        url = f"{GRAPH_API_URL}/users?$expand={expand}&$top={top}&$filter={filter_}&$select={select}"

        try:
            async for page in self._graph_api_client.scroll(url):
                for user in page:
                    yield user
        except NotFound:
            return

    async def group_members(self, group_id):
        url = f"{GRAPH_API_URL}/groups/{group_id}/members"

        try:
            async for page in self._graph_api_client.scroll(url):
                for member in page:
                    yield member
        except NotFound:
            return

    async def group_owners(self, group_id):
        select = "id,mail,userPrincipalName"
        url = f"{GRAPH_API_URL}/groups/{group_id}/owners?$select={select}"

        try:
            async for page in self._graph_api_client.scroll(url):
                for owner in page:
                    yield owner
        except NotFound:
            return

    async def site_users(self, site_web_url):
        self._validate_sharepoint_rest_url(site_web_url)

        url = f"{site_web_url}/_api/web/siteusers"

        try:
            async for page in self._rest_api_client.scroll(url):
                for user in page:
                    yield user
        except NotFound:
            return

    async def sites(
        self,
        sharepoint_host,
        allowed_root_sites,
        enumerate_all_sites=True,
        fetch_subsites=False,
    ):
        if allowed_root_sites == [WILDCARD] or enumerate_all_sites:
            self._logger.debug(f"Looking up all sites to fetch: {allowed_root_sites}")
            async for site in self._all_sites(sharepoint_host, allowed_root_sites):
                yield site
        else:
            self._logger.debug(f"Looking up individual sites: {allowed_root_sites}")
            for allowed_site in allowed_root_sites:
                try:
                    if fetch_subsites:
                        async for site in self._fetch_site_and_subsites_by_path(
                            sharepoint_host, allowed_site
                        ):
                            yield site
                    else:
                        yield await self._fetch_site(sharepoint_host, allowed_site)

                except NotFound:
                    self._logger.warning(
                        f"Could not look up site '{allowed_site}' by relative path in parent site: {sharepoint_host}"
                    )

    async def _all_sites(self, sharepoint_host, allowed_root_sites):
        select = ""
        try:
            async for page in self._graph_api_client.scroll(
                f"{GRAPH_API_URL}/sites/{sharepoint_host}/sites?search=*&$select={select}"
            ):
                for site in page:
                    # Filter out site collections that are not needed
                    if [WILDCARD] != allowed_root_sites and site[
                        "name"
                    ] not in allowed_root_sites:
                        continue
                    yield site
        except PermissionsMissing as exception:
            if allowed_root_sites == [WILDCARD]:
                msg = "The configuration field 'Comma-separated list of sites' with '*' value is only compatible with 'Sites.Read.All' permission."
            else:
                msg = "To enumerate all sites, the connector requires 'Sites.Read.All' permission"
            raise PermissionsMissing(msg) from exception

    async def _fetch_site_and_subsites_by_path(self, sharepoint_host, allowed_site):
        self._logger.debug(
            f"Requesting site '{allowed_site}' and subsites by relative path in host: {sharepoint_host}"
        )
        site_with_subsites = await self._graph_api_client.fetch(
            f"{GRAPH_API_URL}/sites/{sharepoint_host}:/sites/{allowed_site}?expand=sites"
        )
        async for site in self._recurse_sites(site_with_subsites):
            yield site

    async def _fetch_site(self, sharepoint_host, allowed_site):
        self._logger.debug(
            f"Requesting site '{allowed_site}' by relative path in parent site: {sharepoint_host}"
        )
        return await self._graph_api_client.fetch(
            f"{GRAPH_API_URL}/sites/{sharepoint_host}:/sites/{allowed_site}"
        )

    async def _scroll_subsites_by_parent_id(self, parent_site_id):
        self._logger.debug(f"Scrolling subsites of {parent_site_id}")
        async for page in self._graph_api_client.scroll(
            f"{GRAPH_API_URL}/sites/{parent_site_id}/sites?expand=sites"
        ):
            for site in page:
                async for subsite in self._recurse_sites(site):  # pyright: ignore
                    yield subsite

    async def _recurse_sites(self, site_with_subsites):
        subsites = site_with_subsites.pop("sites", [])
        site_with_subsites.pop("sites@odata.context", None)  # remove unnecessary field
        yield site_with_subsites
        if subsites:
            async for site in self._scroll_subsites_by_parent_id(
                site_with_subsites["id"]
            ):
                yield site

    async def site_drives(self, site_id):
        select = "createdDateTime,description,id,lastModifiedDateTime,name,webUrl,driveType,createdBy,lastModifiedBy,owner"

        async for page in self._graph_api_client.scroll(
            f"{GRAPH_API_URL}/sites/{site_id}/drives?$select={select}"
        ):
            for site_drive in page:
                yield site_drive

    async def drive_items_delta(self, url):
        async for response in self._graph_api_client.scroll_delta_url(url):
            delta_link = (
                response[DELTA_LINK_KEY] if DELTA_LINK_KEY in response else None
            )
            if "value" in response and len(response["value"]) > 0:
                yield DriveItemsPage(response["value"], delta_link)

    async def drive_items(self, drive_id, url=None):
        url = (
            (
                f"{GRAPH_API_URL}/drives/{drive_id}/root/delta?$select={DRIVE_ITEMS_FIELDS}"
            )
            if not url
            else url
        )

        async for page in self.drive_items_delta(url):
            yield page

    async def drive_items_permissions_batch(self, drive_id, drive_item_ids):
        requests = []

        for item_id in drive_item_ids:
            permissions_uri = f"/drives/{drive_id}/items/{item_id}/permissions"
            requests.append({"id": item_id, "method": "GET", "url": permissions_uri})

        if not requests:
            self._logger.debug(
                "Skipping fetching empty batch of drive item permissions"
            )
            return
        try:
            batch_url = f"{GRAPH_API_URL}/$batch"
            batch_request = {"requests": requests}
            batch_response = await self._graph_api_client.post(batch_url, batch_request)

            for response in batch_response.get("responses", []):
                yield response
        except NotFound:
            return

    async def download_drive_item(self, drive_id, item_id, async_buffer):
        await self._graph_api_client.pipe(
            f"{GRAPH_API_URL}/drives/{drive_id}/items/{item_id}/content", async_buffer
        )

    async def site_lists(self, site_id):
        select = "createdDateTime,id,lastModifiedDateTime,name,webUrl,displayName,createdBy,lastModifiedBy"

        try:
            async for page in self._graph_api_client.scroll(
                f"{GRAPH_API_URL}/sites/{site_id}/lists?$select={select}"
            ):
                for site_list in page:
                    yield site_list
        except NotFound:
            return

    async def site_list_has_unique_role_assignments(self, site_web_url, site_list_name):
        self._validate_sharepoint_rest_url(site_web_url)

        url = f"{site_web_url}/_api/lists/GetByTitle('{site_list_name}')/HasUniqueRoleAssignments"

        try:
            response = await self._rest_api_client.fetch(url)
            return response.get("value", False)
        except NotFound:
            return False

    async def site_list_role_assignments(self, site_web_url, site_list_name):
        self._validate_sharepoint_rest_url(site_web_url)

        expand = "Member/users,RoleDefinitionBindings"

        url = f"{site_web_url}/_api/lists/GetByTitle('{site_list_name}')/roleassignments?$expand={expand}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for role_assignment in page:
                    yield role_assignment
        except NotFound:
            return

    async def site_list_item_has_unique_role_assignments(
        self, site_web_url, site_list_name, list_item_id
    ):
        self._validate_sharepoint_rest_url(site_web_url)

        url = f"{site_web_url}/_api/lists/GetByTitle('{site_list_name}')/items({list_item_id})/HasUniqueRoleAssignments"

        try:
            response = await self._rest_api_client.fetch(url)
            return response.get("value", False)
        except NotFound:
            return False
        except BadRequestError:
            self._logger.warning(
                f"Received error response when retrieving `{list_item_id}` from list: `{site_list_name}` in site: `{site_web_url}`"
            )
            return False

    async def site_list_item_role_assignments(
        self, site_web_url, site_list_name, list_item_id
    ):
        self._validate_sharepoint_rest_url(site_web_url)

        expand = "Member/users,RoleDefinitionBindings"

        url = f"{site_web_url}/_api/lists/GetByTitle('{site_list_name}')/items({list_item_id})/roleassignments?$expand={expand}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for role_assignment in page:
                    yield role_assignment
        except NotFound:
            return

    async def site_list_items(self, site_id, list_id):
        select = "createdDateTime,id,lastModifiedDateTime,weburl,createdBy,lastModifiedBy,contentType"
        expand = "fields($select=Title,Link,Attachments,LinkTitle,LinkFilename,Description,Conversation)"

        async for page in self._graph_api_client.scroll(
            f"{GRAPH_API_URL}/sites/{site_id}/lists/{list_id}/items?$select={select}&$expand={expand}"
        ):
            for site_list in page:
                yield site_list

    async def site_list_item_attachments(self, site_web_url, list_title, list_item_id):
        self._validate_sharepoint_rest_url(site_web_url)

        url = f"{site_web_url}/_api/lists/GetByTitle('{list_title}')/items({list_item_id})?$expand=AttachmentFiles"

        try:
            list_item = await self._rest_api_client.fetch(url)

            for attachment in list_item["AttachmentFiles"]:
                yield attachment
        except NotFound:
            # We can safely ignore cause Sharepoint can return 404 in case List Item is of specific types that do not support/have attachments
            # Yes, makes no sense to me either.
            return

    async def download_attachment(self, attachment_absolute_path, async_buffer):
        self._validate_sharepoint_rest_url(attachment_absolute_path)

        await self._rest_api_client.pipe(
            f"{attachment_absolute_path}/$value", async_buffer
        )

    async def site_pages(self, site_web_url):
        self._validate_sharepoint_rest_url(site_web_url)

        # select = "Id,Title,LayoutWebpartsContent,CanvasContent1,Description,Created,AuthorId,Modified,EditorId"
        select = "*,EncodedAbsUrl"  # ^ is what we want, but site pages don't have consistent schemas, and this causes errors. Better to fetch all and slice
        url = f"{site_web_url}/_api/web/lists/GetByTitle('Site%20Pages')/items?$select={select}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for site_page in page:
                    yield {
                        "Id": site_page.get("Id"),
                        "Title": site_page.get("Title"),
                        "webUrl": site_page.get("EncodedAbsUrl"),
                        "LayoutWebpartsContent": site_page.get("LayoutWebpartsContent"),
                        "CanvasContent1": site_page.get("CanvasContent1"),
                        "WikiField": site_page.get("WikiField"),
                        "Description": site_page.get("Description"),
                        "Created": site_page.get("Created"),
                        "AuthorId": site_page.get("AuthorId"),
                        "Modified": site_page.get("Modified"),
                        "EditorId": site_page.get("EditorId"),
                        "odata.id": site_page.get("odata.id"),
                        "OData__UIVersionString": site_page.get(
                            "OData__UIVersionString"
                        ),
                    }
        except NotFound:
            # I'm not sure if site can have no pages, but given how weird API is I put this here
            # Just to be on a safe side
            return

    async def site_page_has_unique_role_assignments(self, site_web_url, site_page_id):
        self._validate_sharepoint_rest_url(site_web_url)

        url = f"{site_web_url}/_api/web/lists/GetByTitle('Site Pages')/items('{site_page_id}')/HasUniqueRoleAssignments"

        try:
            response = await self._rest_api_client.fetch(url)
            return response.get("value", False)
        except NotFound:
            return False

    async def site_page_role_assignments(self, site_web_url, site_page_id):
        self._validate_sharepoint_rest_url(site_web_url)

        expand = "Member/users,RoleDefinitionBindings"

        url = f"{site_web_url}/_api/web/lists/GetByTitle('Site Pages')/items('{site_page_id}')/roleassignments?$expand={expand}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for role_assignment in page:
                    yield role_assignment
        except NotFound:
            return

    async def users_and_groups_for_role_assignment(self, site_web_url, role_assignment):
        self._validate_sharepoint_rest_url(site_web_url)

        if "PrincipalId" not in role_assignment:
            return []

        principal_id = role_assignment["PrincipalId"]

        url = f"{site_web_url}/_api/web/GetUserById('{principal_id}')"

        try:
            return await self._rest_api_client.fetch(url)
        except NotFound:
            return []
        except InternalServerError:
            # This can also mean "not found" so handling it explicitly
            return []

    async def groups_user_transitive_member_of(self, user_id):
        url = f"{GRAPH_API_URL}/users/{user_id}/transitiveMemberOf"

        try:
            async for page in self._graph_api_client.scroll(url):
                for group in page:
                    yield group
        except NotFound:
            return

    async def tenant_details(self):
        url = f"{GRAPH_API_AUTH_URL}/common/userrealm/?user=cj@{self._tenant_name}.onmicrosoft.com&api-version=2.1&checkForMicrosoftAccount=false"

        return await self._rest_api_client.fetch(url)

    def _validate_sharepoint_rest_url(self, url):
        # TODO: make it better suitable for ftest
        if "OVERRIDE_URL" in os.environ:
            return

        # I haven't found a better way to validate tenant name for now.
        actual_tenant_name = self._tenant_name_pattern.findall(url)[0]

        if self._tenant_name != actual_tenant_name:
            msg = f"Unable to call Sharepoint REST API - tenant name is invalid. Authenticated for tenant name: {self._tenant_name}, actual tenant name for the service: {actual_tenant_name}. For url: {url}"
            raise InvalidSharepointTenant(msg)

    async def close(self):
        await self._http_session.close()
        self._graph_api_client.close()
        self._rest_api_client.close()
