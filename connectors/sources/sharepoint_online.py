#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
import re
from collections.abc import Iterable, Sized
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import partial, wraps

import aiofiles
import aiohttp
import fastjsonschema
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ClientResponseError
from fastjsonschema import JsonSchemaValueException

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.es.sink import OP_DELETE, OP_INDEX
from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CacheWithTimeout,
    CancellableSleeps,
    ExtractionService,
    convert_to_b64,
    html_to_text,
    iso_utc,
    iterable_batches_generator,
    retryable,
    url_encode,
)

SPO_API_MAX_BATCH_SIZE = 20

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

DEFAULT_GROUPS = ["Visitors", "Owners", "Members"]

if "OVERRIDE_URL" in os.environ:
    logger.warning("x" * 50)
    logger.warning(
        f"SHAREPOINT ONLINE CONNECTOR CALLS ARE REDIRECTED TO {os.environ['OVERRIDE_URL']}"
    )
    logger.warning("IT'S SUPPOSED TO BE USED ONLY FOR TESTING")
    logger.warning("x" * 50)
    override_url = os.environ["OVERRIDE_URL"]
    GRAPH_API_URL = override_url
    GRAPH_API_AUTH_URL = override_url
    REST_API_AUTH_URL = override_url
else:
    GRAPH_API_URL = "https://graph.microsoft.com/v1.0"
    GRAPH_API_AUTH_URL = "https://login.microsoftonline.com"
    REST_API_AUTH_URL = "https://accounts.accesscontrol.windows.net"

DEFAULT_RETRY_COUNT = 5
DEFAULT_RETRY_SECONDS = 30
DEFAULT_PARALLEL_CONNECTION_COUNT = 10
DEFAULT_BACKOFF_MULTIPLIER = 5
FILE_WRITE_CHUNK_SIZE = 1024 * 64  # 64KB default SSD page size
MAX_DOCUMENT_SIZE = 10485760
WILDCARD = "*"
DRIVE_ITEMS_FIELDS = "id,content.downloadUrl,lastModifiedDateTime,lastModifiedBy,root,deleted,file,folder,package,name,webUrl,createdBy,createdDateTime,size,parentReference"

CURSOR_SITE_DRIVE_KEY = "site_drives"

# Microsoft Graph API Delta constants
# https://learn.microsoft.com/en-us/graph/delta-query-overview

DELTA_NEXT_LINK_KEY = "@odata.nextLink"
DELTA_LINK_KEY = "@odata.deltaLink"


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

    def __init__(self, http_session, tenant_id, tenant_name, client_id, client_secret):
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
        self._client_secret = client_secret

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

        # We measure now before request to be on a pessimistic side
        now = datetime.utcnow()
        try:
            access_token, expires_in = await self._fetch_token()
        except ClientResponseError as e:
            # Both Graph API and REST API return error codes that indicate different problems happening when authenticating.
            # Error Code serves as a good starting point classifying these errors, see the messages below:
            match e.status:
                case 400:
                    raise TokenFetchFailed(
                        "Failed to authorize to Sharepoint REST API. Please verify, that provided Tenant Id, Tenant Name and Client ID are valid."
                    ) from e
                case 401:
                    raise TokenFetchFailed(
                        "Failed to authorize to Sharepoint REST API. Please verify, that provided Secret Value is valid."
                    ) from e
                case _:
                    raise TokenFetchFailed(
                        f"Failed to authorize to Sharepoint REST API. Response Status: {e.status}, Message: {e.message}"
                    ) from e

        self._token_cache.set_value(access_token, now + timedelta(seconds=expires_in))

        return access_token

    async def _fetch_token(self):
        """Fetch token from Microsoft service.

        This method needs to be implemented in the class that inherits MicrosoftSecurityToken.

        Returns:
            (str, int) - a tuple containing access token as a string and number of seconds it will be valid for as an integer
        """

        raise NotImplementedError


class GraphAPIToken(MicrosoftSecurityToken):
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

        async with self._http_session.post(url, headers=headers, data=data) as resp:
            json_response = await resp.json()
            access_token = json_response["access_token"]
            expires_in = int(json_response["expires_in"])

            return access_token, expires_in


class SharepointRestAPIToken(MicrosoftSecurityToken):
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

        async with self._http_session.post(url, headers=headers, data=data) as resp:
            json_response = await resp.json()
            access_token = json_response["access_token"]
            expires_in = int(json_response["expires_in"])

            return access_token, expires_in


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
        self._logger.debug(f"Post to url: {url}")
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
                yield resp
        except aiohttp.client_exceptions.ClientOSError:
            self._logger.warning(
                "The Microsoft Graph API dropped the connection. It might indicate that the connector is making too many requests. Decrease concurrency settings, otherwise the Graph API may block this app."
            )
            raise
        except ClientResponseError as e:
            await self._handle_client_response_error(absolute_url, e, retry_count)

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

    async def _handle_client_response_error(self, absolute_url, e, retry_count):
        if e.status == 429 or e.status == 503:
            response_headers = e.headers or {}

            if "Retry-After" in response_headers:
                retry_after = int(response_headers["Retry-After"])
            else:
                self._logger.warning(
                    f"Response Code from Sharepoint Server is {e.status} but Retry-After header is not found, using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
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
            raise PermissionsMissing(
                f"Received Unauthorized response for {absolute_url}.\nVerify that the correct Graph API and Sharepoint permissions are granted to the app and admin consent is given. If the permissions and consent are correct, wait for several minutes and try again."
            ) from e
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
            return retry_after * retry_count * backoff


class SharepointOnlineClient:
    def __init__(self, tenant_id, tenant_name, client_id, client_secret):
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

        self.graph_api_token = GraphAPIToken(
            self._http_session, tenant_id, tenant_name, client_id, client_secret
        )
        self.rest_api_token = SharepointRestAPIToken(
            self._http_session, tenant_id, tenant_name, client_id, client_secret
        )

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
        filter_ = url_encode("siteCollection/root ne null")
        select = "siteCollection,webUrl"

        async for page in self._graph_api_client.scroll(
            f"{GRAPH_API_URL}/sites/?$filter={filter_}&$select={select}"
        ):
            for site_collection in page:
                yield site_collection

    async def user_information_list(self, site_id):
        expand = "fields"
        url = f"{GRAPH_API_URL}/sites/{site_id}/lists/User Information List/items?expand={expand}"

        try:
            async for page in self._graph_api_client.scroll(url):
                for user_information in page:
                    yield user_information
        except NotFound:
            return

    async def user(self, user_principal_name):
        url = f"{GRAPH_API_URL}/users/{user_principal_name}"

        try:
            return await self._graph_api_client.fetch(url)
        except NotFound:
            return {}

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
        url = f"{GRAPH_API_URL}/groups/{group_id}/owners"

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

    async def sites(self, parent_site_id, allowed_root_sites, enumerate_all_sites=True):
        select = ""

        if allowed_root_sites == [WILDCARD] or enumerate_all_sites:
            async for page in self._graph_api_client.scroll(
                f"{GRAPH_API_URL}/sites/{parent_site_id}/sites?search=*&$select={select}"
            ):
                for site in page:
                    # Filter out site collections that are not needed
                    if (
                        WILDCARD not in allowed_root_sites
                        and site["name"] not in allowed_root_sites
                    ):
                        continue
                    yield site
        else:
            self._logger.debug(f"Looking up sites: {allowed_root_sites} individually")
            for allowed_site in allowed_root_sites:
                try:
                    self._logger.debug(
                        f"Requesting site '{allowed_site}' by relative path in parent site: {parent_site_id}"
                    )
                    site = await self._graph_api_client.fetch(
                        f"{GRAPH_API_URL}/sites/{parent_site_id}:/sites/{allowed_site}"
                    )
                    yield site
                except NotFound:
                    self._logger.warning(
                        f"Could not look up site '{allowed_site}' by relative path in parent site: {parent_site_id}"
                    )

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

    async def drive_item_permissions(self, drive_id, item_id):
        try:
            async for page in self._graph_api_client.scroll(
                f"{GRAPH_API_URL}/drives/{drive_id}/items/{item_id}/permissions"
            ):
                for permission in page:
                    yield permission
        except NotFound:
            return

    async def drive_items_permissions_batch(self, drive_id, drive_item_ids):
        requests = []

        for item_id in drive_item_ids:
            permissions_uri = f"/drives/{drive_id}/items/{item_id}/permissions"
            requests.append({"id": item_id, "method": "GET", "url": permissions_uri})

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
            raise InvalidSharepointTenant(
                f"Unable to call Sharepoint REST API - tenant name is invalid. Authenticated for tenant name: {self._tenant_name}, actual tenant name for the service: {actual_tenant_name}."
            )

    async def close(self):
        await self._http_session.close()
        self._graph_api_client.close()
        self._rest_api_client.close()


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


class SharepointOnlineAdvancedRulesValidator(AdvancedRulesValidator):
    SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "skipExtractingDriveItemsOlderThan": {"type": "integer"},  # in Days
        },
        "additionalProperties": False,
    }

    SCHEMA = fastjsonschema.compile(definition=SCHEMA_DEFINITION)

    async def validate(self, advanced_rules):
        try:
            SharepointOnlineAdvancedRulesValidator.SCHEMA(advanced_rules)

            return SyncRuleValidationResult.valid_result(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES
            )
        except JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"{e.message}. Make sure advanced filtering rules follow the following schema: {SharepointOnlineAdvancedRulesValidator.SCHEMA_DEFINITION['properties']}",
            )


def _prefix_group(group):
    return prefix_identity("group", group)


def _prefix_user(user):
    return prefix_identity("user", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_email(email):
    return prefix_identity("email", email)


def _postfix_group(group):
    if group is None:
        return None

    return f"{group} Members"


def is_domain_group(user_fields):
    return user_fields.get(
        "ContentType"
    ) == "DomainGroup" and "federateddirectoryclaimprovider" in user_fields.get(
        "Name", ""
    )


def is_person(user_fields):
    return user_fields["ContentType"] == "Person"


def _domain_group_id(user_info_name):
    """Extracts the domain group id.

    The domain group id can have the following formats:
    - abc|def|domain-group-id
    - abc|def|some-prefix/domain-group-id

    Returns:
        str: domain group id

    """
    if user_info_name is None or len(user_info_name) == 0:
        return None

    name_parts = user_info_name.split("|")

    if len(name_parts) <= 2:
        return None

    domain_group_id = name_parts[2]

    if "/" in domain_group_id:
        domain_group_id = domain_group_id.split("/")[1]

    if "_" in domain_group_id:
        domain_group_id = domain_group_id.split("_")[0]

    if len(domain_group_id) == 0:
        return None

    return domain_group_id


async def _emails_and_usernames_of_domain_group(
    domain_group_id, group_identities_generator
):
    """Yield emails and/or usernames for a specific domain group.
    This function yields both to reduce the number of remote calls made to the group owners or group members API.

    Yields:
        Tuple: tuple of the user's email and the user's username

    """
    async for identity in group_identities_generator(domain_group_id):
        email = identity.get("mail")
        username = identity.get("userPrincipalName")

        yield email, username


def _get_login_name(raw_login_name):
    if raw_login_name and (
        raw_login_name.startswith("i:0#.f|membership|")
        or raw_login_name.startswith("c:0o.c|federateddirectoryclaimprovider|")
    ):
        parts = raw_login_name.split("|")

        if len(parts) > 2:
            return parts[2]

    return None


class SharepointOnlineDataSource(BaseDataSource):
    """Sharepoint Online"""

    name = "Sharepoint Online"
    service_type = "sharepoint_online"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

        self._client = None

        if self.configuration["use_text_extraction_service"]:
            self.extraction_service = ExtractionService()
            self.download_dir = self.extraction_service.get_volume_dir()
        else:
            self.extraction_service = None
            self.download_dir = None

    def _set_internal_logger(self):
        self.client.set_logger(self._logger)

    @property
    def client(self):
        if not self._client:
            tenant_id = self.configuration["tenant_id"]
            tenant_name = self.configuration["tenant_name"]
            client_id = self.configuration["client_id"]
            client_secret = self.configuration["secret_value"]

            self._client = SharepointOnlineClient(
                tenant_id, tenant_name, client_id, client_secret
            )

        return self._client

    @classmethod
    def get_default_configuration(cls):
        return {
            "tenant_id": {
                "label": "Tenant ID",
                "order": 1,
                "type": "str",
            },
            "tenant_name": {  # TODO: when Tenant API is going out of Beta, we can remove this field
                "label": "Tenant name",
                "order": 2,
                "type": "str",
            },
            "client_id": {
                "label": "Client ID",
                "order": 3,
                "type": "str",
            },
            "secret_value": {
                "label": "Secret value",
                "order": 4,
                "sensitive": True,
                "type": "str",
            },
            "site_collections": {
                "display": "textarea",
                "label": "Comma-separated list of sites",
                "tooltip": "A comma-separated list of sites to ingest data from. Use * to include all available sites.",
                "order": 5,
                "type": "list",
            },
            "enumerate_all_sites": {
                "display": "toggle",
                "lavel": "Enumerate all sites?",
                "tooltip": 'Whether sites should be fetched by name from "all sites". If disabled, each configured site will be fetched with an individual request.',
                "order": 6,
                "type": "bool",
                "value": True,
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 7,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "value": False,
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 8,
                "tooltip": "Document level security ensures identities and permissions set in Sharepoint Online are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
            "fetch_drive_item_permissions": {
                "depends_on": [{"field": "use_document_level_security", "value": True}],
                "display": "toggle",
                "label": "Fetch drive item permissions",
                "order": 9,
                "tooltip": "Enable this option to fetch drive item specific permissions. This setting can increase sync time.",
                "type": "bool",
                "value": True,
            },
            "fetch_unique_page_permissions": {
                "depends_on": [{"field": "use_document_level_security", "value": True}],
                "display": "toggle",
                "label": "Fetch unique page permissions",
                "order": 10,
                "tooltip": "Enable this option to fetch unique page permissions. This setting can increase sync time. If this setting is disabled a page will inherit permissions from its parent site.",
                "type": "bool",
                "value": True,
            },
            "fetch_unique_list_permissions": {
                "depends_on": [{"field": "use_document_level_security", "value": True}],
                "display": "toggle",
                "label": "Fetch unique list permissions",
                "order": 11,
                "tooltip": "Enable this option to fetch unique list permissions. This setting can increase sync time. If this setting is disabled a list will inherit permissions from its parent site.",
                "type": "bool",
                "value": True,
            },
            "fetch_unique_list_item_permissions": {
                "depends_on": [{"field": "use_document_level_security", "value": True}],
                "display": "toggle",
                "label": "Fetch unique list item permissions",
                "order": 12,
                "tooltip": "Enable this option to fetch unique list item permissions. This setting can increase sync time. If this setting is disabled a list item will inherit permissions from its parent site.",
                "type": "bool",
                "value": True,
            },
        }

    async def validate_config(self):
        await super().validate_config()

        # Check that we can log in into Graph API
        await self.client.graph_api_token.get()

        # Check that we can log in into Sharepoint REST API
        await self.client.rest_api_token.get()

        # Check that tenant name is valid
        # Sadly we don't check that tenant name is actually the name
        # For the tenant id.
        # Seems like there's an API that allows this, but it's only in beta:
        # https://learn.microsoft.com/en-us/graph/api/managedtenants-tenant-get?view=graph-rest-beta&tabs=http
        # It also might not work cause permissions there are only delegated
        tenant_details = await self.client.tenant_details()

        if tenant_details is None or tenant_details["NameSpaceType"] == "Unknown":
            raise Exception(
                f"Could not find tenant with name {self.configuration['tenant_name']}. Make sure that provided tenant name is valid."
            )

        # Check that we at least have permissions to fetch sites and actual site names are correct
        configured_root_sites = self.configuration["site_collections"]
        if WILDCARD in configured_root_sites:
            return

        retrieved_sites = []

        async for site_collection in self.client.site_collections():
            async for site in self.client.sites(
                site_collection["siteCollection"]["hostname"],
                configured_root_sites,
                self.configuration["enumerate_all_sites"],
            ):
                retrieved_sites.append(self._site_path_from_web_url(site["webUrl"]))

        missing = [x for x in configured_root_sites if x not in retrieved_sites]

        if missing:
            raise Exception(
                f"The specified SharePoint sites [{', '.join(missing)}] could not be retrieved during sync. Examples of sites available on the tenant:[{', '.join(retrieved_sites[:5])}]."
            )

    def _site_path_from_web_url(self, web_url):
        url_parts = web_url.split("/sites/")
        site_path_parts = url_parts[1:]
        return "/sites/".join(
            site_path_parts
        )  # just in case there was a /sites/ in the site path

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )

        return document

    async def _site_access_control(self, site):
        """Fetches all permissions for all owners, members and visitors of a given site.
        All groups and/or persons, which have permissions for a given site are returned with their given identity prefix ("user", "group" or "email").
        For the given site all groups and its corresponding members and owners (username and/or email) are fetched.

        Returns:
            list: access control list for a given site
            [
                "user:spo-user",
                "email:some.user@spo.com",
                "group:1234-abcd-id"
            ]
        """

        self._logger.debug(f"Looking at site: {site['id']}")
        if not self._dls_enabled():
            return [], []

        def _is_site_admin(user):
            return user.get("IsSiteAdmin", False)

        access_control = set()
        site_admins_access_control = set()

        async for user_information in self.client.user_information_list(site["id"]):
            user = user_information["fields"]

            user_access_control = set()

            if is_domain_group(user):
                self._logger.debug(f"It is a domain group with name: {user['Name']}")
                domain_group_id = _domain_group_id(user["Name"])
                self._logger.debug(f"Detected domain groupId as: {domain_group_id}")

                if domain_group_id:
                    user_access_control.add(_prefix_group(domain_group_id))

            if is_person(user):
                login_name = _get_login_name(user.get("Name"))

                if login_name:
                    user_access_control.add(_prefix_user(login_name))

                email = user.get("EMail")

                if email:
                    user_access_control.add(_prefix_email(email))

            if _is_site_admin(user):
                site_admins_access_control |= user_access_control

            access_control |= user_access_control

        return list(access_control), list(site_admins_access_control)

    def _dls_enabled(self):
        if self._features is None:
            return False

        if not self._features.document_level_security_enabled():
            return False

        return self.configuration["use_document_level_security"]

    def access_control_query(self, access_control):
        return es_access_control_query(access_control)

    async def _user_access_control_doc(self, user):
        """Constructs a user access control document, which will be synced to the corresponding access control index.
        The `_id` of the user access control document will either be the username (can also be the email sometimes) or the email itself.
        Note: the `_id` field won't be prefixed with the corresponding identity prefix ("user" or "email").
        The document contains all groups of a user and his email and/or username under `query.template.params.access_control`.

        Returns:
            dict: dictionary representing an user access control document
            {
                "_id": "some.user@spo.com",
                "identity": {
                    "email": "email:some.user@spo.com",
                    "username": "user:some.user",
                    "user_id": "user_id:some user id"
                },
                "created_at": "2023-06-30 12:00:00",
                "query": {
                    "template": {
                        "params": {
                            "access_control": [
                                "email:some.user@spo.com",
                                "user:some.user",
                                "group:1234-abcd-id"
                            ]
                        }
                    }
                }
            }
        """

        if "UserName" in user:
            username_field = "UserName"
        elif "userPrincipalName" in user:
            username_field = "userPrincipalName"
        else:
            return

        email = user.get("EMail", user.get("mail", None))
        username = user[username_field]
        prefixed_groups = set()

        expanded_member_groups = user.get("transitiveMemberOf", [])
        if (
            len(expanded_member_groups) < 100
        ):  # $expand param has a max of 100: see: https://learn.microsoft.com/en-us/graph/known-issues#query-parameters
            for group in expanded_member_groups:
                prefixed_groups.add(_prefix_group(group.get("id", None)))
        else:
            self._logger.debug(
                f"User {username}: {email} belongs to a lot of groups - paging them separately"
            )
            async for group in self.client.groups_user_transitive_member_of(user["id"]):
                group_id = group["id"]
                if group_id:
                    prefixed_groups.add(_prefix_group(group_id))

        prefixed_mail = _prefix_email(email)
        prefixed_username = _prefix_user(username)
        prefixed_user_id = _prefix_user_id(user.get("id"))
        id_ = email if email else username

        access_control = list({prefixed_mail, prefixed_username}.union(prefixed_groups))

        if "createdDateTime" in user:
            created_at = datetime.strptime(user["createdDateTime"], TIMESTAMP_FORMAT)
        else:
            created_at = iso_utc()

        return {
            # For `_id` we're intentionally using the email/username without the prefix
            "_id": id_,
            "identity": {
                "email": prefixed_mail,
                "username": prefixed_username,
                "user_id": prefixed_user_id,
            },
            "created_at": created_at,
        } | self.access_control_query(access_control)

    async def get_access_control(self):
        """Yields an access control document for every user of a site.
        Note: this method will cache users and emails it has already and skip the ingestion for those.

        Yields:
             dict: dictionary representing a user access control document
        """

        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        already_seen_ids = set()

        def _already_seen(*ids):
            for id_ in ids:
                if id_ in already_seen_ids:
                    self._logger.debug(f"We've already seen {id_}")
                    return True

            return False

        def update_already_seen(*ids):
            for id_ in ids:
                # We want to make sure to not add 'None' to the already seen sets
                if id_:
                    already_seen_ids.add(id_)

        async def process_user(user):
            email = user.get("EMail", user.get("mail", None))
            username = user.get("UserName", user.get("userPrincipalName", None))
            self._logger.debug(f"Detected a person: {username}: {email}")

            if _already_seen(email, username):
                return None

            update_already_seen(email, username)

            person_access_control_doc = await self._user_access_control_doc(user)
            if person_access_control_doc:
                return person_access_control_doc

        self._logger.info("Fetching all users")
        async for user in self.client.active_users_with_groups():
            user_doc = await process_user(user)
            if user_doc:
                yield user_doc

    async def _drive_items_batch_with_permissions(self, drive_id, drive_items_batch):
        """Decorate a batch of drive items with their permissions using one API request.

        Args:
            drive_id (int): id of the drive, where the drive items reside
            drive_items_batch (list): list of drive items to decorate with permissions

        Yields:
            drive_item (dict): drive item with or without permissions depending on the config value of `fetch_drive_item_permissions`
        """

        if not self.configuration["fetch_drive_item_permissions"]:
            for drive_item in drive_items_batch:
                yield drive_item

            return

        ids_to_items = {
            drive_item["id"]: drive_item for drive_item in drive_items_batch
        }
        drive_items_ids = list(ids_to_items.keys())

        async for permissions_response in self.client.drive_items_permissions_batch(
            drive_id, drive_items_ids
        ):
            drive_item_id = permissions_response.get("id")
            drive_item = ids_to_items.get(drive_item_id)
            permissions = permissions_response.get("body", {}).get("value", [])

            if drive_item:
                yield self._with_drive_item_permissions(drive_item, permissions)

    async def get_docs(self, filtering=None):
        max_drive_item_age = None

        self.init_sync_cursor()

        if filtering is not None and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            max_drive_item_age = advanced_rules["skipExtractingDriveItemsOlderThan"]

        async for site_collection in self.site_collections():
            yield site_collection, None

            async for site in self.sites(
                site_collection["siteCollection"]["hostname"],
                self.configuration["site_collections"],
            ):
                (
                    site_access_control,
                    site_admin_access_control,
                ) = await self._site_access_control(site)

                yield self._decorate_with_access_control(
                    site, site_access_control
                ), None

                async for site_drive in self.site_drives(site):
                    yield self._decorate_with_access_control(
                        site_drive, site_access_control
                    ), None

                    async for page in self.client.drive_items(site_drive["id"]):
                        for drive_items_batch in iterable_batches_generator(
                            page.items, SPO_API_MAX_BATCH_SIZE
                        ):
                            async for drive_item in self._drive_items_batch_with_permissions(
                                site_drive["id"], drive_items_batch
                            ):
                                drive_item["_id"] = drive_item["id"]
                                drive_item["object_type"] = "drive_item"
                                drive_item["_timestamp"] = drive_item.get(
                                    "lastModifiedDateTime"
                                )

                                # Drive items should inherit site access controls only if
                                # 'fetch_drive_item_permissions' is disabled in the config
                                if not self.configuration[
                                    "fetch_drive_item_permissions"
                                ]:
                                    drive_item = self._decorate_with_access_control(
                                        drive_item, site_access_control
                                    )

                                yield drive_item, self.download_function(
                                    drive_item, max_drive_item_age
                                )

                        self.update_drive_delta_link(
                            drive_id=site_drive["id"], link=page.delta_link()
                        )

                # Sync site list and site list items
                async for site_list in self.site_lists(site, site_access_control):
                    # Always include site admins in site list access controls
                    site_list = self._decorate_with_access_control(
                        site_list, site_admin_access_control
                    )
                    yield site_list, None

                    async for list_item, download_func in self.site_list_items(
                        site=site,
                        site_list_id=site_list["id"],
                        site_list_name=site_list["name"],
                        site_access_control=site_access_control,
                    ):
                        # Always include site admins in list item access controls
                        list_item = self._decorate_with_access_control(
                            list_item, site_admin_access_control
                        )
                        yield list_item, download_func

                # Sync site pages
                async for site_page in self.site_pages(site, site_access_control):
                    # Always include site admins in site page access controls
                    site_page = self._decorate_with_access_control(
                        site_page, site_admin_access_control
                    )
                    yield site_page, None

    async def get_docs_incrementally(self, sync_cursor, filtering=None):
        self._sync_cursor = sync_cursor

        if not self._sync_cursor:
            raise SyncCursorEmpty(
                "Unable to start incremental sync. Please perform a full sync to re-enable incremental syncs."
            )

        max_drive_item_age = None

        if filtering is not None and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            max_drive_item_age = advanced_rules["skipExtractingDriveItemsOlderThan"]

        async for site_collection in self.site_collections():
            yield site_collection, None, OP_INDEX

            async for site in self.sites(
                site_collection["siteCollection"]["hostname"],
                self.configuration["site_collections"],
            ):
                (
                    site_access_control,
                    site_admin_access_control,
                ) = await self._site_access_control(site)

                yield self._decorate_with_access_control(
                    site, site_access_control
                ), None, OP_INDEX

                async for site_drive in self.site_drives(site):
                    yield self._decorate_with_access_control(
                        site_drive, site_access_control
                    ), None, OP_INDEX

                    delta_link = self.get_drive_delta_link(site_drive["id"])

                    async for page in self.client.drive_items(
                        drive_id=site_drive["id"], url=delta_link
                    ):
                        for drive_items_batch in iterable_batches_generator(
                            page.items, SPO_API_MAX_BATCH_SIZE
                        ):
                            async for drive_item in self._drive_items_batch_with_permissions(
                                site_drive["id"], drive_items_batch
                            ):
                                drive_item["_id"] = drive_item["id"]
                                drive_item["object_type"] = "drive_item"
                                drive_item["_timestamp"] = drive_item.get(
                                    "lastModifiedDateTime"
                                )

                                # Drive items should inherit site access controls only if
                                # 'fetch_drive_item_permissions' is disabled in the config
                                if not self.configuration[
                                    "fetch_drive_item_permissions"
                                ]:
                                    drive_item = self._decorate_with_access_control(
                                        drive_item, site_access_control
                                    )

                                yield drive_item, self.download_function(
                                    drive_item, max_drive_item_age
                                ), self.drive_item_operation(drive_item)

                        self.update_drive_delta_link(
                            drive_id=site_drive["id"], link=page.delta_link()
                        )

                # Sync site list and site list items
                async for site_list in self.site_lists(site, site_access_control):
                    # Always include site admins in site list access controls
                    site_list = self._decorate_with_access_control(
                        site_list, site_admin_access_control
                    )
                    yield site_list, None, OP_INDEX

                    async for list_item, download_func in self.site_list_items(
                        site=site,
                        site_list_id=site_list["id"],
                        site_list_name=site_list["name"],
                        site_access_control=site_access_control,
                    ):
                        # Always include site admins in list item access controls
                        list_item = self._decorate_with_access_control(
                            list_item, site_admin_access_control
                        )
                        yield list_item, download_func, OP_INDEX

                # Sync site pages
                async for site_page in self.site_pages(site, site_access_control):
                    # Always include site admins in site page access controls
                    site_page = self._decorate_with_access_control(
                        site_page, site_admin_access_control
                    )
                    yield site_page, None, OP_INDEX

    async def site_collections(self):
        async for site_collection in self.client.site_collections():
            site_collection["_id"] = site_collection["webUrl"]
            site_collection["object_type"] = "site_collection"

            yield site_collection

    async def sites(self, hostname, collections):
        async for site in self.client.sites(
            hostname,
            collections,
            enumerate_all_sites=self.configuration["enumerate_all_sites"],
        ):  # TODO: simplify and eliminate root call
            site["_id"] = site["id"]
            site["object_type"] = "site"

            yield site

    async def site_drives(self, site):
        async for site_drive in self.client.site_drives(site["id"]):
            site_drive["_id"] = site_drive["id"]
            site_drive["object_type"] = "site_drive"

            yield site_drive

    def _with_drive_item_permissions(self, drive_item, drive_item_permissions):
        """Decorates a drive item with its permissions.

        Args:
            drive_item (dict): drive item to fetch the permissions for.
            drive_item_permissions (list): drive item permissions to add to the drive_item.

        Returns:
            drive_item (dict): drive item decorated with its permissions.

        Example permissions for a drive item:

        {
              ...
              "grantedTo": { ... },
              "grantedToV2": {
                "user": {
                  "id": "5D33DD65C6932946",
                  "displayName": "Robin Danielsen"
                },
                "siteUser": {
                  "id": "1",
                  "displayName": "Robin Danielsen",
                  "loginName": "Robin Danielsen"
                },
                "group": {
                  "id": "23234DAJFKA234",
                  "displayName": "Some group",
                },
                "siteGroup": {
                  "id": "2",
                  "displayName": "Some group"
                }
              }
        }

        "grantedTo" has been deprecated, so we only fetch the permissions under "grantedToV2".
        A drive item can have six different identities assigned to it: "application", "device", "group", "user", "siteGroup" and "siteUser".
        In this context we'll only fetch "group", "user", "siteGroup" and "siteUser" and prefix them with different strings to make them distinguishable from each other.

        Note: A "siteUser" can be related to a "user", but not necessarily (same for "group" and "siteGroup").
        """
        if not self.configuration["fetch_drive_item_permissions"]:
            return drive_item

        def _get_id(permissions, identity):
            if identity not in permissions:
                return None

            return permissions.get(identity).get("id")

        drive_item_id = drive_item.get("id")
        access_control = []

        for permission in drive_item_permissions:
            granted_to_v2 = permission.get("grantedToV2")

            if not granted_to_v2:
                self._logger.debug(
                    f"'grantedToV2' missing for drive item (id: '{drive_item_id}'). Skipping permissions..."
                )
                continue

            user_id = _get_id(granted_to_v2, "user")
            group_id = _get_id(granted_to_v2, "group")

            if user_id:
                access_control.append(_prefix_user_id(user_id))

            if group_id:
                access_control.append(_prefix_group(group_id))

        return self._decorate_with_access_control(drive_item, access_control)

    async def drive_items(self, site_drive, max_drive_item_age):
        async for page in self.client.drive_items(site_drive["id"]):
            for drive_item in page:
                drive_item["_id"] = drive_item["id"]
                drive_item["object_type"] = "drive_item"
                drive_item["_timestamp"] = drive_item["lastModifiedDateTime"]

                yield drive_item, self.download_function(drive_item, max_drive_item_age)

    async def site_list_items(
        self, site, site_list_id, site_list_name, site_access_control
    ):
        site_id = site.get("id")
        site_web_url = site.get("webUrl")
        site_collection = site.get("siteCollection", {}).get("hostname")
        async for list_item in self.client.site_list_items(site_id, site_list_id):
            # List Item IDs are unique within list.
            # Therefore we mix in site_list id to it to make sure they are
            # globally unique.
            # Also we need to remember original ID because when a document
            # is yielded, its "id" field is overwritten with content of "_id" field
            list_item_natural_id = list_item["id"]
            list_item["_id"] = f"{site_list_id}-{list_item['id']}"
            list_item["object_type"] = "list_item"

            content_type = list_item["contentType"]["name"]

            if content_type in [
                "Web Template Extensions",
                "Client Side Component Manifests",
            ]:  # TODO: make it more flexible. For now I ignore them cause they 404 all the time
                continue

            has_unique_role_assignments = False

            if (
                self._dls_enabled()
                and self.configuration["fetch_unique_list_item_permissions"]
            ):
                has_unique_role_assignments = (
                    await self.client.site_list_item_has_unique_role_assignments(
                        site_web_url, site_list_name, list_item_natural_id
                    )
                )

                if has_unique_role_assignments:
                    self._logger.debug(
                        f"Fetching unique permissions for list item with id '{list_item_natural_id}'. Ignoring parent site permissions."
                    )

                    list_item_access_control = []

                    async for role_assignment in self.client.site_list_item_role_assignments(
                        site_web_url, site_list_name, list_item_natural_id
                    ):
                        list_item_access_control.extend(
                            await self._get_access_control_from_role_assignment(
                                role_assignment
                            )
                        )

                    list_item = self._decorate_with_access_control(
                        list_item, list_item_access_control
                    )

            if not has_unique_role_assignments:
                list_item = self._decorate_with_access_control(
                    list_item, site_access_control
                )

            if "Attachments" in list_item["fields"]:
                async for list_item_attachment in self.client.site_list_item_attachments(
                    site_web_url, site_list_name, list_item_natural_id
                ):
                    list_item_attachment["_id"] = list_item_attachment["odata.id"]
                    list_item_attachment["object_type"] = "list_item_attachment"
                    list_item_attachment["_timestamp"] = list_item[
                        "lastModifiedDateTime"
                    ]
                    list_item_attachment[
                        "_original_filename"
                    ] = list_item_attachment.get("FileName", "")
                    if (
                        "ServerRelativePath" in list_item_attachment
                        and "DecodedUrl"
                        in list_item_attachment.get("ServerRelativePath", {})
                    ):
                        list_item_attachment[
                            "webUrl"
                        ] = f"https://{site_collection}{list_item_attachment['ServerRelativePath']['DecodedUrl']}"
                    else:
                        self._logger.debug(
                            f"Unable to populate webUrl for list item attachment {list_item_attachment['_id']}"
                        )

                    if self._dls_enabled():
                        list_item_attachment[ACCESS_CONTROL] = list_item.get(
                            ACCESS_CONTROL, []
                        )

                    attachment_download_func = partial(
                        self.get_attachment_content, list_item_attachment
                    )
                    yield list_item_attachment, attachment_download_func

            yield list_item, None

    async def site_lists(self, site, site_access_control):
        async for site_list in self.client.site_lists(site["id"]):
            site_list["_id"] = site_list["id"]
            site_list["object_type"] = "site_list"
            site_url = site["webUrl"]
            site_list_name = site_list["name"]

            has_unique_role_assignments = False

            if (
                self._dls_enabled()
                and self.configuration["fetch_unique_list_permissions"]
            ):
                has_unique_role_assignments = (
                    await self.client.site_list_has_unique_role_assignments(
                        site_url, site_list_name
                    )
                )

                if has_unique_role_assignments:
                    self._logger.debug(
                        f"Fetching unique list permissions for list with id '{site_list['_id']}'. Ignoring parent site permissions."
                    )

                    site_list_access_control = []

                    async for role_assignment in self.client.site_list_role_assignments(
                        site_url, site_list_name
                    ):
                        site_list_access_control.extend(
                            await self._get_access_control_from_role_assignment(
                                role_assignment
                            )
                        )

                    site_list = self._decorate_with_access_control(
                        site_list, site_list_access_control
                    )

            if not has_unique_role_assignments:
                site_list = self._decorate_with_access_control(
                    site_list, site_access_control
                )

            yield site_list

    async def _get_access_control_from_role_assignment(self, role_assignment):
        """Extracts access control from a role assignment.

        Args:
            role_assignment (dict): dictionary representing a role assignment.

        Returns:
            access_control (list): list of usernames and dynamic group ids, which have the role assigned.

        A role can be assigned to a user directly or to a group (and therefore indirectly to the users beneath).
        If any role is assigned to a user this means at least "read" access.
        """

        def _access_control_for_user(user_):
            user_access_control = []

            user_principal_name = user_.get("UserPrincipalName")
            login_name = _get_login_name(user_.get("LoginName"))

            if user_principal_name:
                user_access_control.append(_prefix_user(user_principal_name))

            if login_name:
                user_access_control.append(_prefix_user(login_name))

            return user_access_control

        access_control = []

        identity_type = role_assignment.get("Member", {}).get("odata.type", "")
        is_group = identity_type == "SP.Group"
        is_user = identity_type == "SP.User"

        if is_group:
            users = role_assignment.get("Member", {}).get("Users", [])

            for user in users:
                access_control.extend(_access_control_for_user(user))
        elif is_user:
            user = role_assignment.get("Member", {})
            login_name = user.get("LoginName")

            # in this context the 'odata.type' being 'SP.User' and the 'LoginName' looking like a group indicates a dynamic group
            is_dynamic_group = (
                login_name.startswith("c:0o.c|federateddirectoryclaimprovider|")
                if login_name
                else False
            )

            if is_dynamic_group:
                self._logger.debug(f"Detected dynamic group '{user.get('Title')}'.")
                dynamic_group_id = _get_login_name(login_name)
                access_control.append(_prefix_group(dynamic_group_id))
            else:
                access_control = _access_control_for_user(user)
        else:
            self._logger.debug(
                f"Skipping unique page permissions for identity type '{identity_type}'."
            )

        return access_control

    async def site_pages(self, site, site_access_control):
        site_id = site["id"]
        url = site["webUrl"]
        async for site_page in self.client.site_pages(url):
            # site page object has multiple ids:
            # - Id - not globally unique, just an increment, e.g. 1, 2, 3, 4
            # - GUID - not globally unique, though it's a real guid
            # - odata.id - not even sure what this id is
            # Therefore, we generate id combining unique site id with site page id that is unique within this site
            # Careful with format - changing other ids can overlap with this one if they follow the format of:
            # {site_id}-{some_name_or_string_id}-{autoincremented_id}
            site_page["_id"] = f"{site_id}-site_page-{site_page['Id']}"
            site_page["object_type"] = "site_page"

            has_unique_role_assignments = False

            # ignore parent site permissions and use unique per page permissions ("unique permissions" means breaking the inheritance to the parent site)
            if (
                self._dls_enabled()
                and self.configuration["fetch_unique_page_permissions"]
            ):
                has_unique_role_assignments = (
                    await self.client.site_page_has_unique_role_assignments(
                        url, site_page["Id"]
                    )
                )

                if has_unique_role_assignments:
                    self._logger.debug(
                        f"Fetching unique page permissions for page with id '{site_page['_id']}'. Ignoring parent site permissions."
                    )

                    page_access_control = []

                    async for role_assignment in self.client.site_page_role_assignments(
                        url, site_page["Id"]
                    ):
                        page_access_control.extend(
                            await self._get_access_control_from_role_assignment(
                                role_assignment
                            )
                        )

                    site_page = self._decorate_with_access_control(
                        site_page, page_access_control
                    )

            # set parent site access control
            if not has_unique_role_assignments:
                site_page = self._decorate_with_access_control(
                    site_page, site_access_control
                )

            for html_field in ["LayoutWebpartsContent", "CanvasContent1", "WikiField"]:
                if html_field in site_page:
                    site_page[html_field] = html_to_text(site_page[html_field])

            yield site_page

    def init_sync_cursor(self):
        if not self._sync_cursor:
            self._sync_cursor = {CURSOR_SITE_DRIVE_KEY: {}}

        return self._sync_cursor

    def update_drive_delta_link(self, drive_id, link):
        if not link:
            return

        self._sync_cursor[CURSOR_SITE_DRIVE_KEY][drive_id] = link

    def get_drive_delta_link(self, drive_id):
        return self._sync_cursor.get(CURSOR_SITE_DRIVE_KEY, {}).get(drive_id)

    def drive_item_operation(self, item):
        if "deleted" in item:
            return OP_DELETE
        else:
            return OP_INDEX

    def download_function(self, drive_item, max_drive_item_age):
        if "deleted" in drive_item:
            # deleted drive items do not contain `name` property in the payload
            # so drive_item['id'] is used
            self._logger.debug(
                f"Not downloading the item id={drive_item['id']} because it has been deleted"
            )

            return None

        if "folder" in drive_item:
            self._logger.debug(f"Not downloading folder {drive_item['name']}")
            return None

        if "@microsoft.graph.downloadUrl" not in drive_item:
            self._logger.debug(
                f"Not downloading file {drive_item['name']}: field \"@microsoft.graph.downloadUrl\" is missing"
            )
            return None

        if not self.is_supported_format(drive_item["name"]):
            self._logger.debug(
                f"Not downloading file {drive_item['name']}: file type is not supported"
            )
            return None

        if "lastModifiedDateTime" not in drive_item:
            self._logger.debug(
                f"Not downloading file {drive_item['name']}: field \"lastModifiedDateTime\" is missing"
            )
            return None

        modified_date = datetime.strptime(
            drive_item["lastModifiedDateTime"], TIMESTAMP_FORMAT
        )

        if max_drive_item_age and modified_date < datetime.utcnow() - timedelta(
            days=max_drive_item_age
        ):
            self._logger.warning(
                f"Not downloading file {drive_item['name']}: last modified on {drive_item['lastModifiedDateTime']}"
            )

            return None
        elif (
            drive_item["size"] > MAX_DOCUMENT_SIZE
            and not self.configuration["use_text_extraction_service"]
        ):
            self._logger.warning(
                f"Not downloading file {drive_item['name']} of size {drive_item['size']}"
            )

            return None
        else:
            drive_item["_original_filename"] = drive_item.get("name", "")
            return partial(self.get_drive_item_content, drive_item)

    async def get_attachment_content(self, attachment, timestamp=None, doit=False):
        if not doit:
            return

        if not self.is_supported_format(attachment["_original_filename"]):
            self._logger.debug(
                f"Not downloading attachment {attachment['_original_filename']}: file type is not supported"
            )
            return

        # We don't know attachment sizes unfortunately, so cannot properly ignore them

        # Okay this gets weird.
        # There's no way to learn whether List Item Attachment changed or not
        # Response does not contain metadata on LastUpdated or any dates,
        # but along with that IDs for attachments are actually these attachments'
        # file names. So if someone creates a file text.txt with content "hello",
        # runs a sync, then deletes this file and creates again with different content,
        # the model returned from API will not change at all. It will have same ID,
        # same everything. But it will already be an absolutely new document.
        # Therefore every time we try to download the attachment we say that
        # it was just recently created so that framework would always re-download it.
        new_timestamp = datetime.utcnow()

        doc = {
            "_id": attachment["odata.id"],
            "_timestamp": new_timestamp,
        }

        attached_file, body = await self._download_content(
            partial(self.client.download_attachment, attachment["odata.id"]),
            attachment["_original_filename"],
        )

        if attached_file:
            doc["_attachment"] = attached_file
        if body is not None:
            # accept empty strings for body
            doc["body"] = body

        return doc

    async def get_drive_item_content(self, drive_item, timestamp=None, doit=False):
        document_size = int(drive_item["size"])

        if not (doit and document_size):
            return

        if (
            document_size > MAX_DOCUMENT_SIZE
            and not self.configuration["use_text_extraction_service"]
        ):
            return

        doc = {
            "_id": drive_item["id"],
            "_timestamp": drive_item["lastModifiedDateTime"],
        }

        attached_file, body = await self._download_content(
            partial(
                self.client.download_drive_item,
                drive_item["parentReference"]["driveId"],
                drive_item["id"],
            ),
            drive_item["_original_filename"],
        )

        if attached_file:
            doc["_attachment"] = attached_file
        if body is not None:
            # accept empty strings for body
            doc["body"] = body

        return doc

    async def _download_content(self, download_func, original_filename):
        attachment = None
        body = None
        source_file_name = ""
        file_extension = os.path.splitext(original_filename)[-1]

        try:
            async with NamedTemporaryFile(
                mode="wb", delete=False, suffix=file_extension, dir=self.download_dir
            ) as async_buffer:
                source_file_name = async_buffer.name

                # download_func should always be a partial with async_buffer as last argument that is not filled by the caller!
                # E.g. if download_func is download_drive_item(drive_id, item_id, async_buffer) then it
                # should be passed as partial(download_drive_item, drive_id, item_id)
                # This way async_buffer will be passed from here!!!
                await download_func(async_buffer)

            if self.configuration["use_text_extraction_service"]:
                body = ""
                if self.extraction_service._check_configured():
                    body = await self.extraction_service.extract_text(
                        source_file_name, original_filename
                    )
            else:
                await asyncio.to_thread(
                    convert_to_b64,
                    source=source_file_name,
                )
                async with aiofiles.open(
                    file=source_file_name, mode="r"
                ) as target_file:
                    attachment = (await target_file.read()).strip()
        finally:
            if source_file_name:
                await remove(str(source_file_name))

        return attachment, body

    async def ping(self):
        pass

    async def close(self):
        await self.client.close()
        if self.extraction_service is not None:
            await self.extraction_service._end_session()

    def advanced_rules_validators(self):
        return [SharepointOnlineAdvancedRulesValidator()]

    def is_supported_format(self, filename):
        if "." not in filename:
            return False

        attachment_extension = os.path.splitext(filename)
        if attachment_extension[-1].lower() in TIKA_SUPPORTED_FILETYPES:
            return True

        return False
