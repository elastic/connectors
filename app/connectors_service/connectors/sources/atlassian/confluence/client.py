#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientResponseError, ServerDisconnectedError
from connectors_sdk.logger import logger

from connectors.sources.atlassian.confluence.constants import (
    ATTACHMENT,
    ATTACHMENT_QUERY,
    CONFLUENCE_CLOUD,
    CONFLUENCE_DATA_CENTER,
    CONFLUENCE_SERVER,
    CONTENT,
    DATACENTER_USER_BATCH,
    DEFAULT_RETRY_SECONDS,
    LABEL,
    LIMIT,
    PING_URL,
    RETRIES,
    RETRY_INTERVAL,
    SEARCH,
    SEARCH_FOR_DATA_CENTER,
    SEARCH_QUERY,
    SERVER_USER_BATCH,
    SPACE,
    SPACE_QUERY,
    URLS,
    USERS_FOR_DATA_CENTER,
    USERS_FOR_SERVER,
    WILDCARD,
)
from connectors.utils import CancellableSleeps, RetryStrategy, retryable, ssl_context


class InvalidConfluenceDataSourceTypeError(ValueError):
    pass


class ThrottledError(Exception):
    """Internal exception class to indicate that request was throttled by the API"""

    pass


class InternalServerError(Exception):
    pass


class NotFound(Exception):
    pass


class BadRequest(Exception):
    pass


class Unauthorized(Exception):
    pass


class Forbidden(Exception):
    pass


class ConfluenceClient:
    """Confluence client to handle API calls made to Confluence"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.data_source_type = self.configuration["data_source"]
        self.host_url = self.configuration["confluence_url"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]
        self.retry_count = self.configuration["retry_count"]
        self.index_labels = self.configuration["index_labels"]
        if self.data_source_type == CONFLUENCE_CLOUD:
            self.host_url = os.path.join(self.host_url, "wiki")

        if self.ssl_enabled and self.certificate:
            self.ssl_ctx = ssl_context(certificate=self.certificate)
        else:
            self.ssl_ctx = False
        self.session = None

    def set_logger(self, logger_):
        self._logger = logger_

    def _get_session(self):
        """Generate and return base client session with configuration fields

        Returns:
            aiohttp.ClientSession: An instance of Client Session
        """
        if self.session:
            return self.session

        self._logger.debug(f"Creating a '{self.data_source_type}' client session")
        if self.data_source_type == CONFLUENCE_CLOUD:
            auth = (
                self.configuration["account_email"],
                self.configuration["api_token"],
            )
        elif self.data_source_type == CONFLUENCE_SERVER:
            auth = (
                self.configuration["username"],
                self.configuration["password"],
            )
        elif self.data_source_type == CONFLUENCE_DATA_CENTER:
            auth = (
                self.configuration["data_center_username"],
                self.configuration["data_center_password"],
            )
        else:
            msg = f"Unknown data source type '{self.data_source_type}' for Confluence connector"
            self._logger.error(msg)

            raise InvalidConfluenceDataSourceTypeError(msg)

        basic_auth = aiohttp.BasicAuth(login=auth[0], password=auth[1])
        timeout = aiohttp.ClientTimeout(total=None)  # pyright: ignore
        self.session = aiohttp.ClientSession(
            auth=basic_auth,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=timeout,
            raise_for_status=True,
        )
        return self.session

    async def close_session(self):
        """Closes unclosed client session"""
        self._sleeps.cancel()
        if self.session is None:
            return
        await self.session.close()
        self.session = None

    async def _handle_client_errors(self, url, exception):
        if exception.status == 429:
            response_headers = exception.headers or {}
            retry_seconds = DEFAULT_RETRY_SECONDS
            if "Retry-After" in response_headers:
                try:
                    retry_seconds = int(response_headers["Retry-After"])
                except (TypeError, ValueError) as exception:
                    self._logger.error(
                        f"Rate limit reached but an unexpected error occurred while reading value of 'Retry-After' header: {exception}. Retrying in {DEFAULT_RETRY_SECONDS} seconds..."
                    )
            else:
                self._logger.warning(
                    f"Rate limit reached but no 'Retry-After' header was found. Retrying in {DEFAULT_RETRY_SECONDS} seconds..."
                )
            self._logger.debug(
                f"Rate Limit reached: retrying in {retry_seconds} seconds..."
            )

            await self._sleeps.sleep(retry_seconds)
            raise ThrottledError
        elif exception.status == 400:
            self._logger.error(f"Received Bad Request error for URL: {url}")
            raise BadRequest from exception
        elif exception.status == 401:
            self._logger.error(f"Received Unauthorized error for URL: {url}")
            raise Unauthorized from exception
        elif exception.status == 403:
            self._logger.error(f"Received Forbidden error for URL: {url}")
            raise Forbidden from exception
        elif exception.status == 404:
            self._logger.error(f"Received Not Found error for URL: {url}")
            raise NotFound from exception
        elif exception.status == 500:
            self._logger.error(f"Internal Server Error occurred for URL: {url}")
            raise InternalServerError from exception
        else:
            self._logger.error(
                f"Error while making a GET call for URL: {url}. Error details: {exception}"
            )
            raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=NotFound,
    )
    async def api_call(self, url):
        """Make a GET call for Atlassian API using the passed url with retry for the failed API calls.

        Args:
            url: Request URL to hit the get call

        Raises:
            exception: An instance of an exception class.

        Yields:
            response: Client response
        """
        self._logger.debug(f"Making a GET request to URL: {url}")

        try:
            return await self._get_session().get(
                url=url,
                ssl=self.ssl_ctx,
            )
        except ServerDisconnectedError:
            self._logger.error(
                f"Server was disconnected during GET request to URL: {url}. Closing the session."
            )
            await self.close_session()
            raise
        except ClientResponseError as exception:
            await self._handle_client_errors(url=url, exception=exception)

    async def paginated_api_call(self, url_name, **url_kwargs):
        """Make a paginated API call for Confluence objects using the passed url_name.
        Args:
            url_name (str): URL Name to identify the API endpoint to hit
        Yields:
            response: JSON response.
        """
        url = os.path.join(self.host_url, URLS[url_name].format(**url_kwargs))
        while True:
            try:
                self._logger.debug(f"Starting pagination for API endpoint {url}")
                response = await self.api_call(url=url)
                json_response = await response.json()
                links = json_response.get("_links")
                yield json_response
                if links.get("next") is None:
                    return
                url = os.path.join(
                    self.host_url,
                    links.get("next")[1:],
                )
            # re-raise on specific exceptions
            except ServerDisconnectedError:
                self._logger.error(
                    f"Server was disconnected during paginated GET request to API endpoint {url}."
                )
                raise
            except InternalServerError:
                self._logger.error(
                    f"Internal Server Error occurred during paginated GET request to API endpoint {url}"
                )
                raise
            except Exception as exception:
                self._logger.warning(
                    f"Skipping data for type {url_name} from {url}. Exception: {exception}."
                )
                break

    async def paginated_api_call_for_datacenter_syncrule(self, url_name, **url_kwargs):
        """Make a paginated API call for datacenter using the passed url_name.
        Args:
            url_name (str): URL Name to identify the API endpoint to hit
        Yields:
            response: JSON response.
        """
        start = 0
        while True:
            url = os.path.join(self.host_url, URLS[url_name].format(**url_kwargs))
            json_response = {}
            try:
                self._logger.debug(
                    f"Starting pagination for API endpoint: {URLS[url_name].format(**url_kwargs)} to host: {self.host_url} with the following parameters -> start: {start}, limit: {LIMIT}"
                )
                response = await self.api_call(url=url)
                json_response = await response.json()
                yield json_response

                start = url_kwargs.get("start", 0)
                start += LIMIT
                url_kwargs["start"] = start
                if len(json_response.get("results", [])) < LIMIT:
                    break
            except Exception as exception:
                self._logger.warning(
                    f"Skipping data for type {url_name} from {url}. Exception: {exception}."
                )
                break

    async def download_func(self, url):
        yield await self.api_call(url)

    async def search_by_query(self, query):
        if self.data_source_type == CONFLUENCE_DATA_CENTER:
            search_documents = self.paginated_api_call_for_datacenter_syncrule(
                url_name=SEARCH_FOR_DATA_CENTER,
                query=f"{query}&{SEARCH_QUERY}",
                start=0,
            )
        else:
            search_documents = self.paginated_api_call(
                url_name=SEARCH,
                query=f"{query}&{SEARCH_QUERY}",
            )
        async for response in search_documents:
            for entity in response.get("results", []):
                yield entity

    async def fetch_spaces(self):
        async for response in self.paginated_api_call(
            url_name=SPACE,
            api_query=SPACE_QUERY,
        ):
            for space in response.get("results", []):
                spaces = self.configuration.get("spaces", "")
                if (spaces == [WILDCARD]) or (space.get("key", "") in spaces):
                    yield space

    async def fetch_server_space_permission(self, url):
        try:
            permissions = await self.api_call(url=os.path.join(self.host_url, url))
            permission = await permissions.json()
            return permission
        except ClientResponseError as exception:
            self._logger.warning(
                f"Something went wrong. Make sure you have installed Extender for running confluence datacenter/server DLS. Exception: {exception}."
            )
            return {}

    async def fetch_page_blog_documents(self, api_query):
        async for response in self.paginated_api_call(
            url_name=CONTENT,
            api_query=api_query,
        ):
            attachment_count = 0
            for document in response.get("results", []):
                if document.get("children").get("attachment"):
                    attachment_count = (
                        document.get("children", {})
                        .get("attachment", {})
                        .get("size", 0)
                    )
                if self.index_labels:
                    labels = await self.fetch_label(document["id"])
                    document["labels"] = labels
                yield document, attachment_count

    async def fetch_attachments(self, content_id):
        async for response in self.paginated_api_call(
            url_name=ATTACHMENT,
            api_query=ATTACHMENT_QUERY,
            id=content_id,
        ):
            for attachment in response.get("results", []):
                yield attachment

    async def ping(self):
        await self.api_call(
            url=os.path.join(self.host_url, PING_URL),
        )

    async def fetch_confluence_server_users(self):
        start_at = 0
        if self.data_source_type == CONFLUENCE_DATA_CENTER:
            limit = DATACENTER_USER_BATCH
            key = "results"
            url = urljoin(self.host_url, URLS[USERS_FOR_DATA_CENTER])
        else:
            limit = SERVER_USER_BATCH
            key = "users"
            url = urljoin(self.host_url, URLS[USERS_FOR_SERVER])

        while True:
            url_ = url.format(start=start_at, limit=limit)
            users = await self.api_call(url=url_)
            response = await users.json()
            if len(response.get(key)) == 0:
                return
            yield response.get(key)
            start_at += limit

    async def fetch_label(self, label_id):
        url = os.path.join(self.host_url, URLS[LABEL].format(id=label_id))
        label_data = await self.api_call(url=url)
        labels = await label_data.json()
        return [label.get("name") for label in labels["results"]]
