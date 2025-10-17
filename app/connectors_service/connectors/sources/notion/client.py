#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import json
import os
from functools import cached_property
from typing import Any, Awaitable, Callable

import aiohttp
from aiohttp import ClientResponseError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_unless_exception_type

from connectors_sdk.logger import logger
from notion_client import APIResponseError, AsyncClient

from connectors.utils import CancellableSleeps

RETRIES = 3
RETRY_INTERVAL = 2
DEFAULT_RETRY_SECONDS = 30
BASE_URL = "https://api.notion.com"
MAX_CONCURRENT_CLIENT_SUPPORT = 30


if "OVERRIDE_URL" in os.environ:
    BASE_URL = os.environ["OVERRIDE_URL"]


class NotFound(Exception):
    pass


class NotionClient:
    """Notion API client"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.notion_secret_key = self.configuration["notion_secret_key"]

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def _get_client(self):
        return AsyncClient(
            auth=self.notion_secret_key,
            base_url=BASE_URL,
        )

    @cached_property
    def session(self):
        """Generate aiohttp client session.

        Returns:
            aiohttp.ClientSession: An instance of Client Session
        """
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_CLIENT_SUPPORT)

        return aiohttp.ClientSession(
            connector=connector,
            raise_for_status=True,
        )

    @retry(
        stop=stop_after_attempt(RETRIES),
        wait=wait_exponential(multiplier=1, exp_base=RETRY_INTERVAL),
        reraise=True,
        retry=retry_unless_exception_type(NotFound),
    )
    async def get_via_session(self, url):
        self._logger.debug(f"Fetching data from url {url}")
        try:
            async with self.session.get(url=url) as response:
                yield response
        except ClientResponseError as e:
            if e.status == 429:
                retry_seconds = e.headers.get("Retry-After") or DEFAULT_RETRY_SECONDS
                self._logger.debug(
                    f"Rate Limit reached: retry in {retry_seconds} seconds"
                )
                await self._sleeps.sleep(retry_seconds)
                raise
            elif e.status == 404:
                raise NotFound from e
            else:
                raise

    @retry(
        stop=stop_after_attempt(RETRIES),
        wait=wait_exponential(multiplier=1, exp_base=RETRY_INTERVAL),
        reraise=True,
        retry=retry_unless_exception_type(NotFound),
    )
    async def fetch_results(
        self, function: Callable[..., Awaitable[Any]], next_cursor=None, **kwargs: Any
    ):
        try:
            return await function(start_cursor=next_cursor, **kwargs)
        except APIResponseError as exception:
            if exception.code == "rate_limited" or exception.status == 429:
                retry_after = (
                    exception.headers.get("retry-after") or DEFAULT_RETRY_SECONDS
                )
                request_info = f"Request: {function.__name__} (next_cursor: {next_cursor}, kwargs: {kwargs})"
                self._logger.info(
                    f"Connector will attempt to retry after {int(retry_after)} seconds. {request_info}"
                )
                await self._sleeps.sleep(int(retry_after))
                msg = "Rate limit exceeded."
                raise Exception(msg) from exception
            else:
                raise

    async def async_iterate_paginated_api(
        self, function: Callable[..., Awaitable[Any]], **kwargs: Any
    ):
        """Return an async iterator over the results of any paginated Notion API."""
        next_cursor = kwargs.pop("start_cursor", None)
        while True:
            response = await self.fetch_results(function, next_cursor, **kwargs)
            if response:
                for result in response.get("results"):
                    yield result
                next_cursor = response.get("next_cursor")
                if not response["has_more"] or next_cursor is None:
                    return

    async def fetch_owner(self):
        """Fetch integration authorized owner"""
        await self._get_client.users.me()

    async def close(self):
        self._sleeps.cancel()
        await self._get_client.aclose()
        await self.session.close()
        del self._get_client
        del self.session

    async def fetch_users(self):
        """Iterate over user information retrieved from the API.
        Yields:
        dict: User document information excluding bots."""
        async for user_document in self.async_iterate_paginated_api(
            self._get_client.users.list
        ):
            if user_document.get("type") != "bot":
                yield user_document

    async def fetch_child_blocks(self, block_id):
        """Fetch child blocks recursively for a given block ID.
        Args:
            block_id (str): The ID of the parent block.
        Yields:
            dict: Child block information."""

        async def fetch_children_recursively(block):
            if block.get("has_children") is True:
                async for child_block in self.async_iterate_paginated_api(
                    self._get_client.blocks.children.list, block_id=block.get("id")
                ):
                    yield child_block

                    async for grandchild in fetch_children_recursively(child_block):  # pyright: ignore
                        yield grandchild

        try:
            async for block in self.async_iterate_paginated_api(
                self._get_client.blocks.children.list, block_id=block_id
            ):
                if block.get("type") not in [
                    "child_database",
                    "child_page",
                    "unsupported",
                ]:
                    yield block
                    if block.get("has_children") is True:
                        async for child in fetch_children_recursively(block):
                            yield child
                if block.get("type") == "child_database":
                    async for record in self.query_database(block.get("id")):
                        yield record
        except APIResponseError as error:
            if error.code == "validation_error" and "external_object" in json.loads(
                error.body
            ).get("message"):
                self._logger.warning(
                    f"Encountered external object with id: {block_id}. Skipping : {error}"
                )
            elif error.code == "object_not_found":
                self._logger.warning(f"Object not found: {error}")
            else:
                raise

    async def fetch_by_query(self, query):
        async for document in self.async_iterate_paginated_api(
            self._get_client.search, **query
        ):
            yield document
            if query and query.get("filter", {}).get("value") == "database":
                async for database in self.query_database(document.get("id")):
                    yield database

    async def fetch_comments(self, block_id):
        async for block_comment in self.async_iterate_paginated_api(
            self._get_client.comments.list, block_id=block_id
        ):
            yield block_comment

    async def query_database(self, database_id, body=None):
        if body is None:
            body = {}
        async for result in self.async_iterate_paginated_api(
            self._get_client.databases.query, database_id=database_id, **body
        ):
            yield result
