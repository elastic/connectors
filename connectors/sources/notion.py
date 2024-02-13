#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Notion source module responsible to fetch documents from the Notion Platform."""
import asyncio
import os
from copy import copy
from functools import cached_property, partial
from urllib.parse import unquote

import aiohttp
from aiohttp.client_exceptions import ClientResponseError
from notion_client import APIResponseError, AsyncClient
from notion_client.helpers import async_iterate_paginated_api

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import CancellableSleeps, RetryStrategy, retryable

RETRIES = 3
RETRY_INTERVAL = 2
DEFAULT_RETRY_SECONDS = 30
BASE_URL = "https://api.notion.com"
MAX_CONCURRENT_CLIENT_SUPPORT = 3

ENDPOINTS = {
    "search": "/search",
    "owner": "/users/me",
}

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

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=NotFound,
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

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def make_request(self, endpoint_key, method="GET", **kwargs):
        self._logger.debug(f"Making request to {endpoint_key} with params {kwargs}")
        try:
            if path := ENDPOINTS.get(endpoint_key):
                return await self._get_client.request(
                    path=path, method=method, **kwargs
                )
        except APIResponseError as api_error:
            self._logger.exception(f"Notion API Error - {api_error}")
            if api_error.code == "rate_limited" and api_error.status == 429:
                retry_after = (
                    api_error.headers.get("retry-after") or DEFAULT_RETRY_SECONDS
                )
                self._logger.info(
                    f"Connector will attempt to retry after {int(retry_after)} seconds."
                )
                await self._sleeps.sleep(int(retry_after))
            raise

    async def fetch_owner(self):
        """Fetch integration authorized owner"""
        await self.make_request("owner", method="GET")

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
        async for user_document in async_iterate_paginated_api(
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
            async for child_block in async_iterate_paginated_api(
                self._get_client.blocks.children.list, block_id=block.get("id")
            ):
                yield child_block

        async for block in async_iterate_paginated_api(
            self._get_client.blocks.children.list, block_id=block_id
        ):
            if block.get("type") not in ["child_database", "child_page", "unsupported"]:
                yield block
                if block.get("has_children") is True:
                    async for child in fetch_children_recursively(block):
                        yield child
            if block.get("type") == "child_database":
                async for record in self.query_database(block.get("id")):
                    yield record

    async def fetch_by_query(self, query):
        async for document in async_iterate_paginated_api(
            self._get_client.search, **query
        ):
            yield document
            if query and query.get("filter", {}).get("value") == "database":
                async for database in self.query_database(document.get("id")):
                    yield database

    async def fetch_comments(self, block_id):
        async for block_comment in async_iterate_paginated_api(
            self._get_client.comments.list, block_id=block_id
        ):
            yield block_comment

    async def query_database(self, database_id):
        async for result in async_iterate_paginated_api(
            self._get_client.databases.query, database_id=database_id
        ):
            yield result


class NotionDataSource(BaseDataSource):
    """Notion"""

    name = "Notion"
    service_type = "notion"

    def __init__(self, configuration):
        """Setup the connection to the Notion instance.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.notion_client = NotionClient(configuration=configuration)

        self.configuration = configuration
        self.index_comments = self.configuration["index_comments"]
        self.pages = self.configuration["pages"]
        self.databases = self.configuration["databases"]
        self._logger = logger
        self._sleeps = CancellableSleeps()
        self.concurrent_downloads = self.configuration["concurrent_downloads"]

    def _set_internal_logger(self):
        self.notion_client.set_logger(self._logger)

    async def ping(self):
        try:
            await self.notion_client.fetch_owner()
            self._logger.info("Successfully connected to Notion.")
        except Exception:
            self._logger.exception("Error while connecting to Notion.")
            raise

    async def close(self):
        await self.notion_client.close()

    async def get_entities(self, entity_type, entity_titles):
        """Search for a database or page with the given title."""
        invalid_titles = []
        found_titles = set()
        exact_match_results = []
        if entity_titles != ["*"]:
            try:
                data = {
                    "query": " ".join(entity_titles),
                    "filter": {"value": entity_type, "property": "object"},
                }
                search_results = await self.notion_client.make_request(
                    "search", method="POST", body=data
                )
                get_title = {
                    "database": lambda result: result.get("title", [{}])[0]
                    .get("plain_text", "")
                    .lower(),
                    "page": lambda result: result.get("properties", {})
                    .get("title", {})
                    .get("title", [{}])[0]
                    .get("text", {})
                    .get("content", "")
                    .lower(),
                }.get(entity_type)
                if get_title is not None:
                    found_titles = {
                        get_title(result)
                        for result in search_results.get("results", [])
                    }
                    exact_match_results = [
                        result
                        for result in search_results.get("results", [])
                        if get_title(result).lower() in map(str.lower, entity_titles)
                    ]
                invalid_titles = [
                    title
                    for title in entity_titles
                    if title.lower() not in found_titles
                ]
            except Exception as e:
                self._logger.exception(f"Error searching for {entity_type}: {e}")
                raise
            if invalid_titles:
                msg = f"Invalid {entity_type} titles found: {', '.join(invalid_titles)}"
                raise ConfigurableFieldValueError(msg)
        return exact_match_results

    async def validate_config(self):
        """Validates if user configured databases and pages are available in notion."""
        await super().validate_config()
        await asyncio.gather(
            self.get_entities("page", self.configuration.get("pages", [])),
            self.get_entities("database", self.configuration.get("databases", [])),
        )

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Notion.
        Returns:
            dict: Default configuration.
        """
        return {
            "notion_secret_key": {
                "display": "string",
                "label": "Notion Secret Key",
                "order": 1,
                "required": True,
                "sensitive": True,
                "type": "str",
            },
            "databases": {
                "label": "List of Databases",
                "display": "string",
                "order": 2,
                "required": True,
                "type": "list",
            },
            "pages": {
                "label": "List of Pages",
                "display": "string",
                "order": 3,
                "required": True,
                "type": "list",
            },
            "index_comments": {
                "display": "toggle",
                "label": "Enable indexing comments",
                "order": 4,
                "tooltip": "Enabling this will increase the amount of network calls to the source, and may decrease performance",
                "type": "bool",
                "value": False,
            },
            "concurrent_downloads": {
                "default_value": 20,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
        }

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by Notion

        Args:
            options (dict): Config bulker options.
        """

        options["concurrent_downloads"] = self.concurrent_downloads

    async def get_file_metadata(self, attachment_metadata, file_url):
        response = await anext(self.notion_client.get_via_session(url=file_url))
        attachment_metadata["extension"] = "." + response.url.path.split(".")[-1]
        attachment_metadata["size"] = response.content_length
        attachment_metadata["name"] = unquote(response.url.path.split("/")[-1])
        return attachment_metadata

    async def get_content(self, attachment, file_url, timestamp=None, doit=False):
        """Extracts the content for Apache TIKA supported file types.

        Args:
            attachment (dictionary): Formatted attachment document.
            timestamp (timestamp, optional): Timestamp of attachment last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and attachment content
        """
        if not file_url:
            self._logger.debug(
                f"skipping attachment with id {attachment['id']} as url is empty"
            )
            return
        attachment = await self.get_file_metadata(attachment, file_url)
        attachment_size = int(attachment["size"])

        attachment_name = attachment["name"]
        attachment_extension = attachment["extension"]

        if not self.can_file_be_downloaded(
            attachment_extension, attachment_name, attachment_size
        ):
            return
        document = {
            "_id": f"{attachment['_id']}",
            "_timestamp": attachment["_timestamp"],
        }

        return await self.download_and_extract_file(
            document,
            attachment_name,
            attachment_extension,
            partial(
                self.generic_chunked_download_func,
                partial(self.notion_client.get_via_session, url=file_url),
            ),
        )

    def _format_doc(self, data):
        """Format document for handling empty values & type casting.

        Args:
            data (dict): Fetched record from Notion.

        Returns:
            dict: Formatted document.
        """
        data = {key: value for key, value in data.items() if value}
        data["_id"] = data["id"]
        if "last_edited_time" in data:
            data["_timestamp"] = data["last_edited_time"]

        if "properties" in data:
            data["details"] = str(data["properties"])
            del data["properties"]
        return data

    def generate_query(self):
        if self.pages == ["*"] and self.databases == ["*"]:
            yield {}
        else:
            for page in self.pages:
                yield {
                    "query": "" if page == "*" else page,
                    "filter": {"value": "page", "property": "object"},
                }
            for database in self.databases:
                yield {
                    "query": "" if database == "*" else database,
                    "filter": {"value": "database", "property": "object"},
                }

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch following Notion objects: Users, Pages, Databases, Files,
           Comments, Blocks, Child Blocks in async manner.
        Args:
            filtering (filtering, None): Filtering Rules. Defaults to None.
        Yields:
            dict: Documents from Notion.
        """
        self._logger.info("Fetching users")
        async for user_document in self.notion_client.fetch_users():
            yield self._format_doc(user_document), None

        block_ids_store = []
        for query in self.generate_query():
            self._logger.info(f"Fetching pages and databases using query {query}")

            async for page_database in self.notion_client.fetch_by_query(query=query):
                block_id = page_database.get("id")
                if self.index_comments is True:
                    block_ids_store.append(block_id)

                yield self._format_doc(page_database), None
                self._logger.info(f"Fetching child blocks for block {block_id}")

                async for child_block in self.notion_client.fetch_child_blocks(
                    block_id=block_id
                ):
                    if self.index_comments is True:
                        block_ids_store.append(child_block.get("id"))

                    if child_block.get("type") != "file":
                        yield self._format_doc(child_block), None
                    else:
                        file_url = (
                            child_block.get("file", {}).get("file", {}).get("url")
                        )
                        child_block = self._format_doc(child_block)
                        yield child_block, partial(
                            self.get_content, copy(child_block), file_url
                        )

        if self.index_comments is True:
            for block_id in block_ids_store:
                self._logger.info(f"Fetching comments for block {block_id}")
                async for comment in self.notion_client.fetch_comments(
                    block_id=block_id
                ):
                    yield self._format_doc(comment), None
