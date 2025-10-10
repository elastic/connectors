#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Notion source module responsible to fetch documents from the Notion Platform."""

import asyncio
import re
from copy import copy
from functools import partial
from urllib.parse import unquote

from connectors_sdk.logger import logger
from connectors_sdk.source import BaseDataSource, ConfigurableFieldValueError
from notion_client import APIResponseError

from connectors.sources.notion.client import NotionClient
from connectors.sources.notion.validator import NotionAdvancedRulesValidator
from connectors.utils import CancellableSleeps


class NotionDataSource(BaseDataSource):
    """Notion"""

    name = "Notion"
    service_type = "notion"
    advanced_rules_enabled = True
    incremental_sync_enabled = True

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
                search_results = []
                async for response in self.notion_client.fetch_by_query(data):
                    search_results.append(response)

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
                    found_titles = {get_title(result) for result in search_results}
                    exact_match_results = [
                        result
                        for result in search_results
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
                "display": "text",
                "label": "Notion Secret Key",
                "order": 1,
                "required": True,
                "sensitive": True,
                "type": "str",
            },
            "databases": {
                "label": "List of Databases",
                "display": "text",
                "order": 2,
                "required": True,
                "type": "list",
            },
            "pages": {
                "label": "List of Pages",
                "display": "text",
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
                "default_value": 30,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
        }

    def advanced_rules_validators(self):
        return [NotionAdvancedRulesValidator(self)]

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

    def is_connected_property_block(self, page_database):
        properties = page_database.get("properties")
        if properties is None:
            return False
        for field in properties.keys():
            if re.match(r"^Related to.*\(.*\)$", field):
                return True

        return False

    async def retrieve_and_process_blocks(self, query):
        block_ids_store = []
        async for page_database in self.notion_client.fetch_by_query(query=query):
            block_id = page_database.get("id")
            if self.index_comments is True:
                block_ids_store.append(block_id)

            yield self._format_doc(page_database), None
            self._logger.info(f"Fetching child blocks for block {block_id}")

            if self.is_connected_property_block(page_database):
                self._logger.debug(
                    f"Skipping children of block with id: {block_id} as not supported by API"
                )
                continue

            async for child_block in self.notion_client.fetch_child_blocks(
                block_id=block_id
            ):
                if self.index_comments is True:
                    block_ids_store.append(child_block.get("id"))

                if child_block.get("type") != "file":
                    yield self._format_doc(child_block), None
                else:
                    file_url = child_block.get("file", {}).get("file", {}).get("url")
                    child_block = self._format_doc(child_block)
                    yield (
                        child_block,
                        partial(self.get_content, copy(child_block), file_url),
                    )

        if self.index_comments is True:
            for block_id in block_ids_store:
                self._logger.info(f"Fetching comments for block {block_id}")
                async for comment in self.notion_client.fetch_comments(
                    block_id=block_id
                ):
                    yield self._format_doc(comment), None

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch following Notion objects: Users, Pages, Databases, Files,
           Comments, Blocks, Child Blocks in async manner.
        Args:
            filtering (filtering, None): Filtering Rules. Defaults to None.
        Yields:
            dict: Documents from Notion.
        """
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            for rule in advanced_rules:
                if "searches" in rule:
                    for database_page in advanced_rules.get("searches"):
                        if "filter" in database_page:
                            database_page["filter"]["property"] = "object"
                        self._logger.info(
                            f"Fetching databases and pages using search query: {database_page}"
                        )
                        async for data in self.retrieve_and_process_blocks(
                            database_page
                        ):
                            yield data
                if "database_query_filters" in rule:
                    for database_query_filter in advanced_rules.get(
                        "database_query_filters"
                    ):
                        filter_criteria = (
                            {"filter": database_query_filter.get("filter")}
                            if "filter" in database_query_filter
                            else {}
                        )
                        try:
                            database_id = database_query_filter.get("database_id")
                            self._logger.info(
                                f"Fetching records for database with id: {database_id}"
                            )
                            async for database in self.notion_client.query_database(
                                database_id, filter_criteria
                            ):
                                yield self._format_doc(database), None
                        except APIResponseError as e:
                            msg = (
                                f"Please make sure to include correct filter field, {e}"
                            )
                            raise ConfigurableFieldValueError(msg) from e

        else:
            self._logger.info("Fetching users")
            async for user_document in self.notion_client.fetch_users():
                yield self._format_doc(user_document), None

            for query in self.generate_query():
                self._logger.info(f"Fetching pages and databases using query {query}")
                async for data in self.retrieve_and_process_blocks(query):
                    yield data
