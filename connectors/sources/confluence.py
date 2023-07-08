#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Confluence source module responsible to fetch documents from Confluence Cloud/Server.
"""
import asyncio
import os
from copy import copy
from functools import partial

import aiofiles
import aiohttp
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.sources.atlassian import AtlassianAdvancedRulesValidator
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    ConcurrentTasks,
    MemQueue,
    convert_to_b64,
    iso_utc,
    ssl_context,
)

FILE_SIZE_LIMIT = 10485760
RETRY_INTERVAL = 2
SPACE = "space"
ATTACHMENT = "attachment"
CONTENT = "content"
DOWNLOAD = "download"
SEARCH = "search"
SPACE_QUERY = "limit=100"
ATTACHMENT_QUERY = "limit=100&expand=version"
CONTENT_QUERY = (
    "limit=50&expand=children.attachment,history.lastUpdated,body.storage,space"
)
SEARCH_QUERY = "limit=100&expand=content.extensions,content.container,content.space,space.description"

URLS = {
    SPACE: "rest/api/space?{api_query}",
    CONTENT: "rest/api/content/search?{api_query}",
    ATTACHMENT: "rest/api/content/{id}/child/attachment?{api_query}",
    SEARCH: "rest/api/search?cql={query}",
}
PING_URL = "rest/api/space?limit=1"
MAX_CONCURRENT_DOWNLOADS = 50  # Max concurrent download supported by confluence
CHUNK_SIZE = 1024
MAX_CONCURRENCY = 50
QUEUE_SIZE = 1024
QUEUE_MEM_SIZE = 25 * 1024 * 1024  # Size in Megabytes
END_SIGNAL = "FINISHED_TASK"

CONFLUENCE_CLOUD = "confluence_cloud"
CONFLUENCE_SERVER = "confluence_server"
WILDCARD = "*"


class ConfluenceClient:
    """Confluence client to handle API calls made to Confluence"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.is_cloud = self.configuration["data_source"] == CONFLUENCE_CLOUD
        self.host_url = self.configuration["confluence_url"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]
        self.retry_count = self.configuration["retry_count"]
        if self.is_cloud:
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
        if self.is_cloud:
            auth = (
                self.configuration["account_email"],
                self.configuration["api_token"],
            )
        else:
            auth = self.configuration["username"], self.configuration["password"]

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

    async def api_call(self, url):
        """Make a GET call for Atlassian API using the passed url with retry for the failed API calls.

        Args:
            url: Request URL to hit the get call

        Raises:
            exception: An instance of an exception class.

        Yields:
            response: Client response
        """
        retry_counter = 0
        while True:
            try:
                async with self._get_session().get(
                    url=url,
                    ssl=self.ssl_ctx,
                ) as response:
                    yield response
                    break
            except Exception as exception:
                if isinstance(
                    exception,
                    ServerDisconnectedError,
                ):
                    await self.session.close()  # pyright: ignore
                retry_counter += 1
                if retry_counter > self.retry_count:
                    raise exception
                self._logger.warning(
                    f"Retry count: {retry_counter} out of {self.retry_count}. Exception: {exception}"
                )
                await self._sleeps.sleep(RETRY_INTERVAL**retry_counter)

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
                async for response in self.api_call(
                    url=url,
                ):
                    json_response = await response.json()
                    links = json_response.get("_links")
                    yield json_response
                    if links.get("next") is None:
                        return
                    url = os.path.join(
                        self.host_url,
                        links.get("next")[1:],
                    )
            except Exception as exception:
                self._logger.warning(
                    f"Skipping data for type {url_name} from {url}. Exception: {exception}."
                )
                break


class ConfluenceDataSource(BaseDataSource):
    """Confluence"""

    name = "Confluence"
    service_type = "confluence"
    advanced_rules_enabled = True

    def __init__(self, configuration):
        """Setup the connection to Confluence

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.spaces = self.configuration["spaces"]
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.confluence_client = ConfluenceClient(configuration=configuration)

        self.queue = MemQueue(maxsize=QUEUE_SIZE, maxmemsize=QUEUE_MEM_SIZE)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)
        self.fetcher_count = 0

    def _set_internal_logger(self):
        self.confluence_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Confluence

        Returns:
            dictionary: Default configuration.
        """
        return {
            "data_source": {
                "display": "dropdown",
                "label": "Confluence data source",
                "options": [
                    {"label": "Confluence Cloud", "value": CONFLUENCE_CLOUD},
                    {"label": "Confluence Server", "value": CONFLUENCE_SERVER},
                ],
                "order": 1,
                "type": "str",
                "value": CONFLUENCE_SERVER,
            },
            "username": {
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_SERVER}],
                "label": "Confluence Server username",
                "order": 2,
                "type": "str",
                "value": "admin",
            },
            "password": {
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_SERVER}],
                "label": "Confluence Server password",
                "sensitive": True,
                "order": 3,
                "type": "str",
                "value": "abc@123",
            },
            "account_email": {
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_CLOUD}],
                "label": "Confluence Cloud account email",
                "order": 4,
                "type": "str",
                "value": "me@example.com",
            },
            "api_token": {
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_CLOUD}],
                "label": "Confluence Cloud API token",
                "sensitive": True,
                "order": 5,
                "type": "str",
                "value": "abc#123",
            },
            "confluence_url": {
                "label": "Confluence URL",
                "order": 6,
                "type": "str",
                "value": "http://127.0.0.1:5000",
            },
            "spaces": {
                "display": "textarea",
                "label": "Confluence space keys",
                "order": 7,
                "tooltip": "This configurable field is ignored when Advanced Sync Rules are used.",
                "type": "list",
                "value": WILDCARD,
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "Enable SSL",
                "order": 8,
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 9,
                "type": "str",
                "value": "",
            },
            "retry_count": {
                "default_value": 3,
                "display": "numeric",
                "label": "Retries per request",
                "order": 10,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": 3,
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_DOWNLOADS,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 11,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "validations": [
                    {"type": "less_than", "constraint": MAX_CONCURRENT_DOWNLOADS + 1}
                ],
                "value": MAX_CONCURRENT_DOWNLOADS,
            },
        }

    async def close(self):
        """Closes unclosed client session"""
        await self.confluence_client.close_session()

    def advanced_rules_validators(self):
        return [AtlassianAdvancedRulesValidator(self)]

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by Confluence

        Args:
            options (dictionary): Config bulker options
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields.
        Also validate, if user configured spaces are available in Confluence.

        Raises:
            Exception: Configured keys can't be empty
        """
        self.configuration.check_valid()
        await self._remote_validation()

    async def _remote_validation(self):
        if self.spaces == [WILDCARD]:
            return
        space_keys = []
        async for response in self.confluence_client.paginated_api_call(
            url_name=SPACE, api_query=SPACE_QUERY
        ):
            spaces = response.get("results", [])
            space_keys.extend([space["key"] for space in spaces])
        if unavailable_spaces := set(self.spaces) - set(space_keys):
            raise ConfigurableFieldValueError(
                f"Spaces '{', '.join(unavailable_spaces)}' are not available. Available spaces are: '{', '.join(space_keys)}'"
            )

    async def ping(self):
        """Verify the connection with Confluence"""
        try:
            await anext(
                self.confluence_client.api_call(
                    url=os.path.join(self.confluence_client.host_url, PING_URL),
                )
            )
            self._logger.info("Successfully connected to Confluence")
        except Exception:
            self._logger.exception("Error while connecting to Confluence")
            raise

    async def fetch_spaces(self):
        """Get spaces with the help of REST APIs

        Yields:
            Dictionary: Space document to get indexed
        """
        async for response in self.confluence_client.paginated_api_call(
            url_name=SPACE,
            api_query=SPACE_QUERY,
        ):
            for space in response.get("results", []):
                space_url = os.path.join(
                    self.confluence_client.host_url, space["_links"]["webui"][1:]
                )
                if (self.spaces == [WILDCARD]) or (space["key"] in self.spaces):
                    yield {
                        "_id": space["id"],
                        "type": "Space",
                        "title": space["name"],
                        "_timestamp": iso_utc(),
                        "url": space_url,
                    }

    async def fetch_documents(self, api_query):
        """Get pages and blog posts with the help of REST APIs

        Args:
            api_query (str): Query parameter for the API call

        Yields:
            Dictionary: Page or blog post to be indexed
            Integer: Number of attachments in a page/blogpost
        """
        async for response in self.confluence_client.paginated_api_call(
            url_name=CONTENT,
            api_query=api_query,
        ):
            documents = response.get("results", [])
            attachment_count = 0
            for document in documents:
                updated_time = document["history"]["lastUpdated"]["when"]
                if document.get("children").get("attachment"):
                    attachment_count = document["children"]["attachment"]["size"]
                document_url = os.path.join(
                    self.confluence_client.host_url, document["_links"]["webui"][1:]
                )
                yield {
                    "_id": document["id"],
                    "type": document["type"],
                    "_timestamp": updated_time,
                    "title": document["title"],
                    "space": document["space"]["name"],
                    "body": document["body"]["storage"]["value"],
                    "url": document_url,
                }, attachment_count

    async def fetch_attachments(
        self, content_id, parent_name, parent_space, parent_type
    ):
        """Fetches all the attachments present in the given content (pages and blog posts)

        Args:
            content_id (str): Unique identifier of Confluence content (pages and blogposts)
            parent_name (str): Page/Blog post where the attachment is present
            parent_space (str): Confluence Space where the attachment is present
            parent_type (str): Type of the parent (Blog post or Page)

        Yields:
            Dictionary: Confluence attachment on the given page or blog post
            String: Download link to get the content of the attachment
        """
        async for response in self.confluence_client.paginated_api_call(
            url_name=ATTACHMENT,
            api_query=ATTACHMENT_QUERY,
            id=content_id,
        ):
            attachments = response.get("results", [])
            for attachment in attachments:
                attachment_url = os.path.join(
                    self.confluence_client.host_url,
                    attachment["_links"]["webui"][1:],
                )
                yield {
                    "type": attachment["type"],
                    "title": attachment["title"],
                    "_id": attachment["id"],
                    "space": parent_space,
                    parent_type: parent_name,
                    "_timestamp": attachment["version"]["when"],
                    "size": attachment["extensions"]["fileSize"],
                    "url": attachment_url,
                }, attachment["_links"]["download"]

    async def search_by_query(self, query):
        async for response in self.confluence_client.paginated_api_call(
            url_name=SEARCH,
            query=f"{query}&{SEARCH_QUERY}",
        ):
            results = response.get("results", [])
            for entity in results:
                # entity can be space or content
                entity_details = entity.get(SPACE) or entity.get(CONTENT)

                if (
                    entity_details["type"] == "attachment"
                    and entity_details["container"].get("title") is None
                ):
                    continue

                document = {
                    "_id": entity_details.get("id"),
                    "title": entity.get("title"),
                    "_timestamp": entity.get("lastModified"),
                    "body": entity.get("excerpt"),
                    "type": entity.get("entityType"),
                    "url": os.path.join(
                        self.confluence_client.host_url, entity.get("url")[1:]
                    ),
                }
                download_url = None
                if document["type"] == "content":
                    document.update(
                        {
                            "type": entity_details.get("type"),
                            "space": entity_details.get("space", {}).get("name"),
                        }
                    )

                    if document["type"] == "attachment":
                        container_type = entity_details.get("container", {}).get("type")
                        container_title = entity_details.get("container", {}).get(
                            "title"
                        )
                        file_size = entity_details.get("extensions", {}).get("fileSize")
                        document.update(
                            {"size": file_size, container_type: container_title}
                        )
                        # Removing body as attachment will be downloaded lazily
                        document.pop("body")
                        download_url = entity_details.get("_links", {}).get("download")

                yield document, download_url

    async def download_attachment(self, url, attachment, timestamp=None, doit=False):
        """Downloads the content of the given attachment in chunks using REST API call

        Args:
            url (str): url endpoint to download the attachment content
            attachment (dict): Dictionary containing details of the attachment
            timestamp (timestamp, optional): Timestamp of Confluence last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            Dictionary: Document of the attachment to be indexed.
        """
        attachment_size = int(attachment["size"])
        if not (doit and attachment_size):
            return
        attachment_name = attachment["title"]
        file_extension = os.path.splitext(attachment_name)[-1]
        if file_extension.lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.warning(f"{attachment_name} can't be extracted")
            return

        if attachment_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return
        self._logger.debug(
            f"Downloading {attachment_name} of size {attachment_size} bytes"
        )
        document = {"_id": attachment["_id"], "_timestamp": attachment["_timestamp"]}
        source_file_name = ""
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            async for response in self.confluence_client.api_call(
                url=os.path.join(self.confluence_client.host_url, url),
            ):
                async for data in response.content.iter_chunked(n=CHUNK_SIZE):
                    await async_buffer.write(data)
            source_file_name = str(async_buffer.name)

        self._logger.debug(
            f"Download completed for file: {attachment_name}. Calling convert_to_b64"
        )
        await asyncio.to_thread(
            convert_to_b64,
            source=source_file_name,
        )
        async with aiofiles.open(file=source_file_name, mode="r") as target_file:
            # base64 on macOS will add a EOL, so we strip() here
            document["_attachment"] = (await target_file.read()).strip()
        await remove(source_file_name)
        self._logger.debug(f"Downloaded {attachment_name} for {attachment_size} bytes ")
        return document

    async def _attachment_coro(self, document):
        """Coroutine to add attachments to Queue and download content

        Args:
            document (dict): Formatted document of page/blogpost
        """
        async for attachment, download_link in self.fetch_attachments(
            content_id=document["_id"],
            parent_name=document["title"],
            parent_space=document["space"],
            parent_type=document["type"],
        ):
            await self.queue.put(
                (  # pyright: ignore
                    attachment,
                    partial(
                        self.download_attachment,
                        download_link[1:],
                        copy(attachment),
                    ),
                )
            )
        await self.queue.put(END_SIGNAL)  # pyright: ignore

    async def _space_coro(self):
        """Coroutine to add spaces documents to Queue"""
        async for space in self.fetch_spaces():
            await self.queue.put((space, None))  # pyright: ignore
        await self.queue.put(END_SIGNAL)  # pyright: ignore

    async def _page_blog_coro(self, api_query):
        """Coroutine to add pages/blogposts to Queue

        Args:
            api_query (str): API Query Parameters for fetching page/blogpost
        """
        async for document, attachment_count in self.fetch_documents(api_query):
            await self.queue.put((document, None))  # pyright: ignore
            if attachment_count > 0:
                await self.fetchers.put(partial(self._attachment_coro, copy(document)))
                self.fetcher_count += 1
        await self.queue.put(END_SIGNAL)  # pyright: ignore

    async def _consumer(self):
        """Async generator to process entries of the queue

        Yields:
            dictionary: Documents from Confluence.
        """
        while self.fetcher_count > 0:
            _, item = await self.queue.get()
            if item == END_SIGNAL:
                self.fetcher_count -= 1
            else:
                yield item

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch Confluence content in async manner.

        Args:
            filtering (Filtering): Object of Class Filtering
        Yields:
            dictionary: dictionary containing meta-data of the content.
        """
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            for query_info in advanced_rules:
                query = query_info.get("query")
                logger.debug(f"Fetching confluence content using custom query: {query}")
                async for document, download_link in self.search_by_query(query):
                    if download_link:
                        yield document, partial(
                            self.download_attachment,
                            download_link[1:],
                            copy(document),
                        )
                    else:
                        yield document, None

        else:
            if self.spaces == [WILDCARD]:
                configured_spaces_query = "cql=type="
            else:
                quoted_spaces = "','".join(self.spaces)
                configured_spaces_query = f"cql=space in ('{quoted_spaces}') AND type="
            await self.fetchers.put(self._space_coro)
            await self.fetchers.put(
                partial(
                    self._page_blog_coro,
                    f"{configured_spaces_query}blogpost&{CONTENT_QUERY}",
                )
            )
            await self.fetchers.put(
                partial(
                    self._page_blog_coro,
                    f"{configured_spaces_query}page&{CONTENT_QUERY}",
                )
            )
            self.fetcher_count += 3

            async for item in self._consumer():
                yield item
            await self.fetchers.join()
