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
from connectors.source import BaseDataSource
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
SPACE_QUERY = "limit=100"
ATTACHMENT_QUERY = "limit=100&expand=version"
CONTENT_QUERY = (
    "limit=100&expand=children.attachment,history.lastUpdated,body.storage,space"
)

URLS = {
    SPACE: "rest/api/space?{api_query}",
    CONTENT: "rest/api/content?{api_query}",
    ATTACHMENT: "rest/api/content/{id}/child/attachment?{api_query}",
}
PING_URL = "rest/api/space?limit=1"
MAX_CONCURRENT_DOWNLOADS = 50  # Max concurrent download supported by confluence
CHUNK_SIZE = 1024
MAX_CONCURRENCY = 50
QUEUE_SIZE = 1024
QUEUE_MEM_SIZE = 25 * 1024 * 1024  # Size in Megabytes
END_SIGNAL = "FINISHED_TASK"


class ConfluenceClient:
    """Confluence client to handle API calls made to Confluence"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self.is_cloud = self.configuration["is_cloud"]
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
                logger.warning(
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
                logger.warning(
                    f"Skipping data for type {url_name} from {url}. Exception: {exception}."
                )
                break


class ConfluenceDataSource(BaseDataSource):
    """Confluence"""

    name = "Confluence"
    service_type = "confluence"

    def __init__(self, configuration):
        """Setup the connection to Confluence

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.enable_content_extraction = self.configuration["enable_content_extraction"]
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.confluence_client = ConfluenceClient(configuration)

        self.queue = MemQueue(maxsize=QUEUE_SIZE, maxmemsize=QUEUE_MEM_SIZE)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)
        self.fetcher_count = 0

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Confluence

        Returns:
            dictionary: Default configuration.
        """
        return {
            "is_cloud": {
                "value": False,
                "label": "True if Confluence Cloud, False if Confluence Server",
                "type": "bool",
            },
            "username": {
                "value": "admin",
                "label": "Confluence Server username",
                "type": "str",
            },
            "password": {
                "value": "abc@123",
                "label": "Confluence Server password",
                "type": "str",
            },
            "account_email": {
                "value": "me@example.com",
                "label": "Confluence Cloud account email",
                "type": "str",
            },
            "api_token": {
                "value": "abc#123",
                "label": "Confluence Cloud API token",
                "type": "str",
            },
            "confluence_url": {
                "value": "http://127.0.0.1:5000",
                "label": "Confluence URL",
                "type": "str",
            },
            "ssl_enabled": {
                "value": False,
                "label": "Enable SSL verification (true/false)",
                "type": "bool",
            },
            "ssl_ca": {
                "value": "",
                "label": "SSL certificate",
                "type": "str",
            },
            "enable_content_extraction": {
                "value": True,
                "label": "Enable content extraction (true/false)",
                "type": "bool",
            },
            "retry_count": {
                "value": 3,
                "label": "Maximum retries per request",
                "type": "int",
            },
            "concurrent_downloads": {
                "value": MAX_CONCURRENT_DOWNLOADS,
                "label": "Maximum concurrent downloads",
                "type": "int",
            },
        }

    async def close(self):
        """Closes unclosed client session"""
        await self.confluence_client.close_session()

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by Confluence

        Args:
            options (dictionary): Config bulker options
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields

        Raises:
            Exception: Configured fields can't be empty.
            Exception: SSL certificate must be configured.
            Exception: Concurrent downloads can't be set more than maximum allowed value.
        """
        logger.info("Validating Confluence Configuration...")

        connection_fields = (
            ["confluence_url", "account_email", "api_token"]
            if self.confluence_client.is_cloud
            else ["confluence_url", "username", "password"]
        )
        default_config = self.get_default_configuration()

        if empty_connection_fields := [
            default_config[field]["label"]
            for field in connection_fields
            if self.configuration[field] == ""
        ]:
            raise Exception(
                f"Configured keys: {empty_connection_fields} can't be empty."
            )
        if self.confluence_client.ssl_enabled and (
            self.confluence_client.certificate == ""
            or self.confluence_client.certificate is None
        ):
            raise Exception("SSL certificate must be configured.")

        if self.concurrent_downloads > MAX_CONCURRENT_DOWNLOADS:
            raise Exception(
                f"Configured concurrent downloads can't be set more than {MAX_CONCURRENT_DOWNLOADS}."
            )

    async def ping(self):
        """Verify the connection with Confluence"""
        try:
            await anext(
                self.confluence_client.api_call(
                    url=os.path.join(self.confluence_client.host_url, PING_URL),
                )
            )
            logger.info("Successfully connected to Confluence")
        except Exception:
            logger.exception("Error while connecting to Confluence")
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
        if not (self.enable_content_extraction and doit and attachment_size):
            return
        attachment_name = attachment["title"]
        file_extension = os.path.splitext(attachment_name)[-1]
        if file_extension not in TIKA_SUPPORTED_FILETYPES:
            logger.warning(f"{attachment_name} can't be extracted")
            return

        if attachment_size > FILE_SIZE_LIMIT:
            logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return
        logger.debug(f"Downloading {attachment_name} of size {attachment_size} bytes")
        document = {"_id": attachment["_id"], "_timestamp": attachment["_timestamp"]}
        source_file_name = ""
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            async for response in self.confluence_client.api_call(
                url=os.path.join(self.confluence_client.host_url, url),
            ):
                async for data in response.content.iter_chunked(n=CHUNK_SIZE):
                    await async_buffer.write(data)
            source_file_name = str(async_buffer.name)

        logger.debug(
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
        logger.debug(f"Downloaded {attachment_name} for {attachment_size} bytes ")
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
        await self.fetchers.put(self._space_coro)
        await self.fetchers.put(
            partial(self._page_blog_coro, f"type=blogpost&{CONTENT_QUERY}")
        )
        await self.fetchers.put(partial(self._page_blog_coro, CONTENT_QUERY))
        self.fetcher_count += 3

        async for item in self._consumer():
            yield item
        await self.fetchers.join()
