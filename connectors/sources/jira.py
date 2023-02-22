#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Jira source module responsible to fetch documents from Jira on-prem or cloud server.
"""
import asyncio
import os
from copy import copy
from functools import partial
from urllib import parse

import aiofiles
import aiohttp
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    ConcurrentTasks,
    MemQueue,
    convert_to_b64,
    iso_utc,
)

IS_CLOUD = True
RETRY_INTERVAL = 2
DEFAULT_CONTENT_EXTRACTION = True
DEFAULT_SSL_DISABLED = True
RETRIES = 3
FILE_SIZE_LIMIT = 10485760

FETCH_SIZE = 100
CHUNK_SIZE = 1024
QUEUE_SIZE = 1024
QUEUE_MEM_SIZE = 5
MAX_CONCURRENCY = 5
MAX_CONCURRENT_DOWNLOADS = 50  # Max concurrent download supported by jira

PING = "ping"
PROJECT = "project"
ISSUES = "all_issues"
ISSUE_DATA = "issue_data"
ATTACHMENT_CLOUD = "attachment_cloud"
ATTACHMENT_SERVER = "attachment_server"
URLS = {
    PING: "/rest/api/2/myself",
    PROJECT: "/rest/api/2/project?expand=description,lead,url",
    ISSUES: "/rest/api/2/search?maxResults={maxResults}&startAt={startAt}",
    ISSUE_DATA: "/rest/api/2/issue/{id}",
    ATTACHMENT_CLOUD: "/rest/api/2/attachment/content/{attachment_id}",
    ATTACHMENT_SERVER: "/secure/attachment/{attachment_id}/{attachment_name}",
}


class JiraDataSource(BaseDataSource):
    """Jira"""

    name = "Jira"
    service_type = "jira"

    def __init__(self, configuration):
        """Setup the connection to the Jira

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.is_cloud = self.configuration["is_cloud"]
        self.host_url = self.configuration["host_url"]
        self.ssl_disabled = self.configuration["ssl_disabled"]
        self.certificate = self.configuration["ssl_ca"]
        self.enable_content_extraction = self.configuration["enable_content_extraction"]
        self.retry_count = self.configuration["retry_count"]
        self.concurrent_downloads = self.configuration["concurrent_downloads"]

        self.ssl_ctx = False
        self.session = None
        self.queue = MemQueue(
            maxsize=QUEUE_SIZE, maxmemsize=QUEUE_MEM_SIZE * 1024 * 1024
        )
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Jira

        Returns:
            dictionary: Default configuration.
        """
        return {
            "is_cloud": {
                "value": IS_CLOUD,
                "label": "True if Jira Cloud, False if Jira Server",
                "type": "bool",
            },
            "username": {
                "value": "admin",
                "label": "Jira Server username",
                "type": "str",
            },
            "password": {
                "value": "changeme",
                "label": "Jira Server password",
                "type": "str",
            },
            "service_account_id": {
                "value": "me@example.com",
                "label": "Jira Cloud service account id",
                "type": "str",
            },
            "api_token": {
                "value": "abc#123",
                "label": "Jira Cloud API token",
                "type": "str",
            },
            "host_url": {
                "value": "http://127.0.0.1:8080",
                "label": "Jira host url",
                "type": "str",
            },
            "ssl_disabled": {
                "value": DEFAULT_SSL_DISABLED,
                "label": "Disable SSL verification. True if SSL is disabled else False",
                "type": "bool",
            },
            "ssl_ca": {
                "value": "",
                "label": "SSL certificate",
                "type": "str",
            },
            "enable_content_extraction": {
                "value": DEFAULT_CONTENT_EXTRACTION,
                "label": "Enable content extraction (true/false)",
                "type": "bool",
            },
            "retry_count": {
                "value": RETRIES,
                "label": "Maximum retries for failed requests",
                "type": "int",
            },
            "concurrent_downloads": {
                "value": 50,
                "label": "Number of concurrent downloads for fetching attachment content",
                "type": "int",
            },
        }

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by jira

        Args:
            options (dictionary): Config bulker options

        Raises:
            Exception: Invalid configured concurrent_downloads
        """
        if self.concurrent_downloads > MAX_CONCURRENT_DOWNLOADS:
            raise Exception(
                f"Configured concurrent downloads can't be set more than {MAX_CONCURRENT_DOWNLOADS}."
            )
        options["concurrent_downloads"] = self.concurrent_downloads

    def _validate_configuration(self):
        """Validates whether user input is empty or not for configuration fields

        Raises:
            Exception: Configured keys can't be empty
        """
        logger.info("Validating Jira Configuration...")
        connection_fields = (
            ["host_url", "service_account_id", "api_token"]
            if self.is_cloud
            else ["host_url", "username", "password"]
        )

        if empty_connection_fields := [
            field for field in connection_fields if self.configuration[field] == ""
        ]:
            raise Exception(
                f"Configured keys: {empty_connection_fields} can't be empty."
            )

        if not self.ssl_disabled and self.certificate == "":
            raise Exception("When ssl_disabled is False then ssl_ca can't be empty.")

    def _generate_session(self):
        """Generates a new session"""
        logger.debug("Creating a session...")
        if self.is_cloud:
            auth = (
                self.configuration["service_account_id"],
                self.configuration["api_token"],
            )
        else:
            auth = self.configuration["username"], self.configuration["password"]

        basic_auth = aiohttp.BasicAuth(
            login=auth[0],
            password=auth[1],
        )
        request_headers = {
            "accept": "application/json",
            "content-type": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=None)
        return aiohttp.ClientSession(
            auth=basic_auth,
            headers=request_headers,
            timeout=timeout,
            raise_for_status=True,
        )

    async def close(self):
        """Closes unclosed client session"""
        if self.session is None:
            return
        await self.session.close()

    async def _api_call(self, url_name, is_pagination=False, **url_kwargs):
        """Method for adding retries whenever exception raised during an api calls

        Args:
            url_name (str): Jira query name to be executed.
            is_pagination (bool): Flag to check if the pagination is required
            url_kwargs (dict): Url kwargs to format the query.

        Raises:
            exception: An instance of an exception class.

        Yields:
            response: Return api response.
        """
        retry = 0
        start_at = 0
        url = parse.urljoin(self.host_url, URLS[url_name].format(**url_kwargs))

        while True:
            try:
                async with self.session.get(
                    url=url,
                    ssl=self.ssl_ctx,
                ) as response:
                    if is_pagination:
                        response_json = await response.json()
                        total = response_json.get("total")
                        yield response_json
                        if start_at + FETCH_SIZE >= total or total <= FETCH_SIZE:
                            break
                        start_at += FETCH_SIZE
                        url = parse.urljoin(
                            self.host_url,
                            URLS[url_name].format(
                                startAt=start_at, maxResults=FETCH_SIZE
                            ),
                        )
                        retry = 0
                    else:
                        yield response
                        break
            except Exception as exception:
                if isinstance(
                    exception, aiohttp.client_exceptions.ServerDisconnectedError
                ):
                    await self.close()
                    self.session = self._generate_session()
                retry += 1
                if retry > self.retry_count:
                    raise exception
                logger.warning(
                    f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}"
                )
                await asyncio.sleep(RETRY_INTERVAL**retry)

    async def get_content(self, issue_key, attachment, timestamp=None, doit=False):
        """Extracts the content for allowed file types.

        Args:
            issue_key (str): Issue key to generate `_id` for attachment document
            attachment (dictionary): Formatted attachment document.
            timestamp (timestamp, optional): Timestamp of attachment last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and attachment content
        """
        attachment_size = int(attachment["size"])
        if not (self.enable_content_extraction and doit and attachment_size > 0):
            return

        attachment_name = attachment["filename"]
        if os.path.splitext(attachment_name)[-1] not in TIKA_SUPPORTED_FILETYPES:
            logger.debug(f"{attachment_name} is not supported by TIKA, skipping")
            return

        if attachment_size > FILE_SIZE_LIMIT:
            logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return

        logger.debug(f"Downloading {attachment_name}")

        document = {
            "_id": f"{issue_key}-{attachment['id']}",
            "_timestamp": attachment["created"],
        }
        temp_filename = ""
        attachment_url = ATTACHMENT_CLOUD if self.is_cloud else ATTACHMENT_SERVER
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            async for response in self._api_call(
                url_name=attachment_url,
                attachment_id=attachment["id"],
                attachment_name=attachment["filename"],
            ):
                async for data in response.content.iter_chunked(CHUNK_SIZE):
                    await async_buffer.write(data)
                temp_filename = async_buffer.name

        logger.debug(f"Calling convert_to_b64 for file : {attachment_name}")
        await asyncio.to_thread(convert_to_b64, source=temp_filename)
        async with aiofiles.open(file=temp_filename, mode="r") as async_buffer:
            # base64 on macOS will add a EOL, so we strip() here
            document["_attachment"] = (await async_buffer.read()).strip()
        await remove(temp_filename)
        return document

    async def ping(self):
        """Verify the connection with Jira"""
        self._validate_configuration()
        self.session = self._generate_session()
        if not self.ssl_disabled:
            self.ssl_ctx = self._ssl_context(certificate=self.certificate)
        try:
            await anext(self._api_call(url_name=PING))
            logger.info("Successfully connected to the Jira")
        except Exception:
            logger.exception("Error while connecting to the Jira")
            raise

    async def _get_projects(self):
        """Get projects with the help of REST APIs

        Yields:
            project: Project document to get indexed
        """
        async for response in self._api_call(url_name=PROJECT):
            response = await response.json()
            for project in response:
                yield {
                    "_id": f"{project['name']}-{project['id']}",
                    "_timestamp": iso_utc(),
                    "Type": "Project",
                    "Project": project,
                }

    async def _get_issues(self):
        """Get issues with the help of REST APIs

        Yields:
            Dictionary: Jira issue to get indexed
            issue (dict): Issue response to fetch the attachments
        """
        async for response in self._api_call(
            url_name=ISSUES, is_pagination=True, maxResults=FETCH_SIZE, startAt=0
        ):
            for issue in response.get("issues", []):
                async for response in self._api_call(
                    url_name=ISSUE_DATA, id=issue["key"]
                ):
                    issue = await response.json()
                    if issue:
                        response_fields = issue.get("fields")
                        yield {
                            "_id": f"{response_fields['project']['name']}-{issue['key']}",
                            "_timestamp": response_fields["updated"],
                            "Type": response_fields["issuetype"]["name"],
                            "Issue": response_fields,
                        }, issue

    async def _get_attachments(self, attachments, issue_key):
        """Get attachments of a specific issue

        Args:
            attachments (list): List of attachments for an issue
            issue_key (str): Issue key for generating `_id` field

        Yields:
            Dictionary: Jira attachment on the given issue
            attachment (dict): Attachment dictionary for extracting the content
        """
        for attachment in attachments:
            yield {
                "_id": f"{issue_key}-{attachment['id']}",
                "title": attachment["filename"],
                "type": "Attachment",
                "issue": issue_key,
                "_timestamp": attachment["created"],
                "size": attachment["size"],
            }, attachment

    async def _grab_content(self, attachments, issue_key):
        """Coroutine to add attachments to Queue and get content

        Args:
            attachments (list): List of attachments for an issue
            issue_key (str): Issue key for generating `_id` field
        """
        async for content, attachment in self._get_attachments(
            attachments=attachments, issue_key=issue_key
        ):
            if attachment:
                await self.queue.put(
                    (
                        content,
                        partial(
                            self.get_content,
                            issue_key=issue_key,
                            attachment=copy(attachment),
                        ),
                    )
                )

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch jira objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """

        async def _project_task():
            """Coroutine to add projects documents to Queue"""
            async for project_data in self._get_projects():
                await self.queue.put((project_data, None))
            await self.queue.put("FINISHED")

        async def _document_task():
            """Coroutine to add issues/attachments to Queue"""
            async for document, issue in self._get_issues():
                await self.queue.put((document, None))
                attachments = issue["fields"]["attachment"]
                if len(attachments) > 0:
                    await self.fetchers.put(
                        partial(self._grab_content, attachments, issue["key"])
                    )
            await self.queue.put("FINISHED")

        project_task = asyncio.create_task(_project_task())
        attachment_task = asyncio.create_task(_document_task())
        completed_tasks = 0
        while True:
            _, item = await self.queue.get()
            if item == "FINISHED":
                completed_tasks += 1
                if completed_tasks == 2:
                    break
            else:
                yield item

        await self.fetchers.join()
        await asyncio.gather(project_task, attachment_task)
