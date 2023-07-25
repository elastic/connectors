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
from datetime import datetime
from functools import partial
from urllib import parse

import aiofiles
import aiohttp
import pytz
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.logger import logger
from connectors.source import BaseDataSource
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

RETRY_INTERVAL = 2
FILE_SIZE_LIMIT = 10485760

FETCH_SIZE = 100
CHUNK_SIZE = 1024
QUEUE_MEM_SIZE = 5 * 1024 * 1024  # Size in Megabytes
MAX_CONCURRENCY = 5
MAX_CONCURRENT_DOWNLOADS = 100  # Max concurrent download supported by jira

PING = "ping"
PROJECT = "project"
PROJECT_BY_KEY = "project_by_key"
ISSUES = "all_issues"
ISSUE_DATA = "issue_data"
ATTACHMENT_CLOUD = "attachment_cloud"
ATTACHMENT_SERVER = "attachment_server"
URLS = {
    PING: "/rest/api/2/myself",
    PROJECT: "/rest/api/2/project?expand=description,lead,url",
    PROJECT_BY_KEY: "/rest/api/2/project/{key}",
    ISSUES: "/rest/api/2/search?jql={jql}&maxResults={max_results}&startAt={start_at}",
    ISSUE_DATA: "/rest/api/2/issue/{id}",
    ATTACHMENT_CLOUD: "/rest/api/2/attachment/content/{attachment_id}",
    ATTACHMENT_SERVER: "/secure/attachment/{attachment_id}/{attachment_name}",
}

JIRA_CLOUD = "jira_cloud"
JIRA_SERVER = "jira_server"


class JiraClient:
    """Jira client to handle API calls made to Jira"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.is_cloud = self.configuration["data_source"] == JIRA_CLOUD
        self.host_url = self.configuration["jira_url"]
        self.projects = self.configuration["projects"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]
        self.retry_count = self.configuration["retry_count"]

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
            login, password = (
                self.configuration["account_email"],
                self.configuration["api_token"],
            )
        else:
            login, password = (
                self.configuration["username"],
                self.configuration["password"],
            )

        basic_auth = aiohttp.BasicAuth(login=login, password=password)
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

    async def api_call(self, url_name, **url_kwargs):
        """Make a GET call for Atlassian API using the passed url_name with retry for the failed API calls.

        Args:
            url_name (str): URL Name to identify the API endpoint to hit
            url_kwargs (dict): Url kwargs to format the query.

        Raises:
            exception: An instance of an exception class.

        Yields:
            response: Return api response.
        """
        retry = 0
        url = parse.urljoin(self.host_url, URLS[url_name].format(**url_kwargs))
        while True:
            try:
                async with self._get_session().get(  # pyright: ignore
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
                    await self.close_session()
                retry += 1
                if retry > self.retry_count:
                    raise exception
                self._logger.warning(
                    f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}"
                )
                await self._sleeps.sleep(RETRY_INTERVAL**retry)

    async def paginated_api_call(self, url_name, jql=None):
        """Make a paginated API call for Jira objects using the passed url_name with retry for the failed API calls.

        Args:
            url_name (str): URL Name to identify the API endpoint to hit
            jql (str, None): Jira Query Language to filter the issues.

        Yields:
            response: Return api response.
        """
        start_at = 0

        while True:
            try:
                async for response in self.api_call(
                    url_name=url_name,
                    start_at=start_at,
                    max_results=FETCH_SIZE,
                    jql=jql,
                ):
                    response_json = await response.json()
                    total = response_json["total"]
                    yield response_json
                    if start_at + FETCH_SIZE > total or total <= FETCH_SIZE:
                        return
                    start_at += FETCH_SIZE
            except Exception as exception:
                self._logger.warning(
                    f"Skipping data for type: {url_name}, query params: jql={jql}, startAt={start_at}, maxResults={FETCH_SIZE}. Error: {exception}."
                )
                break


class JiraDataSource(BaseDataSource):
    """Jira"""

    name = "Jira"
    service_type = "jira"
    advanced_rules_enabled = True

    def __init__(self, configuration):
        """Setup the connection to the Jira

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
            logger_ (DocumentLogger): Object of DocumentLogger class.
        """
        super().__init__(configuration=configuration)
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.jira_client = JiraClient(configuration=configuration)

        self.tasks = 0
        self.queue = MemQueue(maxmemsize=QUEUE_MEM_SIZE, refresh_timeout=120)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)

    def _set_internal_logger(self):
        self.jira_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Jira

        Returns:
            dictionary: Default configuration.
        """
        return {
            "data_source": {
                "display": "dropdown",
                "label": "Jira data source",
                "options": [
                    {"label": "Jira Cloud", "value": JIRA_CLOUD},
                    {"label": "Jira Server", "value": JIRA_SERVER},
                ],
                "order": 1,
                "type": "str",
                "value": JIRA_CLOUD,
            },
            "username": {
                "depends_on": [{"field": "data_source", "value": JIRA_SERVER}],
                "label": "Jira Server username",
                "order": 2,
                "type": "str",
                "value": "admin",
            },
            "password": {
                "depends_on": [{"field": "data_source", "value": JIRA_SERVER}],
                "label": "Jira Server password",
                "sensitive": True,
                "order": 3,
                "type": "str",
                "value": "changeme",
            },
            "account_email": {
                "depends_on": [{"field": "data_source", "value": JIRA_CLOUD}],
                "label": "Jira Cloud service account id",
                "order": 4,
                "type": "str",
                "value": "me@example.com",
            },
            "api_token": {
                "depends_on": [{"field": "data_source", "value": JIRA_CLOUD}],
                "label": "Jira Cloud API token",
                "order": 5,
                "sensitive": True,
                "type": "str",
                "value": "abc#123",
            },
            "jira_url": {
                "label": "Jira host url",
                "order": 6,
                "type": "str",
                "value": "http://127.0.0.1:8080",
            },
            "projects": {
                "display": "textarea",
                "label": "Jira project keys",
                "order": 7,
                "tooltip": "This configurable field is ignored when Advanced Sync Rules are used.",
                "type": "list",
                "value": "*",
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
                "label": "Retries for failed requests",
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

    def advanced_rules_validators(self):
        return [AtlassianAdvancedRulesValidator(self)]

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by jira

        Args:
            options (dictionary): Config bulker options
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    async def close(self):
        """Closes unclosed client session"""
        await self.jira_client.close_session()

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
        if not (doit and attachment_size > 0):
            return

        attachment_name = attachment["filename"]
        if (
            os.path.splitext(attachment_name)[-1]
        ).lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.warning(
                f"{attachment_name} is not supported by TIKA, skipping"
            )
            return

        if attachment_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return

        self._logger.debug(f"Downloading {attachment_name}")

        document = {
            "_id": f"{issue_key}-{attachment['id']}",
            "_timestamp": attachment["created"],
        }
        temp_filename = ""
        attachment_url = (
            ATTACHMENT_CLOUD if self.jira_client.is_cloud else ATTACHMENT_SERVER
        )
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            async for response in self.jira_client.api_call(
                url_name=attachment_url,
                attachment_id=attachment["id"],
                attachment_name=attachment["filename"],
            ):
                async for data in response.content.iter_chunked(CHUNK_SIZE):
                    await async_buffer.write(data)
                temp_filename = str(async_buffer.name)

        self._logger.debug(f"Calling convert_to_b64 for file : {attachment_name}")
        await asyncio.to_thread(convert_to_b64, source=temp_filename)
        async with aiofiles.open(file=temp_filename, mode="r") as async_buffer:
            # base64 on macOS will add a EOL, so we strip() here
            document["_attachment"] = (await async_buffer.read()).strip()
        try:
            await remove(temp_filename)
        except Exception as exception:
            self._logger.warning(
                f"Could not remove file from: {temp_filename}. Error: {exception}"
            )
        return document

    async def ping(self):
        """Verify the connection with Jira"""
        try:
            await anext(self.jira_client.api_call(url_name=PING))
            self._logger.debug("Successfully connected to the Jira")
        except Exception:
            self._logger.exception("Error while connecting to the Jira")
            raise

    async def _verify_projects(self):
        """Checks if user configured projects are available in jira

        Raises:
            Exception: Configured unavailable projects: <unavailable_project_keys>
        """
        if self.jira_client.projects == ["*"]:
            return
        project_keys = []
        try:
            async for response in self.jira_client.api_call(url_name=PROJECT):
                response = await response.json()
                project_keys = [project["key"] for project in response]
            if unavailable_projects := set(self.jira_client.projects) - set(
                project_keys
            ):
                raise Exception(
                    f"Configured unavailable projects: {', '.join(unavailable_projects)}"
                )
        except Exception as exception:
            raise Exception(
                f"Unable to verify projects: {self.jira_client.projects}. Error: {exception}"
            ) from exception

    async def _get_timezone(self):
        """Returns the timezone of the Jira deployment"""
        async for response in self.jira_client.api_call(url_name=PING):
            timezone = await response.json()
            return timezone["timeZone"]

    async def _put_projects(self, project, timestamp):
        """Store project documents to queue

        Args:
            project (dict): Project document to store in queue
            timestamp (str): Timestamp to manage project document
        """
        document = {
            "_id": f"project-{project['id']}",
            "_timestamp": timestamp,
            "Type": "Project",
            "Project": project,
        }
        await self.queue.put((document, None))  # pyright: ignore

    async def _get_projects(self):
        """Get projects with the help of REST APIs

        Yields:
            project: Project document to get indexed
        """
        try:
            timezone = await self._get_timezone()

            timestamp = iso_utc(
                when=datetime.now(pytz.timezone(timezone))  # pyright: ignore
            )
            if self.jira_client.projects == ["*"]:
                async for response in self.jira_client.api_call(url_name=PROJECT):
                    response = await response.json()
                    for project in response:
                        await self._put_projects(project=project, timestamp=timestamp)
            else:
                for project_key in self.jira_client.projects:
                    async for response in self.jira_client.api_call(
                        url_name=PROJECT_BY_KEY, key=project_key
                    ):
                        project = await response.json()
                        await self._put_projects(project=project, timestamp=timestamp)
            await self.queue.put("FINISHED")  # pyright: ignore
        except Exception as exception:
            self._logger.warning(
                f"Skipping data for type: {PROJECT}. Error: {exception}"
            )

    async def _put_issue(self, issue):
        """Put specific issue as per the given issue_key in a queue

        Args:
            issue (str): Issue key to fetch an issue
        """
        try:
            async for response in self.jira_client.api_call(
                url_name=ISSUE_DATA, id=issue["key"]
            ):
                issue = await response.json()
                response_fields = issue.get("fields")
                document = {
                    "_id": f"{response_fields['project']['name']}-{issue['key']}",
                    "_timestamp": response_fields["updated"],
                    "Type": response_fields["issuetype"]["name"],
                    "Issue": response_fields,
                }
                await self.queue.put((document, None))  # pyright: ignore
                attachments = issue["fields"]["attachment"]
                if len(attachments) > 0:
                    await self._put_attachment(
                        attachments=attachments, issue_key=issue["key"]
                    )
            await self.queue.put("FINISHED")  # pyright: ignore
        except Exception as exception:
            self._logger.warning(
                f"Skipping data for type: {ISSUE_DATA}. Error: {exception}"
            )

    async def _get_issues(self, custom_query=""):
        """Get issues with the help of REST APIs

        Yields:
            Dictionary: Jira issue to get indexed
            issue (dict): Issue response to fetch the attachments
        """
        wildcard_query = ""
        projects_query = f"project in ({','.join(self.jira_client.projects)})"

        jql = custom_query or (
            wildcard_query if self.jira_client.projects == ["*"] else projects_query
        )

        async for response in self.jira_client.paginated_api_call(
            url_name=ISSUES, jql=jql
        ):
            for issue in response.get("issues", []):
                await self.fetchers.put(partial(self._put_issue, issue))
                self.tasks += 1
        await self.queue.put("FINISHED")  # pyright: ignore

    async def _put_attachment(self, attachments, issue_key):
        """Put attachments of a specific issue in a queue

        Args:
            attachments (list): List of attachments for an issue
            issue_key (str): Issue key for generating `_id` field
        """
        for attachment in attachments:
            document = {
                "_id": f"{issue_key}-{attachment['id']}",
                "title": attachment["filename"],
                "Type": "Attachment",
                "issue": issue_key,
                "_timestamp": attachment["created"],
                "size": attachment["size"],
            }
            await self.queue.put(
                (  # pyright: ignore
                    document,
                    partial(
                        self.get_content,
                        issue_key=issue_key,
                        attachment=copy(attachment),
                    ),
                )
            )

    async def _consumer(self):
        """Async generator to process entries of the queue

        Yields:
            dictionary: Documents from Jira.
        """
        while self.tasks > 0:
            _, item = await self.queue.get()
            if item == "FINISHED":
                self.tasks -= 1
            else:
                yield item

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch jira objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()

            for rule in advanced_rules:
                await self.fetchers.put(
                    partial(self._get_issues, rule.get("query", ""))
                )
                self.tasks += 1

        else:
            await self._verify_projects()

            await self.fetchers.put(self._get_projects)
            await self.fetchers.put(self._get_issues)
            self.tasks += 2

        async for item in self._consumer():
            yield item

        await self.fetchers.join()
