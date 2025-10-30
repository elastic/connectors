#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from urllib import parse

import aiohttp
from aiohttp import ClientResponseError, ServerConnectionError
from connectors_sdk.logger import logger

from connectors.sources.atlassian.jira.constants import (
    ALL_FIELDS,
    DEFAULT_RETRY_SECONDS,
    FETCH_SIZE,
    ISSUE_DATA,
    ISSUE_SECURITY_LEVEL,
    ISSUES,
    JIRA_CLOUD,
    JIRA_DATA_CENTER,
    JIRA_SERVER,
    MAX_USER_FETCH_LIMIT,
    PERMISSIONS_BY_KEY,
    PING,
    PROJECT,
    PROJECT_BY_KEY,
    PROJECT_ROLE_MEMBERS_BY_ROLE_ID,
    RETRIES,
    RETRY_INTERVAL,
    SECURITY_LEVEL_MEMBERS,
    URLS,
)
from connectors.sources.atlassian.utils import (
    prefix_account_id,
    prefix_account_name,
    prefix_group_id,
)
from connectors.utils import CancellableSleeps, RetryStrategy, retryable, ssl_context


class ThrottledError(Exception):
    """Internal exception class to indicate that request was throttled by the API"""

    pass


class InternalServerError(Exception):
    pass


class NotFound(Exception):
    pass


class InvalidJiraDataSourceTypeError(ValueError):
    pass


class EmptyResponseError(Exception):
    """Exception raised when the API response is empty."""

    pass


class JiraClient:
    """Jira client to handle API calls made to Jira"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.data_source_type = self.configuration["data_source"]

        jira_url = self.configuration["jira_url"]
        self.host_url = jira_url if jira_url[-1] == "/" else jira_url + "/"

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

        self._logger.debug(f"Creating a '{self.data_source_type}' client session")
        if self.data_source_type == JIRA_CLOUD:
            login, password = (
                self.configuration["account_email"],
                self.configuration["api_token"],
            )
        elif self.data_source_type == JIRA_SERVER:
            login, password = (
                self.configuration["username"],
                self.configuration["password"],
            )
        elif self.data_source_type == JIRA_DATA_CENTER:
            login, password = (
                self.configuration["data_center_username"],
                self.configuration["data_center_password"],
            )
        else:
            msg = (
                f"Unknown data source type '{self.data_source_type}' for Jira connector"
            )
            self._logger.error(msg)

            raise InvalidJiraDataSourceTypeError(msg)

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

    async def _handle_client_errors(self, url, exception):
        if exception.status == 429:
            response_headers = exception.headers or {}
            retry_seconds = DEFAULT_RETRY_SECONDS
            if "Retry-After" in response_headers:
                try:
                    retry_seconds = int(response_headers["Retry-After"])
                except (TypeError, ValueError) as exception:
                    self._logger.error(
                        f"Error while reading value of retry-after header {exception}. Using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
                    )
            else:
                self._logger.warning(
                    f"Rate Limited but Retry-After header is not found, using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
                )
            self._logger.debug(f"Rate Limit reached: retry in {retry_seconds} seconds")

            await self._sleeps.sleep(retry_seconds)
            raise ThrottledError
        elif exception.status == 404:
            self._logger.error(f"Getting Not Found Error for url: {url}")
            raise NotFound
        elif exception.status == 500:
            self._logger.error("Internal Server Error occurred")
            raise InternalServerError
        else:
            raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=NotFound,
    )
    async def api_call(self, url_name=None, **url_kwargs):
        """Make a GET call for Atlassian API using the passed url_name with retry for the failed API calls.

        Args:
            url_name (str): URL Name to identify the API endpoint to hit
            url_kwargs (dict): Url kwargs to format the query.

        Raises:
            exception: An instance of an exception class.

        Yields:
            response: Return api response.
        """
        url = url_kwargs.get("url") or parse.urljoin(
            self.host_url,
            URLS[url_name].format(**url_kwargs),  # pyright: ignore
        )
        self._logger.debug(f"Making a GET call for url: {url}")
        while True:
            try:
                async with self._get_session().get(  # pyright: ignore
                    url=url,
                    ssl=self.ssl_ctx,
                ) as response:
                    if response.content_length == 0:
                        msg = f"The response body is empty for url: {url}"
                        raise EmptyResponseError(msg)
                    yield response
                    break
            except ServerConnectionError:
                await self.close_session()
                raise
            except ClientResponseError as exception:
                await self._handle_client_errors(url=url, exception=exception)

    async def _paginated_api_call_cursor_based(self, url_name, jql=None, **kwargs):
        if not jql and url_name == ISSUES:
            # only bound jql allowed, so using "key IS NOT EMPTY" as a catch-all for "all issues"
            jql = "key%20IS%20NOT%20EMPTY"

        url = parse.urljoin(
            self.host_url,
            URLS[url_name].format(
                jql=jql,
                max_results=FETCH_SIZE,
                **kwargs,
            ),
        )

        self._logger.info(
            f"Started pagination for the API endpoint: {URLS[url_name]} to host: {self.host_url} with the parameters -> "
            f"maxResults: {FETCH_SIZE} and jql query: {jql}"
        )

        next_page_token = None
        while True:
            try:
                async for response in self.api_call(url=url):
                    response_json = await response.json()
                    yield response_json

                    next_page = response_json.get("nextPageToken")
                    if not next_page:
                        return
                    next_page_token = next_page
                    url_template = URLS[url_name] + "&nextPageToken={next_page_token}"
                    url = parse.urljoin(
                        self.host_url,
                        url_template.format(
                            jql=jql,
                            max_results=FETCH_SIZE,
                            next_page_token=next_page_token,
                            **kwargs,
                        ),
                    )
            except Exception as exception:
                self._logger.warning(
                    f"Skipping data for type: {url_name}, query params: jql={jql}, nextPageToken={next_page_token}, maxResults={FETCH_SIZE}. Error: {exception}."
                )
                break

    async def _paginated_api_call_offset_based(self, url_name, jql=None, **kwargs):
        start_at = 0
        self._logger.info(
            f"Started pagination for the API endpoint: {URLS[url_name]} to host: {self.host_url} with the parameters -> startAt: 0, maxResults: {FETCH_SIZE} and jql query: {jql}"
        )
        while True:
            try:
                url = None
                if kwargs.get("level_id"):
                    url = parse.urljoin(
                        self.host_url,
                        URLS[url_name].format(
                            max_results=FETCH_SIZE,
                            start_at=start_at,
                            level_id=kwargs.get("level_id"),
                        ),  # pyright: ignore
                    )
                async for response in self.api_call(
                    url_name=url_name,
                    start_at=start_at,
                    max_results=FETCH_SIZE,
                    jql=jql,
                    url=url,
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

    async def paginated_api_call(self, url_name, jql=None, **kwargs):
        """Make a paginated API call for Jira objects using the passed url_name with retry for the failed API calls.
        Most Jira API endpoints use offset-based pagination. However, some endpoints use cursor-based pagination.
        This method handles both.

        Args:
            url_name (str): URL Name to identify the API endpoint to hit
            jql (str, None): Jira Query Language to filter the issues.

        Yields:
            response: Return api response.
        """
        is_cursor_based_pagination = url_name == ISSUES
        if is_cursor_based_pagination:
            async for response in self._paginated_api_call_cursor_based(
                url_name=url_name, jql=jql, **kwargs
            ):
                yield response
        else:
            async for response in self._paginated_api_call_offset_based(
                url_name=url_name, jql=jql, **kwargs
            ):
                yield response

    async def get_issues_for_jql(self, jql):
        info_msg = (
            f"Fetching Jira issues for JQL query: {jql}"
            if jql
            else "Fetching all Jira issues"
        )
        self._logger.info(info_msg)
        async for response in self.paginated_api_call(url_name=ISSUES, jql=jql):
            for issue in response.get("issues", []):
                yield issue

    async def get_issues_for_issue_key(self, key):
        try:
            async for response in self.api_call(url_name=ISSUE_DATA, id=key):
                issue = await response.json()
                yield issue
        except Exception as exception:
            self._logger.warning(
                f"Skipping data for type: {ISSUE_DATA}. Error: {exception}"
            )

    async def get_projects(self):
        if self.projects == ["*"]:
            self._logger.info("Fetching all Jira projects")
            async for response in self.api_call(url_name=PROJECT):
                response = await response.json()
                for project in response:
                    yield project
        else:
            self._logger.info(
                f"Fetching user configured Jira projects: {self.projects}"
            )
            for project_key in self.projects:
                async for response in self.api_call(
                    url_name=PROJECT_BY_KEY, key=project_key
                ):
                    project = await response.json()
                    yield project

    async def user_information_list(self, key):
        start_at = 0
        while True:
            async for users in self.api_call(
                url_name=PERMISSIONS_BY_KEY,
                key=key,
                start_at=start_at,
                max_results=MAX_USER_FETCH_LIMIT,
            ):
                response = await users.json()
                if len(response) == 0:
                    return
                yield response
                start_at += MAX_USER_FETCH_LIMIT

    async def project_role_members(self, project, role_id, access_control):
        self._logger.debug(
            f"Fetching users and groups with role ID '{role_id}' for project '{project['key']}'"
        )
        async for actor_response in self.api_call(
            url_name=PROJECT_ROLE_MEMBERS_BY_ROLE_ID,
            project_key=project.get("key"),
            role_id=role_id,
        ):
            actors = await actor_response.json()
            for actor in actors.get("actors", []):
                if actor.get("actorUser"):
                    access_control.add(
                        prefix_account_id(
                            account_id=actor.get("actorUser").get("accountId")
                        )
                    )
                    access_control.add(
                        prefix_account_name(account_name=actor.get("displayName"))
                    )
                elif actor.get("actorGroup"):
                    access_control.add(
                        prefix_group_id(group_id=actor.get("actorGroup").get("groupId"))
                    )
            yield access_control

    async def issue_security_level(self, issue_key):
        self._logger.debug(f"Fetching security level for issue: {issue_key}")
        async for response in self.api_call(
            url_name=ISSUE_SECURITY_LEVEL, issue_key=issue_key
        ):
            yield await response.json()

    async def issue_security_level_members(self, level_id):
        self._logger.debug(f"Fetching members for issue security level: {level_id}")
        async for response in self.paginated_api_call(
            url_name=SECURITY_LEVEL_MEMBERS, level_id=level_id
        ):
            yield response

    async def get_timezone(self):
        async for response in self.api_call(url_name=PING):
            timezone = await response.json()
            return timezone.get("timeZone")

    async def verify_projects(self):
        if self.projects == ["*"]:
            return

        self._logger.info(f"Verifying the configured projects: {self.projects}")
        project_keys = []
        try:
            async for response in self.api_call(url_name=PROJECT):
                response = await response.json()
                project_keys = [project.get("key") for project in response]
            if unavailable_projects := set(self.projects) - set(project_keys):
                msg = f"Configured unavailable projects: {', '.join(unavailable_projects)}"
                raise Exception(msg)
        except Exception as exception:
            msg = f"Unable to verify projects: {self.projects}. Error: {exception}"
            raise Exception(msg) from exception

    async def ping(self):
        await anext(self.api_call(url_name=PING))

    async def get_jira_fields(self):
        async for response in self.api_call(url_name=ALL_FIELDS):
            jira_fields = await response.json()
            yield {
                field["id"]: field["name"]
                for field in jira_fields
                if field["custom"] is True
            }
