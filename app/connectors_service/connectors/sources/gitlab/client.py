#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""GitLab GraphQL and REST API client for interacting with GitLab Cloud."""

import os
import ssl
import time
from typing import AsyncGenerator
from urllib.parse import quote

import aiohttp

from connectors_sdk.logger import logger
from connectors_sdk.source import ConfigurableFieldValueError
from connectors.sources.gitlab.models import (
    GitLabDiscussion,
    GitLabLabel,
    GitLabUser,
    MergeRequestsResponse,
    ProjectsResponse,
    ReleasesResponse,
    WorkItemsGroupResponse,
    WorkItemsProjectResponse,
)
from connectors.sources.gitlab.queries import (
    APPROVEDBY_QUERY,
    ASSIGNEES_QUERY,
    DISCUSSIONS_QUERY,
    LABELS_QUERY,
    MERGE_REQUESTS_QUERY,
    NOTES_QUERY,
    PROJECTS_QUERY,
    RELEASES_QUERY,
    REVIEWERS_QUERY,
    WORK_ITEM_ASSIGNEES_QUERY,
    WORK_ITEM_DISCUSSIONS_QUERY,
    WORK_ITEM_GROUP_ASSIGNEES_QUERY,
    WORK_ITEM_GROUP_DISCUSSIONS_QUERY,
    WORK_ITEM_GROUP_LABELS_QUERY,
    WORK_ITEM_LABELS_QUERY,
    WORK_ITEMS_GROUP_QUERY,
    WORK_ITEMS_PROJECT_QUERY,
)
from connectors.sources.gitlab.validation_utils import (
    safe_get_nested,
    validate_with_fallback,
)
from connectors.utils import CancellableSleeps, RetryStrategy, retryable


class GitLabRateLimitException(Exception):
    """Raised when GitLab API rate limit is exceeded."""

    pass


class GitLabGraphQLException(Exception):
    """Raised when GitLab GraphQL query returns errors."""

    pass


class GitLabNotFoundError(Exception):
    """Raised when a GitLab resource is not found (404)."""

    pass


class GitLabForbiddenError(Exception):
    """Raised when access to a GitLab resource is forbidden (403)."""

    pass


class GitLabUnauthorizedError(Exception):
    """Raised when GitLab authentication fails (401)."""

    pass


GITLAB_FTEST_HOST = os.environ.get("GITLAB_FTEST_HOST")
RUNNING_FTEST = "RUNNING_FTEST" in os.environ

GITLAB_CLOUD_URL = (
    GITLAB_FTEST_HOST if (RUNNING_FTEST and GITLAB_FTEST_HOST) else "https://gitlab.com"
)
RETRIES = 3
RETRY_INTERVAL = 2


class GitLabClient:
    """Client for interacting with GitLab Cloud API using aiohttp."""

    def __init__(self, token) -> None:
        self._logger = logger
        self.token = token
        self.api_url = f"{GITLAB_CLOUD_URL}/api/v4"
        self.graphql_url = f"{GITLAB_CLOUD_URL}/api/graphql"
        self._sleeps = CancellableSleeps()
        self._session = None

    def set_logger(self, logger_) -> None:
        self._logger = logger_

    def _extract_id(self, gid: str) -> str | None:
        """Extract ID from GitLab global ID format.

        GitLab GraphQL returns global IDs like 'gid://gitlab/Project/123'.
        This method extracts the ID portion (the last segment).

        Args:
            gid: GitLab global ID string

        Returns:
            ID string or None if extraction fails
        """
        try:
            return gid.split("/")[-1]
        except (AttributeError, IndexError):
            self._logger.warning(f"Failed to parse GitLab ID: {gid}")
            return None

    def _get_session(self):
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            connector_kwargs = {}
            # Disable SSL verification for ftests (self-signed certificates)
            if RUNNING_FTEST and GITLAB_FTEST_HOST:
                self._logger.info(
                    f"FTEST mode enabled: disabling SSL verification for {GITLAB_FTEST_HOST}"
                )
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                connector_kwargs["connector"] = aiohttp.TCPConnector(ssl=ssl_context)

            self._session = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=aiohttp.ClientTimeout(total=300),
                **connector_kwargs,
            )
        return self._session

    async def _handle_rate_limit(self, response) -> bool:
        """Handle rate limiting by sleeping until reset time.

        Args:
            response: aiohttp response object
        """
        if response.status == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after and int(retry_after) > 0:
                sleep_time = int(retry_after)
            else:
                reset_time = response.headers.get("RateLimit-Reset")
                if reset_time:
                    sleep_time = max(0, int(reset_time) - int(time.time()))
                else:
                    # Default to 60 seconds if no headers found
                    # Note: GitLab has known issues with missing rate limit headers:
                    # - gitlab.com often doesn't return RateLimit-* headers (issue #365728)
                    # - Self-managed GitLab stopped sending Retry-After in v13.0+ (issue #230914)
                    # - GraphQL can return 500 instead of 429 when rate limited (v15.6+)
                    # This fallback ensures we handle rate limits even when headers are absent.
                    sleep_time = 60

            self._logger.warning(
                f"Rate limit exceeded. Sleeping for {sleep_time} seconds. "
                f"Headers: {dict(response.headers)}"
            )
            await self._sleeps.sleep(sleep_time)
            return True
        return False

    async def _execute_graphql(self, query, variables=None):
        """Execute a GraphQL query with rate limit handling.

        Args:
            query (str): GraphQL query string
            variables (dict, optional): Query variables

        Returns:
            dict: GraphQL response data
        """
        session = self._get_session()
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        self._logger.debug(f"GraphQL request payload: {payload}")

        async with session.post(self.graphql_url, json=payload) as response:
            if await self._handle_rate_limit(response):
                msg = "Rate limit exceeded"
                raise GitLabRateLimitException(msg)

            try:
                response.raise_for_status()
            except aiohttp.ClientResponseError as e:
                if e.status == 401:
                    msg = f"Authentication failed: {e.message}"
                    raise GitLabUnauthorizedError(msg) from e
                elif e.status == 403:
                    msg = f"Access forbidden: {e.message}"
                    raise GitLabForbiddenError(msg) from e
                elif e.status == 404:
                    msg = f"Resource not found: {e.message}"
                    raise GitLabNotFoundError(msg) from e
                raise

            result = await response.json()

            if "errors" in result:
                errors = result["errors"]
                error_msg = f"GraphQL errors: {errors}"
                raise GitLabGraphQLException(error_msg)

            return result.get("data", {})

    async def _get_rest(self, endpoint, params=None):
        """Execute a REST GET request with rate limit handling.

        Args:
            endpoint (str): API endpoint path
            params (dict, optional): Query parameters

        Returns:
            dict: JSON response
        """
        session = self._get_session()
        url = f"{self.api_url}/{endpoint}"

        self._logger.debug(
            f"REST GET {endpoint}" + (f" params={params}" if params else "")
        )

        async with session.get(url, params=params) as response:
            if await self._handle_rate_limit(response):
                msg = "Rate limit exceeded"
                raise GitLabRateLimitException(msg)

            try:
                response.raise_for_status()
            except aiohttp.ClientResponseError as e:
                if e.status == 401:
                    msg = f"Authentication failed: {e.message}"
                    raise GitLabUnauthorizedError(msg) from e
                elif e.status == 403:
                    msg = f"Access forbidden: {e.message}"
                    raise GitLabForbiddenError(msg) from e
                elif e.status == 404:
                    msg = f"Resource not found: {e.message}"
                    raise GitLabNotFoundError(msg) from e
                raise

            return await response.json()

    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
        self._sleeps.cancel()

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def get_projects(self):
        """Fetch projects using GraphQL.

        Yields:
            GitLabProject | dict: Validated project data or raw dict on validation failure
        """
        cursor = None

        while True:
            variables = {"cursor": cursor} if cursor else {}

            try:
                result = await self._execute_graphql(PROJECTS_QUERY, variables)
            except (
                aiohttp.ClientError,
                TimeoutError,
                GitLabRateLimitException,
                GitLabGraphQLException,
            ) as e:
                self._logger.exception(f"GraphQL query failed: {e}")
                raise

            # Try validation with graceful fallback
            response = validate_with_fallback(
                ProjectsResponse, result, self._logger, context="projects query"
            )

            # Extract projects and pagination info (works for both validated and raw)
            if isinstance(response, dict):
                # Fallback: manual extraction from raw dict
                projects_data = safe_get_nested(result, "projects", "nodes", default=[])
                page_info = safe_get_nested(result, "projects", "pageInfo", default={})

                for project_data in projects_data:
                    # Yield raw dict, formatter will handle defensively
                    yield project_data

                has_next = page_info.get("hasNextPage", False) if page_info else False
                cursor = page_info.get("endCursor") if page_info else None

                if not has_next:
                    break
            else:
                # Normal path: validated Pydantic model
                for project in response.projects.nodes:
                    yield project

                if not response.projects.page_info.has_next_page:
                    break

                cursor = response.projects.page_info.end_cursor

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def get_merge_requests(self, project_path):
        """Fetch merge requests for a project using GraphQL.

        Args:
            project_path (str): Full path of the project (e.g., 'namespace/project')

        Yields:
            GitLabMergeRequest | dict: Validated merge request data or raw dict on validation failure
        """
        cursor = None

        while True:
            variables = {"projectPath": project_path}
            if cursor:
                variables["cursor"] = cursor

            try:
                result = await self._execute_graphql(MERGE_REQUESTS_QUERY, variables)
            except (
                aiohttp.ClientError,
                TimeoutError,
                GitLabRateLimitException,
                GitLabGraphQLException,
            ) as e:
                self._logger.warning(
                    f"Failed to fetch merge requests for {project_path}: {e}"
                )
                return

            # Try validation with graceful fallback
            response = validate_with_fallback(
                MergeRequestsResponse,
                result,
                self._logger,
                context=f"merge requests for {project_path}",
            )

            # Extract merge requests and pagination info
            if isinstance(response, dict):
                # Fallback: manual extraction from raw dict
                mrs_data = safe_get_nested(
                    result, "project", "mergeRequests", "nodes", default=[]
                )
                page_info = safe_get_nested(
                    result, "project", "mergeRequests", "pageInfo", default={}
                )

                if not mrs_data:
                    return

                for mr_data in mrs_data:
                    yield mr_data

                has_next = page_info.get("hasNextPage", False) if page_info else False
                cursor = page_info.get("endCursor") if page_info else None

                if not has_next:
                    break
            else:
                # Normal path: validated Pydantic model
                if not response.project or not response.project.merge_requests:
                    return

                for mr in response.project.merge_requests.nodes:
                    yield mr

                if not response.project.merge_requests.page_info.has_next_page:
                    break

                cursor = response.project.merge_requests.page_info.end_cursor

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def get_work_items_project(self, project_path, work_item_types):
        """Fetch work items for a project using Work Items API.

        Args:
            project_path (str): Full path of the project (e.g., 'namespace/project')
            work_item_types (list): List of work item type names (e.g., ['ISSUE', 'TASK'])

        Yields:
            GitLabWorkItem | dict: Validated work item data or raw dict on validation failure
        """
        cursor = None

        while True:
            variables = {"projectPath": project_path, "types": work_item_types}
            if cursor:
                variables["cursor"] = cursor

            try:
                result = await self._execute_graphql(
                    WORK_ITEMS_PROJECT_QUERY, variables
                )
            except (
                aiohttp.ClientError,
                TimeoutError,
                GitLabRateLimitException,
                GitLabGraphQLException,
            ) as e:
                self._logger.warning(
                    f"Failed to fetch work items for {project_path}: {e}"
                )
                return

            # Try validation with graceful fallback
            response = validate_with_fallback(
                WorkItemsProjectResponse,
                result,
                self._logger,
                context=f"work items for {project_path}",
            )

            # Extract work items and pagination info
            if isinstance(response, dict):
                # Fallback: manual extraction from raw dict
                work_items_data = safe_get_nested(
                    result, "project", "workItems", "nodes", default=[]
                )
                page_info = safe_get_nested(
                    result, "project", "workItems", "pageInfo", default={}
                )

                if not work_items_data:
                    return

                for work_item_data in work_items_data:
                    yield work_item_data

                has_next = page_info.get("hasNextPage", False) if page_info else False
                cursor = page_info.get("endCursor") if page_info else None

                if not has_next:
                    break
            else:
                # Normal path: validated Pydantic model
                if not response.project or not response.project.work_items:
                    return

                for work_item in response.project.work_items.nodes:
                    yield work_item

                if not response.project.work_items.page_info.has_next_page:
                    break

                cursor = response.project.work_items.page_info.end_cursor

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def get_work_items_group(self, group_path, work_item_types):
        """Fetch work items for a group using Work Items API (for Epics).

        Args:
            group_path (str): Full path of the group (e.g., 'group/subgroup')
            work_item_types (list): List of work item type names (e.g., ['EPIC'])

        Yields:
            GitLabWorkItem | dict: Validated work item data or raw dict on validation failure
        """
        cursor = None

        while True:
            variables = {"groupPath": group_path, "types": work_item_types}
            if cursor:
                variables["cursor"] = cursor

            try:
                result = await self._execute_graphql(WORK_ITEMS_GROUP_QUERY, variables)
            except (
                aiohttp.ClientError,
                TimeoutError,
                GitLabRateLimitException,
                GitLabGraphQLException,
            ) as e:
                self._logger.warning(
                    f"Failed to fetch work items for group {group_path}: {e}"
                )
                return

            # Try validation with graceful fallback
            response = validate_with_fallback(
                WorkItemsGroupResponse,
                result,
                self._logger,
                context=f"work items for group {group_path}",
            )

            # Extract work items and pagination info
            if isinstance(response, dict):
                # Fallback: manual extraction from raw dict
                work_items_data = safe_get_nested(
                    result, "group", "workItems", "nodes", default=[]
                )
                page_info = safe_get_nested(
                    result, "group", "workItems", "pageInfo", default={}
                )

                if not work_items_data:
                    return

                for work_item_data in work_items_data:
                    yield work_item_data

                has_next = page_info.get("hasNextPage", False) if page_info else False
                cursor = page_info.get("endCursor") if page_info else None

                if not has_next:
                    break
            else:
                # Normal path: validated Pydantic model
                if not response.group or not response.group.work_items:
                    return

                for work_item in response.group.work_items.nodes:
                    yield work_item

                if not response.group.work_items.page_info.has_next_page:
                    break

                cursor = response.group.work_items.page_info.end_cursor

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def get_releases(self, project_path):
        """Fetch releases for a project using GraphQL.

        Args:
            project_path (str): Full path of the project (e.g., 'namespace/project')

        Yields:
            GitLabRelease | dict: Validated release data or raw dict on validation failure
        """
        cursor = None

        while True:
            variables = {"projectPath": project_path}
            if cursor:
                variables["cursor"] = cursor

            try:
                result = await self._execute_graphql(RELEASES_QUERY, variables)
            except (
                aiohttp.ClientError,
                TimeoutError,
                GitLabRateLimitException,
                GitLabGraphQLException,
            ) as e:
                self._logger.warning(
                    f"Failed to fetch releases for {project_path}: {e}"
                )
                return

            # Try validation with graceful fallback
            response = validate_with_fallback(
                ReleasesResponse,
                result,
                self._logger,
                context=f"releases for {project_path}",
            )

            # Extract releases and pagination info
            if isinstance(response, dict):
                # Fallback: manual extraction from raw dict
                releases_data = safe_get_nested(
                    result, "project", "releases", "nodes", default=[]
                )
                page_info = safe_get_nested(
                    result, "project", "releases", "pageInfo", default={}
                )

                if not releases_data:
                    return

                for release_data in releases_data:
                    yield release_data

                has_next = page_info.get("hasNextPage", False) if page_info else False
                cursor = page_info.get("endCursor") if page_info else None

                if not has_next:
                    break
            else:
                # Normal path: validated Pydantic model
                if not response.project or not response.project.releases:
                    return

                for release in response.project.releases.nodes:
                    yield release

                if not response.project.releases.page_info.has_next_page:
                    break

                cursor = response.project.releases.page_info.end_cursor

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def fetch_remaining_field(
        self, project_path, iid, field_type, issuable_type, cursor
    ):
        """Fetch remaining items for a paginated field.

        Args:
            project_path (str): Full path of the project
            iid (str): Issue or MR internal ID
            field_type (str): Type of field ('assignees', 'labels', 'discussions', 'reviewers', 'approvedBy')
            issuable_type (str): Type of issuable ('issue' or 'mergeRequest')
            cursor (str): Pagination cursor

        Yields:
            dict: Field items
        """
        query_map = {
            "assignees": ASSIGNEES_QUERY,
            "labels": LABELS_QUERY,
            "discussions": DISCUSSIONS_QUERY,
            "reviewers": REVIEWERS_QUERY,
            "approvedBy": APPROVEDBY_QUERY,
        }

        query_template = query_map.get(field_type)
        if not query_template:
            self._logger.warning(f"Unknown field type: {field_type}")
            return

        query = query_template.format(issuable_type=issuable_type)

        while cursor:
            variables = {"projectPath": project_path, "iid": str(iid), "cursor": cursor}

            try:
                result = await self._execute_graphql(query, variables)
            except (aiohttp.ClientError, TimeoutError) as e:
                self._logger.warning(
                    f"Failed to fetch remaining {field_type} for {issuable_type} {iid}: {e}"
                )
                raise

            project_data = result.get("project")
            if not project_data:
                return

            issuable_data = project_data.get(issuable_type)
            if not issuable_data:
                return

            field_data = issuable_data.get(field_type, {})
            items = field_data.get("nodes", [])

            for item in items:
                yield item

            page_info = field_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def fetch_remaining_notes(
        self, project_path, iid, discussion_id, issuable_type, cursor
    ):
        """Fetch remaining notes for a specific discussion.

        Args:
            project_path (str): Full path of the project
            iid (str): Issue or MR internal ID
            discussion_id (str): Discussion ID
            issuable_type (str): 'issue' or 'mergeRequest'
            cursor (str): Pagination cursor

        Yields:
            dict: Note data
        """

        query = NOTES_QUERY.format(issuable_type=issuable_type)

        while cursor:
            variables = {
                "projectPath": project_path,
                "iid": str(iid),
                "discussionId": discussion_id,
                "cursor": cursor,
            }

            try:
                result = await self._execute_graphql(query, variables)
            except (aiohttp.ClientError, TimeoutError) as e:
                self._logger.warning(
                    f"Failed to fetch remaining notes for discussion {discussion_id}: {e}"
                )
                raise

            project_data = result.get("project")
            if not project_data:
                return

            issuable_data = project_data.get(issuable_type)
            if not issuable_data:
                return

            discussions_data = issuable_data.get("discussions", {})
            discussion_nodes = discussions_data.get("nodes", [])

            if not discussion_nodes:
                return

            discussion = discussion_nodes[0]
            notes_data = discussion.get("notes", {})
            notes = notes_data.get("nodes", [])

            for note in notes:
                yield note

            page_info = notes_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def fetch_remaining_work_item_assignees(
        self, project_path: str, iid: int, work_item_type: str, cursor: str
    ) -> AsyncGenerator[GitLabUser, None]:
        """Fetch remaining assignees for a work item.

        Args:
            project_path: Full path of the project
            iid: Work item internal ID
            work_item_type: Work item type (e.g., 'ISSUE', 'TASK')
            cursor: Pagination cursor

        Yields:
            GitLabUser: Validated assignee model
        """
        while cursor:
            variables = {
                "projectPath": project_path,
                "iid": str(iid),
                "workItemType": work_item_type,
                "cursor": cursor,
            }

            try:
                result = await self._execute_graphql(
                    WORK_ITEM_ASSIGNEES_QUERY, variables
                )
            except (aiohttp.ClientError, TimeoutError) as e:
                self._logger.warning(
                    f"Failed to fetch remaining assignees for work item {iid}: {e}"
                )
                raise

            project_data = result.get("project")
            if not project_data:
                return

            work_items = project_data.get("workItems", {}).get("nodes", [])
            if not work_items:
                return

            work_item = work_items[0]
            widgets = work_item.get("widgets", [])

            assignees_data = None
            for widget in widgets:
                if widget.get("__typename") == "WorkItemWidgetAssignees":
                    assignees_data = widget.get("assignees", {})
                    break

            if not assignees_data:
                return

            assignees = assignees_data.get("nodes", [])
            for assignee in assignees:
                yield GitLabUser.model_validate(assignee)

            page_info = assignees_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def fetch_remaining_work_item_labels(
        self, project_path: str, iid: int, work_item_type: str, cursor: str
    ) -> AsyncGenerator[GitLabLabel, None]:
        """Fetch remaining labels for a work item.

        Args:
            project_path: Full path of the project
            iid: Work item internal ID
            work_item_type: Work item type (e.g., 'ISSUE', 'TASK')
            cursor: Pagination cursor

        Yields:
            GitLabLabel: Validated label model
        """
        while cursor:
            variables = {
                "projectPath": project_path,
                "iid": str(iid),
                "workItemType": work_item_type,
                "cursor": cursor,
            }

            try:
                result = await self._execute_graphql(WORK_ITEM_LABELS_QUERY, variables)
            except (aiohttp.ClientError, TimeoutError) as e:
                self._logger.warning(
                    f"Failed to fetch remaining labels for work item {iid}: {e}"
                )
                raise

            project_data = result.get("project")
            if not project_data:
                return

            work_items = project_data.get("workItems", {}).get("nodes", [])
            if not work_items:
                return

            work_item = work_items[0]
            widgets = work_item.get("widgets", [])

            labels_data = None
            for widget in widgets:
                if widget.get("__typename") == "WorkItemWidgetLabels":
                    labels_data = widget.get("labels", {})
                    break

            if not labels_data:
                return

            labels = labels_data.get("nodes", [])
            for label in labels:
                yield GitLabLabel.model_validate(label)

            page_info = labels_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def fetch_remaining_work_item_discussions(
        self, project_path: str, iid: int, work_item_type: str, cursor: str
    ) -> AsyncGenerator[GitLabDiscussion, None]:
        """Fetch remaining discussions for a work item.

        Args:
            project_path: Full path of the project
            iid: Work item internal ID
            work_item_type: Work item type (e.g., 'ISSUE', 'TASK')
            cursor: Pagination cursor

        Yields:
            GitLabDiscussion: Validated discussion model
        """
        while cursor:
            variables = {
                "projectPath": project_path,
                "iid": str(iid),
                "workItemType": work_item_type,
                "cursor": cursor,
            }

            try:
                result = await self._execute_graphql(
                    WORK_ITEM_DISCUSSIONS_QUERY, variables
                )
            except (aiohttp.ClientError, TimeoutError) as e:
                self._logger.warning(
                    f"Failed to fetch remaining discussions for work item {iid}: {e}"
                )
                raise

            project_data = result.get("project")
            if not project_data:
                return

            work_items = project_data.get("workItems", {}).get("nodes", [])
            if not work_items:
                return

            work_item = work_items[0]
            widgets = work_item.get("widgets", [])

            discussions_data = None
            for widget in widgets:
                if widget.get("__typename") == "WorkItemWidgetNotes":
                    discussions_data = widget.get("discussions", {})
                    break

            if not discussions_data:
                return

            discussions = discussions_data.get("nodes", [])
            for discussion in discussions:
                yield GitLabDiscussion.model_validate(discussion)

            page_info = discussions_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def fetch_remaining_work_item_group_discussions(
        self, group_path: str, iid: int, work_item_type: str, cursor: str
    ) -> AsyncGenerator[GitLabDiscussion, None]:
        """Fetch remaining discussions for a group-level work item (e.g., Epic).

        Args:
            group_path: Full path of the group
            iid: Work item internal ID
            work_item_type: Work item type (e.g., 'EPIC')
            cursor: Pagination cursor

        Yields:
            GitLabDiscussion: Validated discussion model
        """
        while cursor:
            variables = {
                "groupPath": group_path,
                "iid": str(iid),
                "workItemType": work_item_type,
                "cursor": cursor,
            }

            try:
                result = await self._execute_graphql(
                    WORK_ITEM_GROUP_DISCUSSIONS_QUERY, variables
                )
            except (aiohttp.ClientError, TimeoutError) as e:
                self._logger.warning(
                    f"Failed to fetch remaining discussions for group work item {iid}: {e}"
                )
                raise

            group_data = result.get("group")
            if not group_data:
                return

            work_items = group_data.get("workItems", {}).get("nodes", [])
            if not work_items:
                return

            work_item = work_items[0]
            widgets = work_item.get("widgets", [])

            discussions_data = None
            for widget in widgets:
                if widget.get("__typename") == "WorkItemWidgetNotes":
                    discussions_data = widget.get("discussions", {})
                    break

            if not discussions_data:
                return

            discussions = discussions_data.get("nodes", [])
            for discussion in discussions:
                yield GitLabDiscussion.model_validate(discussion)

            page_info = discussions_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def fetch_remaining_work_item_assignees_group(
        self, group_path: str, iid: int, work_item_type: str, cursor: str
    ) -> AsyncGenerator[GitLabUser, None]:
        """Fetch remaining assignees for a group-level work item (Epic).

        Args:
            group_path: Full path of the group
            iid: Work item internal ID
            work_item_type: Work item type (e.g., 'EPIC')
            cursor: Pagination cursor

        Yields:
            GitLabUser: Validated assignee model
        """
        while cursor:
            variables = {
                "groupPath": group_path,
                "iid": str(iid),
                "workItemType": work_item_type,
                "cursor": cursor,
            }

            try:
                result = await self._execute_graphql(
                    WORK_ITEM_GROUP_ASSIGNEES_QUERY, variables
                )
            except (aiohttp.ClientError, TimeoutError) as e:
                self._logger.warning(
                    f"Failed to fetch remaining assignees for group work item {iid}: {e}"
                )
                raise

            group_data = result.get("group")
            if not group_data:
                return

            work_items = group_data.get("workItems", {}).get("nodes", [])
            if not work_items:
                return

            work_item = work_items[0]
            widgets = work_item.get("widgets", [])

            assignees_data = None
            for widget in widgets:
                if widget.get("__typename") == "WorkItemWidgetAssignees":
                    assignees_data = widget.get("assignees", {})
                    break

            if not assignees_data:
                return

            assignees = assignees_data.get("nodes", [])
            for assignee in assignees:
                yield GitLabUser.model_validate(assignee)

            page_info = assignees_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def fetch_remaining_work_item_labels_group(
        self, group_path: str, iid: int, work_item_type: str, cursor: str
    ) -> AsyncGenerator[GitLabLabel, None]:
        """Fetch remaining labels for a group-level work item (Epic).

        Args:
            group_path: Full path of the group
            iid: Work item internal ID
            work_item_type: Work item type (e.g., 'EPIC')
            cursor: Pagination cursor

        Yields:
            GitLabLabel: Validated label model
        """
        while cursor:
            variables = {
                "groupPath": group_path,
                "iid": str(iid),
                "workItemType": work_item_type,
                "cursor": cursor,
            }

            try:
                result = await self._execute_graphql(
                    WORK_ITEM_GROUP_LABELS_QUERY, variables
                )
            except (aiohttp.ClientError, TimeoutError) as e:
                self._logger.warning(
                    f"Failed to fetch remaining labels for group work item {iid}: {e}"
                )
                raise

            group_data = result.get("group")
            if not group_data:
                return

            work_items = group_data.get("workItems", {}).get("nodes", [])
            if not work_items:
                return

            work_item = work_items[0]
            widgets = work_item.get("widgets", [])

            labels_data = None
            for widget in widgets:
                if widget.get("__typename") == "WorkItemWidgetLabels":
                    labels_data = widget.get("labels", {})
                    break

            if not labels_data:
                return

            labels = labels_data.get("nodes", [])
            for label in labels:
                yield GitLabLabel.model_validate(label)

            page_info = labels_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[
            GitLabUnauthorizedError,
            GitLabForbiddenError,
            GitLabNotFoundError,
        ],
    )
    async def get_file_content(self, project_id, file_path, ref=None):
        """Get file content from repository using REST API.

        Args:
            project_id (int): Project ID
            file_path (str): Path to file
            ref (str, optional): Branch/tag name

        Returns:
            dict: File data with content (base64 encoded)
        """
        try:
            # URL encode the file path
            encoded_path = quote(file_path, safe="")
            endpoint = f"projects/{project_id}/repository/files/{encoded_path}"
            params = {"ref": ref} if ref else {}

            file_data = await self._get_rest(endpoint, params=params)
            return file_data
        except (aiohttp.ClientError, TimeoutError) as e:
            self._logger.warning(
                f"Failed to fetch file {file_path} from project {project_id}: {e}"
            )
            raise

    async def ping(self) -> None:
        """Test the connection to GitLab."""
        try:
            await self._get_rest("user")
            self._logger.debug("Successfully authenticated with GitLab")
        except Exception as e:
            msg = f"Failed to connect to GitLab: {e}"
            raise ConfigurableFieldValueError(msg) from e
