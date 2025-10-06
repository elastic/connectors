#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""GitLab source module responsible to fetch documents from GitLab Cloud."""

from functools import partial
from typing import Any
from urllib.parse import quote

import aiohttp
from pydantic import BaseModel, Field

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    decode_base64_value,
    retryable,
)

GITLAB_CLOUD_URL = "https://gitlab.com"
RETRIES = 3
RETRY_INTERVAL = 2
DEFAULT_PAGE_SIZE = 100

# Pagination sizes for GraphQL queries
NODE_SIZE = 100  # For projects, issues, merge requests
NESTED_FIELD_SIZE = 100  # For assignees, labels, discussions

SUPPORTED_EXTENSION = [".md", ".rst", ".txt"]


# Pydantic models for type-safe parsing
class PageInfo(BaseModel):
    """GraphQL pagination info."""

    has_next_page: bool = Field(alias="hasNextPage")
    end_cursor: str | None = Field(alias="endCursor", default=None)

    class Config:
        populate_by_name = True


class GitLabUser(BaseModel):
    """GitLab user model."""

    username: str
    name: str | None = None


class GitLabLabel(BaseModel):
    """GitLab label model."""

    title: str


class GitLabNote(BaseModel):
    """GitLab note (comment) model."""

    id: str
    body: str
    created_at: str = Field(alias="createdAt")
    updated_at: str = Field(alias="updatedAt")
    author: GitLabUser | None = None

    class Config:
        populate_by_name = True


class GitLabDiscussion(BaseModel):
    """GitLab discussion model."""

    id: str
    notes: dict[str, Any]  # Contains nodes and pageInfo

    class Config:
        populate_by_name = True


class GitLabIssue(BaseModel):
    """GitLab issue model."""

    id: str | None = None
    iid: int
    title: str
    description: str | None = None
    state: str
    web_url: str = Field(alias="webUrl")
    created_at: str = Field(alias="createdAt")
    updated_at: str = Field(alias="updatedAt")
    closed_at: str | None = Field(alias="closedAt", default=None)
    author: GitLabUser | None = None
    assignees: dict[str, Any]  # Contains nodes and pageInfo
    labels: dict[str, Any]  # Contains nodes and pageInfo
    discussions: dict[str, Any]  # Contains nodes and pageInfo

    class Config:
        populate_by_name = True


class GitLabMergeRequest(BaseModel):
    """GitLab merge request model."""

    id: str | None = None
    iid: int
    title: str
    description: str | None = None
    state: str
    web_url: str = Field(alias="webUrl")
    created_at: str = Field(alias="createdAt")
    updated_at: str = Field(alias="updatedAt")
    merged_at: str | None = Field(alias="mergedAt", default=None)
    closed_at: str | None = Field(alias="closedAt", default=None)
    source_branch: str = Field(alias="sourceBranch")
    target_branch: str = Field(alias="targetBranch")
    author: GitLabUser | None = None
    assignees: dict[str, Any]  # Contains nodes and pageInfo
    reviewers: dict[str, Any]  # Contains nodes and pageInfo
    labels: dict[str, Any]  # Contains nodes and pageInfo
    discussions: dict[str, Any]  # Contains nodes and pageInfo
    approved_by: dict[str, Any] = Field(alias="approvedBy")  # Contains nodes and pageInfo
    merged_by: GitLabUser | None = Field(alias="mergedBy", default=None)

    class Config:
        populate_by_name = True


class GitLabProject(BaseModel):
    """GitLab project model."""

    id: str
    name: str
    path: str
    full_path: str = Field(alias="fullPath")
    description: str | None = None
    visibility: str
    star_count: int = Field(alias="starCount")
    forks_count: int = Field(alias="forksCount")
    created_at: str = Field(alias="createdAt")
    last_activity_at: str | None = Field(alias="lastActivityAt", default=None)
    archived: bool | None = None
    web_url: str = Field(alias="webUrl")
    repository: dict[str, Any] | None = None  # Contains rootRef

    @property
    def default_branch(self) -> str | None:
        """Extract default branch from repository."""
        if self.repository:
            return self.repository.get("rootRef")
        return None

    class Config:
        populate_by_name = True


# GraphQL query to fetch projects (simplified to avoid complexity limits)
PROJECTS_QUERY = f"""
query($cursor: String) {{
  projects(membership: true, first: {NODE_SIZE}, after: $cursor) {{
    pageInfo {{
      hasNextPage
      endCursor
    }}
    nodes {{
      id
      name
      path
      fullPath
      description
      visibility
      starCount
      forksCount
      createdAt
      lastActivityAt
      archived
      webUrl
      repository {{
        rootRef
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch issues for a project
ISSUES_QUERY = f"""
query($projectPath: ID!, $cursor: String) {{
  project(fullPath: $projectPath) {{
    issues(first: {NODE_SIZE}, after: $cursor) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        iid
        title
        description
        state
        createdAt
        updatedAt
        closedAt
        webUrl
        author {{
          username
          name
        }}
        assignees(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            username
            name
          }}
        }}
        labels(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            title
          }}
        }}
        userNotesCount
        discussions(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            id
            notes(first: {NESTED_FIELD_SIZE}) {{
              pageInfo {{
                hasNextPage
                endCursor
              }}
              nodes {{
                id
                body
                createdAt
                updatedAt
                system
                position {{
                  newLine
                  oldLine
                  newPath
                  oldPath
                  positionType
                }}
                author {{
                  username
                  name
                }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch merge requests for a project
MERGE_REQUESTS_QUERY = f"""
query($projectPath: ID!, $cursor: String) {{
  project(fullPath: $projectPath) {{
    mergeRequests(first: {NODE_SIZE}, after: $cursor) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        iid
        title
        description
        state
        createdAt
        updatedAt
        mergedAt
        closedAt
        webUrl
        sourceBranch
        targetBranch
        author {{
          username
          name
        }}
        assignees(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            username
            name
          }}
        }}
        reviewers(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            username
            name
          }}
        }}
        approvedBy(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            username
            name
          }}
        }}
        mergeUser {{
          username
          name
        }}
        labels(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            title
          }}
        }}
        userNotesCount
        discussions(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            id
            notes(first: {NESTED_FIELD_SIZE}) {{
              pageInfo {{
                hasNextPage
                endCursor
              }}
              nodes {{
                id
                body
                createdAt
                updatedAt
                system
                position {{
                  newLine
                  oldLine
                  newPath
                  oldPath
                  positionType
                }}
                author {{
                  username
                  name
                }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch remaining assignees for an issue or MR
ASSIGNEES_QUERY = f"""
query($projectPath: ID!, $iid: String!, $cursor: String) {{{{
  project(fullPath: $projectPath) {{{{
    {{issuable_type}}(iid: $iid) {{{{
      assignees(first: {NESTED_FIELD_SIZE}, after: $cursor) {{{{
        pageInfo {{{{
          hasNextPage
          endCursor
        }}}}
        nodes {{{{
          username
          name
        }}}}
      }}}}
    }}}}
  }}}}
}}}}
"""

# GraphQL query to fetch remaining labels for an issue or MR
LABELS_QUERY = f"""
query($projectPath: ID!, $iid: String!, $cursor: String) {{{{
  project(fullPath: $projectPath) {{{{
    {{issuable_type}}(iid: $iid) {{{{
      labels(first: {NESTED_FIELD_SIZE}, after: $cursor) {{{{
        pageInfo {{{{
          hasNextPage
          endCursor
        }}}}
        nodes {{{{
          title
        }}}}
      }}}}
    }}}}
  }}}}
}}}}
"""

# GraphQL query to fetch remaining discussions for an issue or MR
DISCUSSIONS_QUERY = f"""
query($projectPath: ID!, $iid: String!, $cursor: String) {{{{
  project(fullPath: $projectPath) {{{{
    {{issuable_type}}(iid: $iid) {{{{
      discussions(first: {NESTED_FIELD_SIZE}, after: $cursor) {{{{
        pageInfo {{{{
          hasNextPage
          endCursor
        }}}}
        nodes {{{{
          notes {{{{
            nodes {{{{
              id
              body
              createdAt
              updatedAt
              system
              position {{{{
                newLine
                oldLine
                newPath
                oldPath
                positionType
              }}}}
              author {{{{
                username
                name
              }}}}
            }}}}
          }}}}
        }}}}
      }}}}
    }}}}
  }}}}
}}}}
"""

# GraphQL query to fetch remaining reviewers for a merge request
REVIEWERS_QUERY = f"""
query($projectPath: ID!, $iid: String!, $cursor: String) {{{{
  project(fullPath: $projectPath) {{{{
    {{issuable_type}}(iid: $iid) {{{{
      reviewers(first: {NESTED_FIELD_SIZE}, after: $cursor) {{{{
        pageInfo {{{{
          hasNextPage
          endCursor
        }}}}
        nodes {{{{
          username
          name
        }}}}
      }}}}
    }}}}
  }}}}
}}}}
"""

# GraphQL query to fetch remaining approvers for a merge request
APPROVEDBY_QUERY = f"""
query($projectPath: ID!, $iid: String!, $cursor: String) {{{{
  project(fullPath: $projectPath) {{{{
    {{issuable_type}}(iid: $iid) {{{{
      approvedBy(first: {NESTED_FIELD_SIZE}, after: $cursor) {{{{
        pageInfo {{{{
          hasNextPage
          endCursor
        }}}}
        nodes {{{{
          username
          name
        }}}}
      }}}}
    }}}}
  }}}}
}}}}
"""

# GraphQL query to fetch remaining notes for a specific discussion
NOTES_QUERY = f"""
query($projectPath: ID!, $iid: String!, $discussionId: ID!, $cursor: String) {{{{
  project(fullPath: $projectPath) {{{{
    {{issuable_type}}(iid: $iid) {{{{
      discussions(filter: {{discussionIds: [$discussionId]}}) {{{{
        nodes {{{{
          notes(first: {NESTED_FIELD_SIZE}, after: $cursor) {{{{
            pageInfo {{{{
              hasNextPage
              endCursor
            }}}}
            nodes {{{{
              id
              body
              createdAt
              updatedAt
              system
              position {{{{
                newLine
                oldLine
                newPath
                oldPath
                positionType
              }}}}
              author {{{{
                username
                name
              }}}}
            }}}}
          }}}}
        }}}}
      }}}}
    }}}}
  }}}}
}}}}
"""


class GitLabClient:
    """Client for interacting with GitLab Cloud API using aiohttp."""

    def __init__(self, token):
        self._logger = logger
        self.token = token
        self.api_url = f"{GITLAB_CLOUD_URL}/api/v4"
        self.graphql_url = f"{GITLAB_CLOUD_URL}/api/graphql"
        self._sleeps = CancellableSleeps()
        self._session = None

    def set_logger(self, logger_):
        self._logger = logger_

    def _get_session(self):
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=aiohttp.ClientTimeout(total=300),
            )
        return self._session

    async def _handle_rate_limit(self, response):
        """Handle rate limiting by sleeping until reset time.

        Args:
            response: aiohttp response object
        """
        if response.status == 429:
            # Check for Retry-After header (in seconds)
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                sleep_time = int(retry_after)
            else:
                # Check for RateLimit-Reset header (Unix timestamp)
                reset_time = response.headers.get("RateLimit-Reset")
                if reset_time:
                    import time

                    sleep_time = max(0, int(reset_time) - int(time.time()))
                else:
                    # Default to 60 seconds if no headers found
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

        async with session.post(self.graphql_url, json=payload) as response:
            # Handle rate limiting - sleeps and raises exception for @retryable to catch
            if await self._handle_rate_limit(response):
                msg = "Rate limit exceeded"
                raise Exception(msg)

            response.raise_for_status()
            result = await response.json()

            # Check for GraphQL errors
            if "errors" in result:
                errors = result["errors"]
                # Check if it's a rate limit error in GraphQL response
                for error in errors:
                    error_msg = str(error.get("message", "")).lower()
                    if "rate" in error_msg and "limit" in error_msg:
                        self._logger.warning(f"GraphQL rate limit error: {error}")
                        await self._sleeps.sleep(60)
                        msg = "GraphQL rate limit exceeded"
                        raise Exception(msg)

                # If not rate limit, raise the error
                error_msg = f"GraphQL errors: {errors}"
                raise Exception(error_msg)

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

        async with session.get(url, params=params) as response:
            # Handle rate limiting - sleeps and raises exception for @retryable to catch
            if await self._handle_rate_limit(response):
                msg = "Rate limit exceeded"
                raise Exception(msg)

            response.raise_for_status()
            return await response.json()

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
        self._sleeps.cancel()

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_projects(self):
        """Fetch projects using GraphQL.

        Yields:
            GitLabProject: Validated project data
        """
        cursor = None

        while True:
            variables = {"cursor": cursor} if cursor else {}

            try:
                result = await self._execute_graphql(PROJECTS_QUERY, variables)
            except Exception as e:
                self._logger.exception(f"GraphQL query failed: {e}")
                raise

            projects_data = result.get("projects", {})
            projects = projects_data.get("nodes", [])

            for project_data in projects:
                project = GitLabProject.model_validate(project_data)
                yield project

            # Check pagination
            page_info_data = projects_data.get("pageInfo", {})
            page_info = PageInfo.model_validate(page_info_data)
            if not page_info.has_next_page:
                break

            cursor = page_info.end_cursor

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_issues(self, project_path):
        """Fetch issues for a project using GraphQL.

        Args:
            project_path (str): Full path of the project (e.g., 'namespace/project')

        Yields:
            GitLabIssue: Validated issue data
        """
        cursor = None

        while True:
            variables = {"projectPath": project_path}
            if cursor:
                variables["cursor"] = cursor

            try:
                result = await self._execute_graphql(ISSUES_QUERY, variables)
            except Exception as e:
                self._logger.warning(f"Failed to fetch issues for {project_path}: {e}")
                return

            project_data = result.get("project")
            if not project_data:
                return

            issues_data = project_data.get("issues", {})
            issues = issues_data.get("nodes", [])

            for issue_data in issues:
                issue = GitLabIssue.model_validate(issue_data)
                yield issue

            # Check pagination
            page_info_data = issues_data.get("pageInfo", {})
            page_info = PageInfo.model_validate(page_info_data)
            if not page_info.has_next_page:
                break

            cursor = page_info.end_cursor

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_merge_requests(self, project_path):
        """Fetch merge requests for a project using GraphQL.

        Args:
            project_path (str): Full path of the project (e.g., 'namespace/project')

        Yields:
            GitLabMergeRequest: Validated merge request data
        """
        cursor = None

        while True:
            variables = {"projectPath": project_path}
            if cursor:
                variables["cursor"] = cursor

            try:
                result = await self._execute_graphql(MERGE_REQUESTS_QUERY, variables)
            except Exception as e:
                self._logger.warning(
                    f"Failed to fetch merge requests for {project_path}: {e}"
                )
                return

            project_data = result.get("project")
            if not project_data:
                return

            mrs_data = project_data.get("mergeRequests", {})
            mrs = mrs_data.get("nodes", [])

            for mr_data in mrs:
                mr = GitLabMergeRequest.model_validate(mr_data)
                yield mr

            # Check pagination
            page_info_data = mrs_data.get("pageInfo", {})
            page_info = PageInfo.model_validate(page_info_data)
            if not page_info.has_next_page:
                break

            cursor = page_info.end_cursor

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
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

        # Format the query with the issuable type
        query = query_template.format(issuable_type=issuable_type)

        while cursor:
            variables = {"projectPath": project_path, "iid": str(iid), "cursor": cursor}

            try:
                result = await self._execute_graphql(query, variables)
            except Exception as e:
                self._logger.warning(
                    f"Failed to fetch remaining {field_type} for {issuable_type} {iid}: {e}"
                )
                return

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

            # Check pagination
            page_info = field_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
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
        # Format the query with the issuable type
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
            except Exception as e:
                self._logger.warning(
                    f"Failed to fetch remaining notes for discussion {discussion_id}: {e}"
                )
                return

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

            # Should only be one discussion (filtered by ID)
            discussion = discussion_nodes[0]
            notes_data = discussion.get("notes", {})
            notes = notes_data.get("nodes", [])

            for note in notes:
                yield note

            # Check pagination
            page_info = notes_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

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
        except Exception as e:
            self._logger.warning(
                f"Failed to fetch file {file_path} from project {project_id}: {e}"
            )
            return None

    async def ping(self):
        """Test the connection to GitLab."""
        try:
            await self._get_rest("user")
            self._logger.debug("Successfully authenticated with GitLab")
        except Exception as e:
            msg = f"Failed to connect to GitLab: {e}"
            raise ConfigurableFieldValueError(msg) from e


class GitLabDataSource(BaseDataSource):
    """GitLab Cloud Data Source."""

    name = "GitLab"
    service_type = "gitlab"
    advanced_rules_enabled = False
    dls_enabled = False
    incremental_sync_enabled = False

    def __init__(self, configuration):
        """Setup the connection to the GitLab instance.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.gitlab_client = GitLabClient(
            token=self.configuration["token"],
        )
        self.configured_projects = self.configuration["projects"]

    def _set_internal_logger(self):
        self.gitlab_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for GitLab.

        Returns:
            dict: Default configuration.
        """
        return {
            "token": {
                "label": "Personal Access Token",
                "order": 1,
                "sensitive": True,
                "type": "str",
                "tooltip": "GitLab Personal Access Token with api, read_api, and read_repository scopes.",
            },
            "projects": {
                "display": "textarea",
                "label": "List of projects",
                "order": 2,
                "tooltip": "List of project paths (e.g., 'group/project'). Use '*' to sync all accessible projects.",
                "type": "list",
                "value": [],
            },
        }

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields.
        Also validates if the configured projects are accessible.
        """
        await super().validate_config()
        await self._remote_validation()

    async def _remote_validation(self):
        """Validate GitLab connection and project accessibility.

        Raises:
            ConfigurableFieldValueError: If validation fails.
        """
        # Test authentication
        try:
            await self.gitlab_client.ping()
        except Exception as e:
            msg = f"Failed to authenticate with GitLab: {e}"
            raise ConfigurableFieldValueError(msg) from e

        # If specific projects are configured, validate they exist and are accessible
        if self.configured_projects and self.configured_projects != ["*"]:
            await self._validate_configured_projects()

    async def _validate_configured_projects(self):
        """Validate that configured projects exist and are accessible (runs in thread pool).

        Raises:
            ConfigurableFieldValueError: If any project is invalid.
        """

        invalid_projects = []

        for project_path in self.configured_projects:
            if not project_path or project_path.strip() == "":
                continue

            try:
                # URL encode the project path for the API
                encoded_path = quote(project_path, safe="")
                await self.gitlab_client._get_rest(f"projects/{encoded_path}")
                self._logger.debug(f"✓ Project '{project_path}' is accessible")
            except Exception as e:
                self._logger.warning(
                    f"✗ Project '{project_path}' is not accessible: {e}"
                )
                invalid_projects.append(project_path)

        if invalid_projects:
            msg = f"The following projects are not accessible: {', '.join(invalid_projects)}. Please check the project paths and ensure your token has access."
            raise ConfigurableFieldValueError(msg)

    async def ping(self):
        """Test the connection to GitLab."""
        try:
            await self.gitlab_client.ping()
            self._logger.info("Successfully connected to GitLab.")
        except Exception:
            self._logger.exception("Error while connecting to GitLab.")
            raise

    async def close(self):
        """Close the GitLab client connection."""
        await self.gitlab_client.close()

    def _should_sync_project(self, project_path):
        """Check if a project should be synced based on configuration.

        Args:
            project_path (str): Full path of the project (e.g., 'group/project')

        Returns:
            bool: True if project should be synced, False otherwise
        """
        # If no projects configured or wildcard, sync all
        if not self.configured_projects or "*" in self.configured_projects:
            return True

        # Check if project is in the configured list
        return project_path in self.configured_projects

    async def _fetch_remaining_issue_fields(
        self, issuable, project_path, issuable_type
    ):
        """Fetch remaining items for paginated fields in an issue or MR.

        Args:
            issuable (GitLabIssue | GitLabMergeRequest): Issue or MR Pydantic model
            project_path (str): Full path of the project
            issuable_type (str): 'issue' or 'mergeRequest'
        """
        # Check and fetch remaining assignees
        assignees_page_info = issuable.assignees.get("pageInfo", {})
        if assignees_page_info.get("hasNextPage"):
            cursor = assignees_page_info.get("endCursor")
            async for assignee in self.gitlab_client.fetch_remaining_field(
                project_path, issuable.iid, "assignees", issuable_type, cursor
            ):
                issuable.assignees["nodes"].append(assignee)

        # Check and fetch remaining labels
        labels_page_info = issuable.labels.get("pageInfo", {})
        if labels_page_info.get("hasNextPage"):
            cursor = labels_page_info.get("endCursor")
            async for label in self.gitlab_client.fetch_remaining_field(
                project_path, issuable.iid, "labels", issuable_type, cursor
            ):
                issuable.labels["nodes"].append(label)

        # Check and fetch remaining discussions
        discussions_page_info = issuable.discussions.get("pageInfo", {})
        if discussions_page_info.get("hasNextPage"):
            cursor = discussions_page_info.get("endCursor")
            async for discussion in self.gitlab_client.fetch_remaining_field(
                project_path, issuable.iid, "discussions", issuable_type, cursor
            ):
                issuable.discussions["nodes"].append(discussion)

        # Check and fetch remaining reviewers (MRs only)
        if issuable_type == "mergeRequest":
            reviewers_page_info = issuable.reviewers.get("pageInfo", {})
            if reviewers_page_info.get("hasNextPage"):
                cursor = reviewers_page_info.get("endCursor")
                async for reviewer in self.gitlab_client.fetch_remaining_field(
                    project_path, issuable.iid, "reviewers", issuable_type, cursor
                ):
                    issuable.reviewers["nodes"].append(reviewer)

            # Check and fetch remaining approvedBy (MRs only)
            approvedby_page_info = issuable.approved_by.get("pageInfo", {})
            if approvedby_page_info.get("hasNextPage"):
                cursor = approvedby_page_info.get("endCursor")
                async for approver in self.gitlab_client.fetch_remaining_field(
                    project_path, issuable.iid, "approvedBy", issuable_type, cursor
                ):
                    issuable.approved_by["nodes"].append(approver)

    async def get_docs(self, filtering=None):
        """Main method to fetch documents from GitLab.

        Args:
            filtering (Filtering, optional): Filtering rules. Defaults to None.

        Yields:
            tuple: (document dict, download function or None)
        """
        # Fetch projects via GraphQL
        async for project in self.gitlab_client.get_projects():
            # Extract project ID (GraphQL returns global ID, need numeric ID)
            # GitLab GraphQL IDs are like "gid://gitlab/Project/123"
            project_id = int(project.id.split("/")[-1]) if "/" in project.id else None

            if not project_id:
                self._logger.warning(f"Could not extract project ID from {project.id}")
                continue

            # Filter projects based on configuration
            if not self._should_sync_project(project.full_path):
                self._logger.debug(
                    f"Skipping project '{project.full_path}' (not in configured projects)"
                )
                continue

            # Yield project document
            project_doc = self._format_project_doc(project)
            yield project_doc, None

            # Fetch and process issues for this project
            async for issue in self.gitlab_client.get_issues(project.full_path):
                # Fetch remaining fields if they have more pages
                await self._fetch_remaining_issue_fields(
                    issue, project.full_path, "issue"
                )

                issue_doc = self._format_issue_doc(issue, project)

                # Extract notes from GraphQL response (with pagination)
                notes = await self._extract_notes_from_discussions(
                    issue.discussions,
                    project.full_path,
                    issue.iid,
                    "issue",
                )
                issue_doc["notes"] = notes

                yield issue_doc, None

            # Fetch and process merge requests for this project
            async for mr in self.gitlab_client.get_merge_requests(project.full_path):
                # Fetch remaining fields if they have more pages
                await self._fetch_remaining_issue_fields(
                    mr, project.full_path, "mergeRequest"
                )

                mr_doc = self._format_merge_request_doc(mr, project)

                # Extract notes from GraphQL response (with pagination)
                notes = await self._extract_notes_from_discussions(
                    mr.discussions,
                    project.full_path,
                    mr.iid,
                    "mergeRequest",
                )
                mr_doc["notes"] = notes

                yield mr_doc, None

            # Fetch README files via REST
            async for readme_doc, download_func in self._fetch_readme_files(
                project_id, project
            ):
                yield readme_doc, download_func

    def _format_project_doc(self, project: GitLabProject):
        """Format project data into Elasticsearch document.

        Args:
            project (GitLabProject): Validated project model

        Returns:
            dict: Formatted project document
        """
        project_id = project.id.split("/")[-1] if "/" in project.id else project.id

        return {
            "_id": f"project_{project_id}",
            "_timestamp": project.last_activity_at,
            "type": "Project",
            "id": project_id,
            "name": project.name,
            "path": project.path,
            "full_path": project.full_path,
            "description": project.description,
            "visibility": project.visibility,
            "star_count": project.star_count,
            "forks_count": project.forks_count,
            "created_at": project.created_at,
            "last_activity_at": project.last_activity_at,
            "archived": project.archived,
            "default_branch": project.default_branch,
            "web_url": project.web_url,
        }

    def _format_issue_doc(self, issue: GitLabIssue, project: GitLabProject):
        """Format issue data into Elasticsearch document.

        Args:
            issue (GitLabIssue): Validated issue model
            project (GitLabProject): Parent project model

        Returns:
            dict: Formatted issue document
        """
        project_id = project.id.split("/")[-1] if "/" in project.id else project.id

        return {
            "_id": f"issue_{project_id}_{issue.iid}",
            "_timestamp": issue.updated_at,
            "type": "Issue",
            "project_id": project_id,
            "project_path": project.full_path,
            "iid": issue.iid,
            "title": issue.title,
            "description": issue.description,
            "state": issue.state,
            "created_at": issue.created_at,
            "updated_at": issue.updated_at,
            "closed_at": issue.closed_at,
            "web_url": issue.web_url,
            "author": issue.author.username if issue.author else None,
            "author_name": issue.author.name if issue.author else None,
            "assignees": [
                a.get("username") for a in issue.assignees.get("nodes", [])
            ],
            "labels": [
                label.get("title") for label in issue.labels.get("nodes", [])
            ],
        }

    async def _extract_notes_from_discussions(
        self, discussions_data, project_path, iid, issuable_type
    ):
        """Extract and flatten notes from discussions structure, paginating if needed.

        Args:
            discussions_data (dict): GraphQL discussions data
            project_path (str): Full path of the project
            iid (str): Issue or MR internal ID
            issuable_type (str): 'issue' or 'mergeRequest'

        Returns:
            list: Flattened list of notes
        """
        notes = []
        for discussion in discussions_data.get("nodes", []):
            discussion_id = discussion.get("id")
            notes_data = discussion.get("notes", {})

            # Add all notes from first fetch
            for note in notes_data.get("nodes", []):
                note_dict = {
                    "id": note.get("id"),
                    "body": note.get("body"),
                    "created_at": note.get("createdAt"),
                    "updated_at": note.get("updatedAt"),
                    "system": note.get("system", False),
                    "author": note.get("author", {}).get("username"),
                    "author_name": note.get("author", {}).get("name"),
                }

                # Add position info for diff/inline comments
                if position := note.get("position"):
                    note_dict["position"] = {
                        "new_line": position.get("newLine"),
                        "old_line": position.get("oldLine"),
                        "new_path": position.get("newPath"),
                        "old_path": position.get("oldPath"),
                        "position_type": position.get("positionType"),
                    }

                notes.append(note_dict)

            # Check if there are more notes to fetch for this discussion
            notes_page_info = notes_data.get("pageInfo", {})
            if notes_page_info.get("hasNextPage") and discussion_id:
                cursor = notes_page_info.get("endCursor")
                async for note in self.gitlab_client.fetch_remaining_notes(
                    project_path, iid, discussion_id, issuable_type, cursor
                ):
                    note_dict = {
                        "id": note.get("id"),
                        "body": note.get("body"),
                        "created_at": note.get("createdAt"),
                        "updated_at": note.get("updatedAt"),
                        "system": note.get("system", False),
                        "author": note.get("author", {}).get("username"),
                        "author_name": note.get("author", {}).get("name"),
                    }

                    # Add position info for diff/inline comments
                    if position := note.get("position"):
                        note_dict["position"] = {
                            "new_line": position.get("newLine"),
                            "old_line": position.get("oldLine"),
                            "new_path": position.get("newPath"),
                            "old_path": position.get("oldPath"),
                            "position_type": position.get("positionType"),
                        }

                    notes.append(note_dict)

        return notes

    def _format_merge_request_doc(self, mr: GitLabMergeRequest, project: GitLabProject):
        """Format merge request data into Elasticsearch document.

        Args:
            mr (GitLabMergeRequest): Validated merge request model
            project (GitLabProject): Parent project model

        Returns:
            dict: Formatted merge request document
        """
        project_id = project.id.split("/")[-1] if "/" in project.id else project.id

        return {
            "_id": f"mr_{project_id}_{mr.iid}",
            "_timestamp": mr.updated_at,
            "type": "Merge Request",
            "project_id": project_id,
            "project_path": project.full_path,
            "iid": mr.iid,
            "title": mr.title,
            "description": mr.description,
            "state": mr.state,
            "created_at": mr.created_at,
            "updated_at": mr.updated_at,
            "merged_at": mr.merged_at,
            "closed_at": mr.closed_at,
            "web_url": mr.web_url,
            "source_branch": mr.source_branch,
            "target_branch": mr.target_branch,
            "author": mr.author.username if mr.author else None,
            "author_name": mr.author.name if mr.author else None,
            "assignees": [
                a.get("username") for a in mr.assignees.get("nodes", [])
            ],
            "reviewers": [
                r.get("username") for r in mr.reviewers.get("nodes", [])
            ],
            "approved_by": [
                a.get("username") for a in mr.approved_by.get("nodes", [])
            ],
            "merged_by": mr.merged_by.username if mr.merged_by else None,
            "labels": [
                label.get("title") for label in mr.labels.get("nodes", [])
            ],
        }

    async def _fetch_readme_files(self, project_id, project: GitLabProject):
        """Fetch README files from a project using REST API.

        Args:
            project_id (int): Numeric project ID
            project (GitLabProject): Validated project model

        Yields:
            tuple: (document dict, download function)
        """
        default_branch = project.default_branch
        if not default_branch:
            return

        # Use REST API to get repository tree
        # GET /projects/:id/repository/tree?ref=:branch&path=/&recursive=false
        try:
            tree_items = await self.gitlab_client._get_rest(
                f"projects/{project_id}/repository/tree",
                params={"ref": default_branch, "path": "/", "recursive": "false"},
            )
        except Exception as e:
            self._logger.debug(f"Failed to fetch tree for project {project_id}: {e}")
            return

        if not tree_items:
            return

        for item in tree_items:
            # Only process blob (file) items
            if item.get("type") != "blob":
                continue

            file_name = item.get("name", "").lower()
            file_path = item.get("path", "")

            # Check if it's a README file
            if not file_name.startswith("readme"):
                continue

            # Check if extension is supported
            file_extension = ""
            if "." in file_name:
                file_extension = file_name[file_name.rfind(".") :]

            if file_extension not in SUPPORTED_EXTENSION and file_extension != "":
                continue

            # Create README document
            readme_doc = {
                "_id": f"file_{project_id}_{file_path}",
                "_timestamp": project.last_activity_at,
                "type": "File",
                "project_id": project_id,
                "project_path": project.full_path,
                "file_path": file_path,
                "file_name": item.get("name"),
                "extension": file_extension,
                "web_url": f"{project.web_url}/-/blob/{default_branch}/{file_path}",
            }

            # Create metadata for download
            file_metadata = {
                "project_id": project_id,
                "file_path": file_path,
                "file_name": item.get("name"),
                "ref": default_branch,
                "_timestamp": project.last_activity_at,
            }

            yield readme_doc, partial(self.get_content, attachment=file_metadata)

    async def get_content(self, attachment, timestamp=None, doit=False):
        """Extract content for supported file types.

        Args:
            attachment (dict): File metadata
            timestamp (str, optional): File timestamp
            doit (bool, optional): Whether to download content

        Returns:
            dict: Content document with _id, _timestamp, and attachment content
        """
        if not doit:
            return

        project_id = attachment["project_id"]
        file_path = attachment["file_path"]
        file_name = attachment["file_name"]

        # Check if file can be downloaded
        file_extension = self.get_file_extension(file_name)
        if not self.can_file_be_downloaded(file_extension, file_name, 0):
            return

        document = {
            "_id": f"file_{project_id}_{file_path}",
            "_timestamp": attachment["_timestamp"],
        }

        return await self.download_and_extract_file(
            document,
            file_name,
            file_extension,
            partial(
                self.download_func,
                attachment["project_id"],
                attachment["file_path"],
                attachment["ref"],
            ),
        )

    async def download_func(self, project_id, file_path, ref=None):
        """Download file content from GitLab.

        Args:
            project_id (int): Project ID
            file_path (str): File path
            ref (str, optional): Branch/tag reference

        Yields:
            bytes: File content
        """
        file_data = await self.gitlab_client.get_file_content(
            project_id, file_path, ref
        )

        if file_data and "content" in file_data:
            # GitLab returns base64-encoded content
            content = file_data["content"]
            yield decode_base64_value(content=content)
        else:
            yield
