#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""GitLab source module responsible to fetch documents from GitLab Cloud."""

import asyncio
from functools import partial

import gitlab

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
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
            notes {{
              nodes {{
                id
                body
                createdAt
                updatedAt
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
            notes {{
              nodes {{
                id
                body
                createdAt
                updatedAt
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


class GitLabClient:
    """Client for interacting with GitLab Cloud API using python-gitlab library."""

    def __init__(self, token):
        self._logger = logger
        self.token = token
        # Initialize async GraphQL client
        self.graphql_client = gitlab.AsyncGraphQL(GITLAB_CLOUD_URL, token=token)
        # Initialize REST client for fallbacks
        self.rest_client = gitlab.Gitlab(GITLAB_CLOUD_URL, private_token=token)

    def set_logger(self, logger_):
        self._logger = logger_

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_projects(self):
        """Fetch projects using GraphQL.

        Yields:
            dict: Project data
        """
        cursor = None

        while True:
            variables = {"cursor": cursor} if cursor else {}

            try:
                result = await self.graphql_client.execute(PROJECTS_QUERY, variables)
            except Exception as e:
                self._logger.exception(f"GraphQL query failed: {e}")
                raise

            projects_data = result.get("projects", {})
            projects = projects_data.get("nodes", [])

            for project in projects:
                yield project

            # Check pagination
            page_info = projects_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

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
            dict: Issue data
        """
        cursor = None

        while True:
            variables = {"projectPath": project_path}
            if cursor:
                variables["cursor"] = cursor

            try:
                result = await self.graphql_client.execute(ISSUES_QUERY, variables)
            except Exception as e:
                self._logger.warning(f"Failed to fetch issues for {project_path}: {e}")
                return

            project_data = result.get("project")
            if not project_data:
                return

            issues_data = project_data.get("issues", {})
            issues = issues_data.get("nodes", [])

            for issue in issues:
                yield issue

            # Check pagination
            page_info = issues_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

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
            dict: Merge request data
        """
        cursor = None

        while True:
            variables = {"projectPath": project_path}
            if cursor:
                variables["cursor"] = cursor

            try:
                result = await self.graphql_client.execute(
                    MERGE_REQUESTS_QUERY, variables
                )
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

            for mr in mrs:
                yield mr

            # Check pagination
            page_info = mrs_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

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
                result = await self.graphql_client.execute(query, variables)
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

    async def get_file_content(self, project_id, file_path, ref=None):
        """Get file content from repository using REST API (runs in thread pool).

        Args:
            project_id (int): Project ID
            file_path (str): Path to file
            ref (str, optional): Branch/tag name

        Returns:
            dict: File data with content (base64 encoded)
        """

        def _sync_get_file():
            try:
                project = self.rest_client.projects.get(project_id)
                file_data = project.files.get(
                    file_path, ref=ref or project.default_branch
                )
                return file_data.attributes
            except Exception as e:
                self._logger.warning(
                    f"Failed to fetch file {file_path} from project {project_id}: {e}"
                )
                return None

        return await asyncio.to_thread(_sync_get_file)

    async def ping(self):
        """Test the connection to GitLab (runs in thread pool)."""

        def _sync_auth():
            try:
                self.rest_client.auth()
                self._logger.debug("Successfully authenticated with GitLab")
            except Exception as e:
                msg = f"Failed to connect to GitLab: {e}"
                raise ConfigurableFieldValueError(msg) from e

        await asyncio.to_thread(_sync_auth)

    async def close(self):
        """Close the client connections."""
        # python-gitlab clients don't need explicit closing
        pass


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
        # Test authentication by trying to authenticate
        try:
            self.gitlab_client.rest_client.auth()
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

        def _sync_validate_project(project_path):
            try:
                self.gitlab_client.rest_client.projects.get(project_path)
                self._logger.debug(f"✓ Project '{project_path}' is accessible")
                return None
            except Exception as e:
                self._logger.warning(
                    f"✗ Project '{project_path}' is not accessible: {e}"
                )
                return project_path

        invalid_projects = []

        for project_path in self.configured_projects:
            if not project_path or project_path.strip() == "":
                continue

            # Run validation in thread pool
            result = await asyncio.to_thread(_sync_validate_project, project_path)
            if result:
                invalid_projects.append(result)

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
            issuable (dict): Issue or MR data from GraphQL
            project_path (str): Full path of the project
            issuable_type (str): 'issue' or 'mergeRequest'
        """
        iid = issuable.get("iid")
        if not iid:
            return

        # Check and fetch remaining assignees
        assignees_data = issuable.get("assignees", {})
        assignees_page_info = assignees_data.get("pageInfo", {})
        if assignees_page_info.get("hasNextPage"):
            cursor = assignees_page_info.get("endCursor")
            async for assignee in self.gitlab_client.fetch_remaining_field(
                project_path, iid, "assignees", issuable_type, cursor
            ):
                issuable["assignees"]["nodes"].append(assignee)

        # Check and fetch remaining labels
        labels_data = issuable.get("labels", {})
        labels_page_info = labels_data.get("pageInfo", {})
        if labels_page_info.get("hasNextPage"):
            cursor = labels_page_info.get("endCursor")
            async for label in self.gitlab_client.fetch_remaining_field(
                project_path, iid, "labels", issuable_type, cursor
            ):
                issuable["labels"]["nodes"].append(label)

        # Check and fetch remaining discussions
        discussions_data = issuable.get("discussions", {})
        discussions_page_info = discussions_data.get("pageInfo", {})
        if discussions_page_info.get("hasNextPage"):
            cursor = discussions_page_info.get("endCursor")
            async for discussion in self.gitlab_client.fetch_remaining_field(
                project_path, iid, "discussions", issuable_type, cursor
            ):
                issuable["discussions"]["nodes"].append(discussion)

        # Check and fetch remaining reviewers (MRs only)
        if issuable_type == "mergeRequest":
            reviewers_data = issuable.get("reviewers", {})
            reviewers_page_info = reviewers_data.get("pageInfo", {})
            if reviewers_page_info.get("hasNextPage"):
                cursor = reviewers_page_info.get("endCursor")
                async for reviewer in self.gitlab_client.fetch_remaining_field(
                    project_path, iid, "reviewers", issuable_type, cursor
                ):
                    issuable["reviewers"]["nodes"].append(reviewer)

            # Check and fetch remaining approvedBy (MRs only)
            approvedby_data = issuable.get("approvedBy", {})
            approvedby_page_info = approvedby_data.get("pageInfo", {})
            if approvedby_page_info.get("hasNextPage"):
                cursor = approvedby_page_info.get("endCursor")
                async for approver in self.gitlab_client.fetch_remaining_field(
                    project_path, iid, "approvedBy", issuable_type, cursor
                ):
                    issuable["approvedBy"]["nodes"].append(approver)

    async def get_docs(self, filtering=None):
        """Main method to fetch documents from GitLab.

        Args:
            filtering (Filtering, optional): Filtering rules. Defaults to None.

        Yields:
            tuple: (document dict, download function or None)
        """
        # Fetch projects via GraphQL
        async for project_data in self.gitlab_client.get_projects():
            # Extract project ID (GraphQL returns global ID, need numeric ID)
            project_gid = project_data.get("id", "")
            # GitLab GraphQL IDs are like "gid://gitlab/Project/123"
            project_id = int(project_gid.split("/")[-1]) if "/" in project_gid else None

            if not project_id:
                self._logger.warning(f"Could not extract project ID from {project_gid}")
                continue

            # Filter projects based on configuration
            project_full_path = project_data.get("fullPath", "")
            if not self._should_sync_project(project_full_path):
                self._logger.debug(
                    f"Skipping project '{project_full_path}' (not in configured projects)"
                )
                continue

            # Yield project document
            project_doc = self._format_project_doc(project_data)
            yield project_doc, None

            # Fetch and process issues for this project
            async for issue in self.gitlab_client.get_issues(project_full_path):
                # Fetch remaining fields if they have more pages
                await self._fetch_remaining_issue_fields(
                    issue, project_full_path, "issue"
                )

                issue_doc = self._format_issue_doc(issue, project_data)

                # Extract notes from GraphQL response
                notes = self._extract_notes_from_discussions(
                    issue.get("discussions", {})
                )
                issue_doc["notes"] = notes

                yield issue_doc, None

            # Fetch and process merge requests for this project
            async for mr in self.gitlab_client.get_merge_requests(project_full_path):
                # Fetch remaining fields if they have more pages
                await self._fetch_remaining_issue_fields(
                    mr, project_full_path, "mergeRequest"
                )

                mr_doc = self._format_merge_request_doc(mr, project_data)

                # Extract notes from GraphQL response
                notes = self._extract_notes_from_discussions(mr.get("discussions", {}))
                mr_doc["notes"] = notes

                yield mr_doc, None

            # Fetch README files via REST
            async for readme_doc, download_func in self._fetch_readme_files(
                project_id, project_data
            ):
                yield readme_doc, download_func

    def _format_project_doc(self, project_data):
        """Format project data into Elasticsearch document.

        Args:
            project_data (dict): GraphQL project data

        Returns:
            dict: Formatted project document
        """
        project_gid = project_data.get("id", "")
        project_id = project_gid.split("/")[-1] if "/" in project_gid else project_gid

        return {
            "_id": f"project_{project_id}",
            "_timestamp": project_data.get("lastActivityAt"),
            "type": "Project",
            "id": project_id,
            "name": project_data.get("name"),
            "path": project_data.get("path"),
            "full_path": project_data.get("fullPath"),
            "description": project_data.get("description"),
            "visibility": project_data.get("visibility"),
            "star_count": project_data.get("starCount", 0),
            "forks_count": project_data.get("forksCount", 0),
            "created_at": project_data.get("createdAt"),
            "last_activity_at": project_data.get("lastActivityAt"),
            "default_branch": project_data.get("repository", {}).get("rootRef"),
            "archived": project_data.get("archived", False),
            "web_url": project_data.get("webUrl"),
        }

    def _format_issue_doc(self, issue, project_data):
        """Format issue data into Elasticsearch document.

        Args:
            issue (dict): GraphQL issue data
            project_data (dict): Parent project data

        Returns:
            dict: Formatted issue document
        """
        project_gid = project_data.get("id", "")
        project_id = project_gid.split("/")[-1] if "/" in project_gid else project_gid

        return {
            "_id": f"issue_{project_id}_{issue['iid']}",
            "_timestamp": issue.get("updatedAt"),
            "type": "Issue",
            "project_id": project_id,
            "project_path": project_data.get("fullPath"),
            "iid": issue.get("iid"),
            "title": issue.get("title"),
            "description": issue.get("description"),
            "state": issue.get("state"),
            "created_at": issue.get("createdAt"),
            "updated_at": issue.get("updatedAt"),
            "closed_at": issue.get("closedAt"),
            "web_url": issue.get("webUrl"),
            "author": issue.get("author", {}).get("username"),
            "author_name": issue.get("author", {}).get("name"),
            "assignees": [
                a.get("username") for a in issue.get("assignees", {}).get("nodes", [])
            ],
            "labels": [
                label.get("title") for label in issue.get("labels", {}).get("nodes", [])
            ],
        }

    def _extract_notes_from_discussions(self, discussions_data):
        """Extract and flatten notes from discussions structure.

        Args:
            discussions_data (dict): GraphQL discussions data

        Returns:
            list: Flattened list of notes
        """
        notes = []
        for discussion in discussions_data.get("nodes", []):
            for note in discussion.get("notes", {}).get("nodes", []):
                notes.append(
                    {
                        "id": note.get("id"),
                        "body": note.get("body"),
                        "created_at": note.get("createdAt"),
                        "updated_at": note.get("updatedAt"),
                        "author": note.get("author", {}).get("username"),
                        "author_name": note.get("author", {}).get("name"),
                    }
                )
        return notes

    def _format_merge_request_doc(self, mr, project_data):
        """Format merge request data into Elasticsearch document.

        Args:
            mr (dict): GraphQL merge request data
            project_data (dict): Parent project data

        Returns:
            dict: Formatted merge request document
        """
        project_gid = project_data.get("id", "")
        project_id = project_gid.split("/")[-1] if "/" in project_gid else project_gid

        return {
            "_id": f"mr_{project_id}_{mr['iid']}",
            "_timestamp": mr.get("updatedAt"),
            "type": "Merge Request",
            "project_id": project_id,
            "project_path": project_data.get("fullPath"),
            "iid": mr.get("iid"),
            "title": mr.get("title"),
            "description": mr.get("description"),
            "state": mr.get("state"),
            "created_at": mr.get("createdAt"),
            "updated_at": mr.get("updatedAt"),
            "merged_at": mr.get("mergedAt"),
            "closed_at": mr.get("closedAt"),
            "web_url": mr.get("webUrl"),
            "source_branch": mr.get("sourceBranch"),
            "target_branch": mr.get("targetBranch"),
            "author": mr.get("author", {}).get("username"),
            "author_name": mr.get("author", {}).get("name"),
            "assignees": [
                a.get("username") for a in mr.get("assignees", {}).get("nodes", [])
            ],
            "reviewers": [
                r.get("username") for r in mr.get("reviewers", {}).get("nodes", [])
            ],
            "approved_by": [
                a.get("username")
                for a in mr.get("approvedBy", {}).get("nodes", [])
            ],
            "merged_by": mr.get("mergeUser", {}).get("username")
            if mr.get("mergeUser")
            else None,
            "labels": [
                label.get("title") for label in mr.get("labels", {}).get("nodes", [])
            ],
        }

    async def _fetch_readme_files(self, project_id, project_data):
        """Fetch README files from a project.

        Args:
            project_id (int): Numeric project ID
            project_data (dict): Project data from GraphQL

        Yields:
            tuple: (document dict, download function)
        """
        # Get repository data from GraphQL
        repository = project_data.get("repository", {})
        if not repository:
            return

        default_branch = repository.get("rootRef")
        if not default_branch:
            return

        # Get file tree from GraphQL data
        tree = repository.get("tree", {})
        blobs = tree.get("blobs", {})
        tree_items = blobs.get("nodes", [])

        for item in tree_items:
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
                "_timestamp": project_data.get("lastActivityAt"),
                "type": "File",
                "project_id": project_id,
                "project_path": project_data.get("fullPath"),
                "file_path": file_path,
                "file_name": item.get("name"),
                "extension": file_extension,
                "size": 0,  # Tree API doesn't provide size
                "web_url": f"{project_data.get('webUrl')}/-/blob/{default_branch}/{file_path}",
            }

            # Create metadata for download
            file_metadata = {
                "project_id": project_id,
                "file_path": file_path,
                "file_name": item.get("name"),
                "ref": default_branch,
                "_timestamp": project_data.get("lastActivityAt"),
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
