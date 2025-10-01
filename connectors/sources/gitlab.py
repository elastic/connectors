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

SUPPORTED_EXTENSION = [".md", ".rst", ".txt"]

# GraphQL query to fetch projects with nested issues and MRs
PROJECTS_QUERY = """
query($cursor: String) {
  projects(membership: true, first: 100, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
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
      defaultBranch
      archived
      webUrl
      repository {
        tree(recursive: true) {
          blobs {
            nodes {
              name
              path
              type
            }
          }
        }
      }
      issues(first: 100) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          iid
          title
          description
          state
          createdAt
          updatedAt
          closedAt
          webUrl
          author {
            username
            name
          }
          assignees {
            nodes {
              username
              name
            }
          }
          labels {
            nodes {
              title
            }
          }
          userNotesCount
          discussions(first: 100) {
            nodes {
              notes {
                nodes {
                  id
                  body
                  createdAt
                  updatedAt
                  author {
                    username
                    name
                  }
                }
              }
            }
          }
        }
      }
      mergeRequests(first: 100) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
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
          author {
            username
            name
          }
          assignees {
            nodes {
              username
              name
            }
          }
          labels {
            nodes {
              title
            }
          }
          userNotesCount
          discussions(first: 100) {
            nodes {
              notes {
                nodes {
                  id
                  body
                  createdAt
                  updatedAt
                  author {
                    username
                    name
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
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
    async def get_projects_with_issues_and_mrs(self):
        """Fetch projects with nested issues and merge requests using GraphQL.

        This is the main bulk fetch method that reduces API calls significantly.

        Yields:
            dict: Project data with nested issues and merge requests
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
                file_data = project.files.get(file_path, ref=ref or project.default_branch)
                return file_data.attributes
            except Exception as e:
                self._logger.warning(f"Failed to fetch file {file_path} from project {project_id}: {e}")
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
        self.configured_projects = self.configuration.get("projects", [])

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
                project = self.gitlab_client.rest_client.projects.get(project_path)
                self._logger.debug(f"✓ Project '{project_path}' is accessible")
                return None
            except Exception as e:
                self._logger.warning(f"✗ Project '{project_path}' is not accessible: {e}")
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

    async def get_docs(self, filtering=None):
        """Main method to fetch documents from GitLab.

        Args:
            filtering (Filtering, optional): Filtering rules. Defaults to None.

        Yields:
            tuple: (document dict, download function or None)
        """
        # Fetch projects with nested issues and MRs via GraphQL
        async for project_data in self.gitlab_client.get_projects_with_issues_and_mrs():
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
                self._logger.debug(f"Skipping project '{project_full_path}' (not in configured projects)")
                continue

            # Yield project document
            project_doc = self._format_project_doc(project_data)
            yield project_doc, None

            # Process issues from GraphQL response
            issues_data = project_data.get("issues", {})
            for issue in issues_data.get("nodes", []):
                issue_doc = self._format_issue_doc(issue, project_data)

                # Extract notes from GraphQL response
                notes = self._extract_notes_from_discussions(issue.get("discussions", {}))
                issue_doc["notes"] = notes

                yield issue_doc, None

            # Process merge requests from GraphQL response
            mrs_data = project_data.get("mergeRequests", {})
            for mr in mrs_data.get("nodes", []):
                mr_doc = self._format_merge_request_doc(mr, project_data)

                # Extract notes from GraphQL response
                notes = self._extract_notes_from_discussions(mr.get("discussions", {}))
                mr_doc["notes"] = notes

                yield mr_doc, None

            # Fetch README files via REST
            async for readme_doc, download_func in self._fetch_readme_files(project_id, project_data):
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
            "default_branch": project_data.get("defaultBranch"),
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
            "assignees": [a.get("username") for a in issue.get("assignees", {}).get("nodes", [])],
            "labels": [l.get("title") for l in issue.get("labels", {}).get("nodes", [])],
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
                notes.append({
                    "id": note.get("id"),
                    "body": note.get("body"),
                    "created_at": note.get("createdAt"),
                    "updated_at": note.get("updatedAt"),
                    "author": note.get("author", {}).get("username"),
                    "author_name": note.get("author", {}).get("name"),
                })
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
            "assignees": [a.get("username") for a in mr.get("assignees", {}).get("nodes", [])],
            "labels": [l.get("title") for l in mr.get("labels", {}).get("nodes", [])],
        }

    async def _fetch_readme_files(self, project_id, project_data):
        """Fetch README files from a project.

        Args:
            project_id (int): Numeric project ID
            project_data (dict): Project data from GraphQL

        Yields:
            tuple: (document dict, download function)
        """
        default_branch = project_data.get("defaultBranch")
        if not default_branch:
            return

        # Get repository tree from GraphQL data
        repository = project_data.get("repository", {})
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
                file_extension = file_name[file_name.rfind("."):]

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
        file_data = await self.gitlab_client.get_file_content(project_id, file_path, ref)

        if file_data and "content" in file_data:
            # GitLab returns base64-encoded content
            content = file_data["content"]
            yield decode_base64_value(content=content)
        else:
            yield