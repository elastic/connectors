#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""GitLab source module responsible to fetch documents from GitLab Cloud.

This connector fetches:
- Projects (repositories)
- Issues (using Work Items API)
- Merge Requests (using legacy API)
- Epics (using Work Items API, group-level, requires Premium/Ultimate tier)
- Releases (project-level version releases with changelogs)
- README files (.md, .rst, .txt)

This connector uses GitLab's Work Items API for Issues and Epics. Merge Requests
continue to use the legacy GraphQL API as they have not yet been migrated to Work Items.

Work Items API Reference: https://docs.gitlab.com/ee/api/graphql/reference/#workitem
"""

from functools import partial
from typing import Any, AsyncGenerator, Type, TypeVar

from pydantic import BaseModel

from connectors.source import (
    BaseDataSource,
    ConfigurableFieldValueError,
    DataSourceConfiguration,
)
from connectors.sources.gitlab.client import GitLabClient
from connectors.sources.gitlab.document_schemas import (
    FileDocument,
)
from connectors.sources.gitlab.models import (
    GitLabDiscussion,
    GitLabIssue,
    GitLabLabel,
    GitLabMergeRequest,
    GitLabProject,
    GitLabRelease,
    GitLabUser,
    GitLabWorkItem,
    PaginatedList,
    WorkItemType,
    WorkItemWidgetAssignees,
    WorkItemWidgetDescription,
    WorkItemWidgetHierarchy,
    WorkItemWidgetLabels,
    WorkItemWidgetLinkedItems,
    WorkItemWidgetNotes,
)
from connectors.sources.gitlab.queries import VALIDATE_PROJECTS_QUERY
from connectors.utils import decode_base64_value

SUPPORTED_EXTENSION = [".md", ".rst", ".txt"]


class GitLabDataSource(BaseDataSource):
    """GitLab Cloud Data Source."""

    name = "GitLab"
    service_type = "gitlab"
    advanced_rules_enabled = False
    dls_enabled = False
    incremental_sync_enabled = False

    def __init__(self, configuration: DataSourceConfiguration) -> None:
        """Setup the connection to the GitLab instance.

        Args:
            configuration: Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.gitlab_client = GitLabClient(
            token=self.configuration["token"],
        )
        self.configured_projects = self.configuration["projects"]

    def _set_internal_logger(self) -> None:
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
                "tooltip": "List of project paths (e.g., 'group/project'). Use '*' to sync all projects where the token's user is a member.",
                "type": "list",
                "value": [],
            },
        }

    async def validate_config(self) -> None:
        """Validates whether user input is empty or not for configuration fields.
        Also validates if the configured projects are accessible.
        """
        await super().validate_config()
        await self._remote_validation()

    async def _remote_validation(self) -> None:
        """Validate GitLab connection and project accessibility.

        Raises:
            ConfigurableFieldValueError: If validation fails.
        """
        try:
            await self.gitlab_client.ping()
        except Exception as e:
            msg = f"Failed to authenticate with GitLab: {e}"
            raise ConfigurableFieldValueError(msg) from e

        if self.configured_projects and self.configured_projects != ["*"]:
            await self._validate_configured_projects()

    async def _validate_configured_projects(self, batch_size: int = 50) -> None:
        """Validate that configured projects exist and are accessible using batched GraphQL queries.

        GitLab's GraphQL API allows querying up to 50 projects at once using the fullPaths parameter.

        Args:
            batch_size: Number of projects to validate per GraphQL query (default: 50).

        Raises:
            ConfigurableFieldValueError: If any project is invalid.
        """
        valid_project_paths = [
            p.strip() for p in self.configured_projects if p and p.strip()
        ]

        if not valid_project_paths:
            return

        accessible_projects = set()

        for i in range(0, len(valid_project_paths), batch_size):
            batch = valid_project_paths[i : i + batch_size]

            try:
                result = await self.gitlab_client._execute_graphql(
                    VALIDATE_PROJECTS_QUERY, {"projectPaths": batch}
                )

                projects_data = result.get("projects", {})
                nodes = projects_data.get("nodes", [])

                for node in nodes:
                    full_path = node.get("fullPath")
                    if full_path:
                        accessible_projects.add(full_path)
                        self._logger.debug(f"✓ Project '{full_path}' is accessible")

            except Exception as e:
                self._logger.warning(f"Failed to validate project batch {batch}: {e}")
                # If batch query fails, we can't determine which specific projects are invalid
                # so we skip this batch and continue

        requested_projects = set(valid_project_paths)
        invalid_projects = requested_projects - accessible_projects

        for project in invalid_projects:
            self._logger.warning(f"✗ Project '{project}' is not accessible")

        if invalid_projects:
            msg = f"The following projects are not accessible: {', '.join(sorted(invalid_projects))}. Please check the project paths and ensure your token has access."
            raise ConfigurableFieldValueError(msg)

    async def ping(self) -> None:
        """Test the connection to GitLab."""
        try:
            await self.gitlab_client.ping()
            self._logger.info("Successfully connected to GitLab.")
        except Exception:
            self._logger.exception("Error while connecting to GitLab.")
            raise

    async def close(self) -> None:
        """Close the GitLab client connection."""
        await self.gitlab_client.close()

    def _should_sync_project(self, project_path):
        """Check if a project should be synced based on configuration.

        Args:
            project_path (str): Full path of the project (e.g., 'group/project')

        Returns:
            bool: True if project should be synced, False otherwise
        """
        if not self.configured_projects or "*" in self.configured_projects:
            return True

        return project_path in self.configured_projects

    def _validate_and_filter_project(self, project: GitLabProject) -> str | None:
        """Validate and filter project based on configuration.

        Args:
            project: GitLab project to validate

        Returns:
            Project ID if valid and should be synced, None otherwise
        """
        # Extract project ID (GraphQL returns global ID, need numeric ID)
        project_id = self.gitlab_client._extract_numeric_id(project.id)

        if not project_id:
            self._logger.warning(f"Could not extract project ID from {project.id}")
            return None

        if not self._should_sync_project(project.full_path):
            self._logger.debug(
                f"Skipping project '{project.full_path}' (not in configured projects)"
            )
            return None

        return project_id

    T = TypeVar("T", bound=BaseModel)

    def _extract_widget(
        self, work_item: GitLabWorkItem, widget_type: Type[T]
    ) -> T | None:
        """Generic method to extract a specific widget type from work item.

        Args:
            work_item: Work item containing widgets
            widget_type: The widget class type to search for

        Returns:
            The widget instance if found, None otherwise
        """
        for widget in work_item.widgets:
            if isinstance(widget, widget_type):
                return widget
        return None

    def _extract_widget_description(self, work_item: GitLabWorkItem) -> str | None:
        """Extract description from work item widgets."""
        widget = self._extract_widget(work_item, WorkItemWidgetDescription)
        return widget.description if widget else None

    def _extract_widget_assignees(
        self, work_item: GitLabWorkItem
    ) -> PaginatedList[GitLabUser]:
        """Extract assignees data from work item widgets."""
        widget = self._extract_widget(work_item, WorkItemWidgetAssignees)
        return widget.assignees if widget else PaginatedList[GitLabUser](nodes=[])

    def _extract_widget_labels(
        self, work_item: GitLabWorkItem
    ) -> PaginatedList[GitLabLabel]:
        """Extract labels data from work item widgets."""
        widget = self._extract_widget(work_item, WorkItemWidgetLabels)
        return widget.labels if widget else PaginatedList[GitLabLabel](nodes=[])

    def _extract_widget_discussions(
        self, work_item: GitLabWorkItem
    ) -> PaginatedList[GitLabDiscussion]:
        """Extract discussions from work item widgets."""
        widget = self._extract_widget(work_item, WorkItemWidgetNotes)
        return (
            widget.discussions if widget else PaginatedList[GitLabDiscussion](nodes=[])
        )

    def _extract_widget_hierarchy(
        self, work_item: GitLabWorkItem
    ) -> WorkItemWidgetHierarchy | None:
        """Extract hierarchy info from work item widgets (for Epics)."""
        return self._extract_widget(work_item, WorkItemWidgetHierarchy)

    def _extract_widget_linked_items(
        self, work_item: GitLabWorkItem
    ) -> WorkItemWidgetLinkedItems | None:
        """Extract linked items from work item widgets (for Epics/Issues)."""
        return self._extract_widget(work_item, WorkItemWidgetLinkedItems)

    async def _fetch_remaining_paginated_field(
        self,
        paginated_list: PaginatedList[T],
        project_path: str,
        iid: int,
        field_name: str,
        issuable_type: str,
        model_class: Type[T],
    ) -> None:
        """Generic method to fetch remaining items for any paginated field.

        Args:
            paginated_list: The paginated list to append items to
            project_path: Full path of the project
            iid: Issue or MR internal ID
            field_name: Name of field ('assignees', 'labels', 'discussions', 'reviewers', 'approvedBy')
            issuable_type: Type of issuable ('issue' or 'mergeRequest')
            model_class: Pydantic model class for validation
        """
        if paginated_list.page_info.has_next_page:
            cursor = paginated_list.page_info.end_cursor
            async for item in self.gitlab_client.fetch_remaining_field(
                project_path, iid, field_name, issuable_type, cursor
            ):
                paginated_list.nodes.append(model_class.model_validate(item))

    async def _fetch_remaining_assignees(
        self,
        issuable: GitLabIssue | GitLabMergeRequest,
        project_path: str,
        issuable_type: str,
    ) -> None:
        """Fetch remaining assignees for an issue or MR."""
        await self._fetch_remaining_paginated_field(
            issuable.assignees,
            project_path,
            issuable.iid,
            "assignees",
            issuable_type,
            GitLabUser,
        )

    async def _fetch_remaining_labels(
        self,
        issuable: GitLabIssue | GitLabMergeRequest,
        project_path: str,
        issuable_type: str,
    ) -> None:
        """Fetch remaining labels for an issue or MR."""
        await self._fetch_remaining_paginated_field(
            issuable.labels,
            project_path,
            issuable.iid,
            "labels",
            issuable_type,
            GitLabLabel,
        )

    async def _fetch_remaining_discussions(
        self,
        issuable: GitLabIssue | GitLabMergeRequest,
        project_path: str,
        issuable_type: str,
    ) -> None:
        """Fetch remaining discussions for an issue or MR."""
        await self._fetch_remaining_paginated_field(
            issuable.discussions,
            project_path,
            issuable.iid,
            "discussions",
            issuable_type,
            GitLabDiscussion,
        )

    async def _fetch_remaining_issue_fields(
        self,
        issue: GitLabIssue,
        project_path: str,
    ) -> None:
        """Fetch remaining paginated fields for an issue.

        Args:
            issue: Issue Pydantic model
            project_path: Full path of the project
        """
        await self._fetch_remaining_assignees(issue, project_path, "issue")
        await self._fetch_remaining_labels(issue, project_path, "issue")
        await self._fetch_remaining_discussions(issue, project_path, "issue")

    async def _fetch_remaining_mr_fields(
        self,
        mr: GitLabMergeRequest,
        project_path: str,
    ) -> None:
        """Fetch remaining paginated fields for a merge request.

        Args:
            mr: Merge request Pydantic model
            project_path: Full path of the project
        """
        await self._fetch_remaining_assignees(mr, project_path, "mergeRequest")
        await self._fetch_remaining_labels(mr, project_path, "mergeRequest")
        await self._fetch_remaining_discussions(mr, project_path, "mergeRequest")

        # MR-specific fields
        await self._fetch_remaining_paginated_field(
            mr.reviewers, project_path, mr.iid, "reviewers", "mergeRequest", GitLabUser
        )
        await self._fetch_remaining_paginated_field(
            mr.approved_by,
            project_path,
            mr.iid,
            "approvedBy",
            "mergeRequest",
            GitLabUser,
        )

    async def _fetch_work_items_for_project(
        self,
        project: GitLabProject,
        work_item_type: WorkItemType,
    ):
        """Fetch and process work items (Issues) for a project.

        Args:
            project: GitLab project
            work_item_type: Type of work items to fetch (ISSUE, TASK, etc.)

        Yields:
            tuple: (work item document dict, None)
        """
        async for work_item in self.gitlab_client.get_work_items_project(
            project.full_path, [work_item_type]
        ):
            assignees_data = self._extract_widget_assignees(work_item)
            labels_data = self._extract_widget_labels(work_item)

            if assignees_data.page_info.has_next_page:
                cursor = assignees_data.page_info.end_cursor
                async for (
                    assignee
                ) in self.gitlab_client.fetch_remaining_work_item_assignees(
                    project.full_path, work_item.iid, work_item_type, cursor
                ):
                    assignees_data.nodes.append(GitLabUser.model_validate(assignee))

            if labels_data.page_info.has_next_page:
                cursor = labels_data.page_info.end_cursor
                async for label in self.gitlab_client.fetch_remaining_work_item_labels(
                    project.full_path, work_item.iid, work_item_type, cursor
                ):
                    labels_data.nodes.append(GitLabLabel.model_validate(label))

            discussions_data = self._extract_widget_discussions(work_item)

            if discussions_data.page_info.has_next_page:
                cursor = discussions_data.page_info.end_cursor
                async for (
                    discussion
                ) in self.gitlab_client.fetch_remaining_work_item_discussions(
                    project.full_path, work_item.iid, work_item_type, cursor
                ):
                    discussions_data.nodes.append(
                        GitLabDiscussion.model_validate(discussion)
                    )

            notes = await self._extract_notes_from_discussions(
                discussions_data,
                project.full_path,
                work_item.iid,
                "issue",
            )

            work_item_doc = self._format_work_item_doc(
                work_item,
                project=project,
                assignees_data=assignees_data,
                labels_data=labels_data,
                notes=notes,
            )

            yield work_item_doc, None

    async def _fetch_merge_requests_for_project(self, project: GitLabProject):
        """Fetch and process merge requests for a project.

        Args:
            project: GitLab project

        Yields:
            tuple: (merge request document dict, None)
        """
        async for mr in self.gitlab_client.get_merge_requests(project.full_path):
            await self._fetch_remaining_mr_fields(mr, project.full_path)

            notes = await self._extract_notes_from_discussions(
                mr.discussions,
                project.full_path,
                mr.iid,
                "mergeRequest",
            )

            mr_doc = self._format_merge_request_doc(mr, project, notes=notes)

            yield mr_doc, None

    async def _fetch_releases_for_project(self, project: GitLabProject):
        """Fetch and process releases for a project.

        Args:
            project: GitLab project

        Yields:
            tuple: (release document dict, None)
        """
        async for release in self.gitlab_client.get_releases(project.full_path):
            release_doc = self._format_release_doc(release, project)
            yield release_doc, None

    async def _fetch_epics_for_group(self, group_path: str, seen_groups: set):
        """Fetch and process epics for a group (once per group).

        Args:
            group_path: Full path of the group
            seen_groups: Set of groups already processed

        Yields:
            tuple: (epic document dict, None)
        """
        # Only fetch epics once per group
        if group_path in seen_groups:
            return

        seen_groups.add(group_path)
        self._logger.debug(f"Fetching epics for group: {group_path}")

        try:
            async for epic in self.gitlab_client.get_work_items_group(
                group_path, [WorkItemType.EPIC]
            ):
                assignees_data = self._extract_widget_assignees(epic)
                labels_data = self._extract_widget_labels(epic)

                if assignees_data.page_info.has_next_page:
                    cursor = assignees_data.page_info.end_cursor
                    async for (
                        assignee
                    ) in self.gitlab_client.fetch_remaining_work_item_assignees_group(
                        group_path, epic.iid, WorkItemType.EPIC, cursor
                    ):
                        assignees_data.nodes.append(GitLabUser.model_validate(assignee))

                if labels_data.page_info.has_next_page:
                    cursor = labels_data.page_info.end_cursor
                    async for (
                        label
                    ) in self.gitlab_client.fetch_remaining_work_item_labels_group(
                        group_path, epic.iid, WorkItemType.EPIC, cursor
                    ):
                        labels_data.nodes.append(GitLabLabel.model_validate(label))

                discussions_data = self._extract_widget_discussions(epic)

                if discussions_data.page_info.has_next_page:
                    cursor = discussions_data.page_info.end_cursor
                    async for (
                        discussion
                    ) in self.gitlab_client.fetch_remaining_work_item_group_discussions(
                        group_path, epic.iid, WorkItemType.EPIC, cursor
                    ):
                        discussions_data.nodes.append(
                            GitLabDiscussion.model_validate(discussion)
                        )

                notes = await self._extract_notes_from_discussions(
                    discussions_data,
                    group_path,
                    epic.iid,
                    "epic",
                )

                epic_doc = self._format_work_item_doc(
                    epic,
                    group_path=group_path,
                    assignees_data=assignees_data,
                    labels_data=labels_data,
                    notes=notes,
                )

                yield epic_doc, None
        except Exception as e:
            # Epics require Premium/Ultimate, may fail on Free tier
            self._logger.warning(
                f"Failed to fetch epics for group {group_path}: {e}. "
                "This may be because epics require GitLab Premium or Ultimate tier."
            )

    async def get_docs(self, filtering=None):
        """Main method to fetch documents from GitLab using Work Items API.

        Args:
            filtering (Filtering, optional): Filtering rules. Defaults to None.

        Yields:
            tuple: (document dict, download function or None)
        """
        seen_groups = set()

        async for project in self.gitlab_client.get_projects():
            project_id = self._validate_and_filter_project(project)
            if not project_id:
                continue

            yield self._format_project_doc(project), None

            async for doc in self._fetch_work_items_for_project(
                project, WorkItemType.ISSUE
            ):
                yield doc

            async for doc in self._fetch_merge_requests_for_project(project):
                yield doc

            async for doc in self._fetch_releases_for_project(project):
                yield doc

            if project.group:
                async for doc in self._fetch_epics_for_group(
                    project.group.full_path, seen_groups
                ):
                    yield doc

            async for readme_doc, download_func in self._fetch_readme_files(
                project_id, project
            ):
                yield readme_doc, download_func

    def _format_project_doc(self, project: GitLabProject) -> dict[str, Any]:
        """Format project data into Elasticsearch document.

        Args:
            project (GitLabProject): Validated project model

        Returns:
            dict: Formatted project document dict
        """
        project_id = self.gitlab_client._extract_numeric_id(project.id) or project.id

        return {
            "_id": f"project_{project_id}",
            "_timestamp": project.last_activity_at or project.created_at,
            "type": "Project",
            "id": str(project_id),
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
        project_id = self.gitlab_client._extract_numeric_id(project.id) or project.id

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
            "assignees": [a.username for a in issue.assignees.nodes],
            "labels": [label.title for label in issue.labels.nodes],
        }

    async def _extract_notes_from_discussions(
        self,
        discussions: PaginatedList[GitLabDiscussion],
        project_path: str,
        iid: int,
        issuable_type: str,
    ) -> list[dict[str, Any]]:
        """Extract and flatten notes from discussions structure, paginating if needed.

        Args:
            discussions: Paginated list of discussions
            project_path: Full path of the project
            iid: Issue or MR internal ID
            issuable_type: 'issue' or 'mergeRequest'

        Returns:
            list: Flattened list of notes
        """
        notes = []

        for discussion in discussions.nodes:
            discussion_id = discussion.id
            notes_list = discussion.notes

            for note in notes_list.nodes:
                note_dict = {
                    "id": note.id,
                    "body": note.body,
                    "created_at": note.created_at,
                    "updated_at": note.updated_at,
                    "system": note.system,
                    "author": note.author.username if note.author else None,
                    "author_name": note.author.name if note.author else None,
                }
                if position := note.position:
                    note_dict["position"] = position.model_dump()
                notes.append(note_dict)

            if notes_list.page_info.has_next_page and discussion_id:
                cursor = notes_list.page_info.end_cursor
                async for note_data in self.gitlab_client.fetch_remaining_notes(
                    project_path, iid, discussion_id, issuable_type, cursor
                ):
                    note_dict = {
                        "id": note_data.get("id"),
                        "body": note_data.get("body"),
                        "created_at": note_data.get("createdAt"),
                        "updated_at": note_data.get("updatedAt"),
                        "system": note_data.get("system", False),
                        "author": note_data.get("author", {}).get("username"),
                        "author_name": note_data.get("author", {}).get("name"),
                    }
                    if position := note_data.get("position"):
                        note_dict["position"] = {
                            "new_line": position.get("newLine"),
                            "old_line": position.get("oldLine"),
                            "new_path": position.get("newPath"),
                            "old_path": position.get("oldPath"),
                            "position_type": position.get("positionType"),
                        }
                    notes.append(note_dict)

        return notes

    def _format_merge_request_doc(
        self,
        mr: GitLabMergeRequest,
        project: GitLabProject,
        notes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Format merge request data into Elasticsearch document.

        Args:
            mr (GitLabMergeRequest): Validated merge request model
            project (GitLabProject): Parent project model
            notes: Pre-extracted notes from discussions or None

        Returns:
            dict: Formatted merge request document
        """
        project_id = self.gitlab_client._extract_numeric_id(project.id) or project.id

        return {
            "_id": f"mr_{project_id}_{mr.iid}",
            "_timestamp": mr.updated_at,
            "type": "Merge Request",
            "project_id": str(project_id),
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
            "assignees": [a.username for a in mr.assignees.nodes],
            "reviewers": [r.username for r in mr.reviewers.nodes],
            "approved_by": [a.username for a in mr.approved_by.nodes],
            "merged_by": mr.merged_by.username if mr.merged_by else None,
            "labels": [label.title for label in mr.labels.nodes],
            "notes": notes,
        }

    def _format_work_item_doc(
        self,
        work_item: GitLabWorkItem,
        project: GitLabProject | None = None,
        group_path: str | None = None,
        assignees_data: PaginatedList[GitLabUser] | None = None,
        labels_data: PaginatedList[GitLabLabel] | None = None,
        notes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Format work item data into Elasticsearch document.

        Args:
            work_item: Work item model (Issue, Task, Epic, etc.)
            project: Parent project (for Issues/MRs) or None (for Epics)
            group_path: Group path (for Epics) or None (for Issues/MRs)
            assignees_data: Pre-fetched assignees data (with pagination) or None to extract from widgets
            labels_data: Pre-fetched labels data (with pagination) or None to extract from widgets
            notes: Pre-extracted notes from discussions or None

        Returns:
            dict: Formatted work item document
        """
        description = self._extract_widget_description(work_item)

        if assignees_data is None:
            assignees_data = self._extract_widget_assignees(work_item)
        if labels_data is None:
            labels_data = self._extract_widget_labels(work_item)

        hierarchy = self._extract_widget_hierarchy(work_item)
        linked_items = self._extract_widget_linked_items(work_item)

        if project:
            parent_id = self.gitlab_client._extract_numeric_id(project.id) or project.id
            parent_path = project.full_path
            parent_type = "project"
        elif group_path:
            parent_id = group_path.replace("/", "_")
            parent_path = group_path
            parent_type = "group"
        else:
            parent_id = "unknown"
            parent_path = "unknown"
            parent_type = "unknown"

        kwargs = {
            "_id": f"{work_item.type_name.lower().replace(' ', '_')}_{parent_id}_{work_item.iid}",
            "_timestamp": work_item.updated_at,
            "type": work_item.type_name,
            "iid": work_item.iid,
            "title": work_item.title,
            "description": description,
            "state": work_item.state,
            "created_at": work_item.created_at,
            "updated_at": work_item.updated_at,
            "closed_at": work_item.closed_at,
            "web_url": work_item.web_url,
            "author": work_item.author.username if work_item.author else None,
            "author_name": work_item.author.name if work_item.author else None,
            "assignees": [a.username for a in assignees_data.nodes],
            "labels": [label.title for label in labels_data.nodes],
            "notes": notes,
        }

        # Add dynamic parent fields
        if parent_type == "project":
            kwargs["project_id"] = parent_id
            kwargs["project_path"] = parent_path
        elif parent_type == "group":
            kwargs["group_id"] = parent_id
            kwargs["group_path"] = parent_path

        # Add hierarchy info for Epics (parent/child relationships)
        if hierarchy:
            if hierarchy.parent:
                kwargs["parent_epic_iid"] = hierarchy.parent.iid
                kwargs["parent_epic_title"] = hierarchy.parent.title

            children = hierarchy.children.nodes
            kwargs["children_count"] = len(children)
            kwargs["children"] = [
                {
                    "id": child.id,
                    "iid": child.iid,
                    "title": child.title,
                }
                for child in children
            ]

        # Add linked items info (related/blocking items - separate from hierarchy)
        if linked_items:
            items = linked_items.linked_items.nodes
            kwargs["linked_items_count"] = len(items)
            kwargs["linked_items"] = [
                {
                    "link_id": item.link_id,
                    "link_type": item.link_type,
                    "link_created_at": item.link_created_at,
                    "link_updated_at": item.link_updated_at,
                    "work_item_id": item.work_item.id,
                    "work_item_iid": item.work_item.iid,
                    "work_item_title": item.work_item.title,
                }
                for item in items
            ]

        return kwargs

    def _format_release_doc(
        self, release: GitLabRelease, project: GitLabProject
    ) -> dict[str, Any]:
        """Format release data into Elasticsearch document.

        Args:
            release: Release model
            project: Parent project model

        Returns:
            dict: Formatted release document
        """
        project_id = self.gitlab_client._extract_numeric_id(project.id) or project.id
        milestone_titles = [m.title for m in release.milestones.nodes]
        asset_links = release.assets.links.nodes

        kwargs = {
            "_id": f"release_{project_id}_{release.tag_name}",
            "_timestamp": release.released_at or release.created_at,
            "type": "Release",
            "project_id": str(project_id),
            "project_path": project.full_path,
            "tag_name": release.tag_name,
            "name": release.name,
            "description": release.description,
            "created_at": release.created_at,
            "released_at": release.released_at,
            "author": release.author.username if release.author else None,
            "author_name": release.author.name if release.author else None,
            "milestones": milestone_titles,
            "asset_count": release.assets.count,
        }

        if release.commit:
            kwargs["commit_sha"] = release.commit.sha
            kwargs["commit_title"] = release.commit.title

        if asset_links:
            kwargs["asset_links"] = [
                {
                    "name": link.name,
                    "url": link.url,
                    "type": link.link_type,
                }
                for link in asset_links
            ]

        return kwargs

    async def _fetch_readme_files(
        self, project_id: int, project: GitLabProject
    ) -> AsyncGenerator[tuple[dict[str, Any], Any], None]:
        """Fetch README files from a project using REST API.

        Args:
            project_id: Numeric project ID
            project: Validated project model

        Yields:
            Tuple of (document dict, download function)
        """
        default_branch = project.default_branch
        if not default_branch:
            return

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
            if item.get("type") != "blob":
                continue

            file_name = item.get("name", "").lower()
            file_path = item.get("path", "")

            if not file_name.startswith("readme"):
                continue

            file_extension = ""
            if "." in file_name:
                file_extension = file_name[file_name.rfind(".") :]

            # Skip files with unsupported extensions, but allow files without extensions
            # (e.g., plain "README" files are allowed)
            if file_extension not in SUPPORTED_EXTENSION and file_extension != "":
                continue

            readme_doc: FileDocument = {
                "_id": f"file_{project_id}_{file_path}",
                "_timestamp": project.last_activity_at or project.created_at,
                "type": "File",
                "project_id": str(project_id),
                "project_path": project.full_path,
                "file_path": file_path,
                "file_name": item.get("name"),
                "extension": file_extension,
                "web_url": f"{project.web_url}/-/blob/{default_branch}/{file_path}",
            }

            file_metadata = {
                "project_id": project_id,
                "file_path": file_path,
                "file_name": item.get("name"),
                "ref": default_branch,
                "_timestamp": project.last_activity_at,
            }

            yield readme_doc, partial(self.get_content, attachment=file_metadata)

    async def get_content(self, attachment, timestamp=None, doit: bool = False):
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
