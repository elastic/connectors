#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Unit tests for GitLab connector."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock, patch

import pytest

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources.gitlab.client import GitLabClient
from connectors.sources.gitlab.datasource import GitLabDataSource
from connectors.sources.gitlab.models import (
    GitLabAssetLink,
    GitLabAssets,
    GitLabCommit,
    GitLabDiscussion,
    GitLabGroup,
    GitLabIssue,
    GitLabLabel,
    GitLabMergeRequest,
    GitLabMilestone,
    GitLabNote,
    GitLabProject,
    GitLabRelease,
    GitLabRepository,
    GitLabUser,
    GitLabWorkItem,
    PageInfo,
    PaginatedList,
    WorkItemType,
    WorkItemTypeInfo,
    WorkItemWidgetAssignees,
    WorkItemWidgetDescription,
)
from tests.sources.support import create_source


# Helper function to create GitLab source
@asynccontextmanager
async def create_gitlab_source(
    token="test-token-123",
    projects=None,
):
    """Create a GitLab source with test configuration."""
    if projects is None:
        projects = ["group/project1", "group/project2"]
    async with create_source(
        GitLabDataSource,
        token=token,
        projects=projects,
    ) as source:
        yield source


# Fixtures for test data
@pytest.fixture
def mock_configuration():
    """Mock configuration for GitLab data source."""
    config = GitLabDataSource.get_default_configuration()
    config["token"]["value"] = "test-token-123"
    config["projects"]["value"] = ["group/project1", "group/project2"]
    return DataSourceConfiguration(config)


@pytest.fixture
def mock_configuration_wildcard():
    """Mock configuration with wildcard projects."""
    config = GitLabDataSource.get_default_configuration()
    config["token"]["value"] = "test-token-123"
    config["projects"]["value"] = ["*"]
    return DataSourceConfiguration(config)


@pytest.fixture
def mock_configuration_empty_projects():
    """Mock configuration with empty projects list."""
    config = GitLabDataSource.get_default_configuration()
    config["token"]["value"] = "test-token-123"
    config["projects"]["value"] = []
    return DataSourceConfiguration(config)


@pytest.fixture
def mock_gitlab_user():
    """Mock GitLab user."""
    return GitLabUser(username="testuser", name="Test User")


@pytest.fixture
def mock_gitlab_project():
    """Mock GitLab project."""
    return GitLabProject(
        id="gid://gitlab/Project/123",
        name="Test Project",
        path="test-project",
        full_path="group/test-project",
        description="Test project description",
        visibility="public",
        star_count=10,
        forks_count=5,
        created_at="2023-01-01T00:00:00Z",
        last_activity_at="2023-12-01T00:00:00Z",
        archived=False,
        web_url="https://gitlab.com/group/test-project",
        repository=GitLabRepository(root_ref="main"),
        group=GitLabGroup(id="gid://gitlab/Group/456", full_path="group"),
    )


@pytest.fixture
def mock_gitlab_work_item():
    """Mock GitLab work item (issue)."""
    return GitLabWorkItem.model_validate(
        {
            "id": "gid://gitlab/WorkItem/789",
            "iid": 1,
            "title": "Test Issue",
            "state": "opened",
            "createdAt": "2023-01-01T00:00:00Z",
            "updatedAt": "2023-12-01T00:00:00Z",
            "closedAt": None,
            "webUrl": "https://gitlab.com/group/project/issues/1",
            "author": {"username": "testuser", "name": "Test User"},
            "workItemType": {"name": "Issue"},
            "widgets": [
                {
                    "__typename": "WorkItemWidgetDescription",
                    "description": "Issue description",
                },
                {
                    "__typename": "WorkItemWidgetAssignees",
                    "assignees": {
                        "nodes": [{"username": "assignee1", "name": "Assignee One"}],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                },
                {
                    "__typename": "WorkItemWidgetLabels",
                    "labels": {
                        "nodes": [{"title": "bug"}],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                },
                {
                    "__typename": "WorkItemWidgetNotes",
                    "discussions": {
                        "nodes": [],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                },
            ],
        }
    )


@pytest.fixture
def mock_gitlab_merge_request(mock_gitlab_user):
    """Mock GitLab merge request."""
    return GitLabMergeRequest(
        id="gid://gitlab/MergeRequest/999",
        iid=1,
        title="Test MR",
        description="MR description",
        state="opened",
        web_url="https://gitlab.com/group/project/merge_requests/1",
        created_at="2023-01-01T00:00:00Z",
        updated_at="2023-12-01T00:00:00Z",
        merged_at=None,
        closed_at=None,
        source_branch="feature",
        target_branch="main",
        author=mock_gitlab_user,
        assignees=PaginatedList(nodes=[], page_info=PageInfo(has_next_page=False)),
        reviewers=PaginatedList(nodes=[], page_info=PageInfo(has_next_page=False)),
        labels=PaginatedList(nodes=[], page_info=PageInfo(has_next_page=False)),
        discussions=PaginatedList(nodes=[], page_info=PageInfo(has_next_page=False)),
        approved_by=PaginatedList(nodes=[], page_info=PageInfo(has_next_page=False)),
        merged_by=None,
    )


@pytest.fixture
def mock_gitlab_release(mock_gitlab_user):
    """Mock GitLab release."""
    return GitLabRelease(
        tag_name="v1.0.0",
        name="Release 1.0.0",
        description="Release description",
        created_at="2023-01-01T00:00:00Z",
        released_at="2023-01-01T00:00:00Z",
        author=mock_gitlab_user,
        commit=GitLabCommit(
            sha="abc123", title="Release commit", message="Release commit message"
        ),
        milestones=PaginatedList(
            nodes=[GitLabMilestone(id="gid://gitlab/Milestone/1", title="v1.0")],
            page_info=PageInfo(has_next_page=False),
        ),
        assets=GitLabAssets(
            count=1,
            links=PaginatedList(
                nodes=[
                    GitLabAssetLink(
                        name="binary",
                        url="https://example.com/binary",
                        link_type="other",
                    )
                ],
                page_info=PageInfo(has_next_page=False),
            ),
        ),
    )


# Tests for GitLabDataSource
class TestGitLabDataSource:
    """Test suite for GitLabDataSource class."""

    def test_get_default_configuration(self):
        """Test that default configuration is returned correctly."""
        config = GitLabDataSource.get_default_configuration()

        assert "token" in config
        assert config["token"]["label"] == "Personal Access Token"
        assert config["token"]["type"] == "str"
        assert config["token"]["sensitive"] is True

        assert "projects" in config
        assert config["projects"]["type"] == "list"
        assert config["projects"]["value"] == []

    def test_init(self, mock_configuration):
        """Test GitLabDataSource initialization."""
        source = GitLabDataSource(configuration=mock_configuration)

        # Verify configured_projects is set
        assert source.configured_projects == ["group/project1", "group/project2"]
        # Verify client was initialized with token
        assert source.gitlab_client.token == "test-token-123"

    @pytest.mark.asyncio
    async def test_ping_success(self, mock_configuration):
        """Test successful ping."""
        source = GitLabDataSource(configuration=mock_configuration)
        source.gitlab_client.ping = AsyncMock()

        await source.ping()

        source.gitlab_client.ping.assert_called_once()

    @pytest.mark.asyncio
    async def test_ping_failure(self, mock_configuration):
        """Test ping failure raises exception."""
        source = GitLabDataSource(configuration=mock_configuration)
        source.gitlab_client.ping = AsyncMock(
            side_effect=Exception("Connection failed")
        )

        with pytest.raises(Exception, match="Connection failed"):
            await source.ping()

    @pytest.mark.asyncio
    async def test_close(self, mock_configuration):
        """Test close method calls client.close()."""
        source = GitLabDataSource(configuration=mock_configuration)
        source.gitlab_client.close = AsyncMock()

        await source.close()

        source.gitlab_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_validate_config_success(self, mock_configuration):
        """Test successful configuration validation."""
        source = GitLabDataSource(configuration=mock_configuration)
        source.gitlab_client.ping = AsyncMock()
        source.gitlab_client._execute_graphql = AsyncMock(
            return_value={
                "projects": {
                    "nodes": [
                        {"fullPath": "group/project1"},
                        {"fullPath": "group/project2"},
                    ]
                }
            }
        )

        await source.validate_config()

        # Should call ping
        source.gitlab_client.ping.assert_called_once()

    @pytest.mark.asyncio
    async def test_validate_config_ping_failure(self, mock_configuration):
        """Test validation fails when ping fails."""
        source = GitLabDataSource(configuration=mock_configuration)
        source.gitlab_client.ping = AsyncMock(side_effect=Exception("Auth failed"))

        with pytest.raises(ConfigurableFieldValueError, match="Failed to authenticate"):
            await source.validate_config()

    @pytest.mark.asyncio
    async def test_validate_config_wildcard_projects(self, mock_configuration_wildcard):
        """Test validation with wildcard projects skips project validation."""
        source = GitLabDataSource(configuration=mock_configuration_wildcard)
        source.gitlab_client.ping = AsyncMock()
        source.gitlab_client._execute_graphql = AsyncMock()

        await source.validate_config()

        # Should call ping but not _execute_graphql for project validation
        source.gitlab_client.ping.assert_called_once()
        source.gitlab_client._execute_graphql.assert_not_called()

    @pytest.mark.asyncio
    async def test_validate_config_empty_projects(
        self, mock_configuration_empty_projects
    ):
        """Test validation fails with empty projects list."""
        source = GitLabDataSource(configuration=mock_configuration_empty_projects)
        source.gitlab_client.ping = AsyncMock()

        # Empty list should fail validation
        with pytest.raises(ConfigurableFieldValueError, match="cannot be empty"):
            await source.validate_config()

    @pytest.mark.asyncio
    async def test_validate_configured_projects_inaccessible(self, mock_configuration):
        """Test validation fails when configured projects are not accessible."""
        source = GitLabDataSource(configuration=mock_configuration)
        source.gitlab_client._execute_graphql = AsyncMock(
            return_value={
                "projects": {
                    "nodes": [
                        {"fullPath": "group/project1"},
                        # project2 is missing - not accessible
                    ]
                }
            }
        )

        with pytest.raises(ConfigurableFieldValueError, match="not accessible"):
            await source._validate_configured_projects()

    @pytest.mark.asyncio
    async def test_validate_configured_projects_batching(self):
        """Test project validation handles batching (max 50 per query)."""
        # Create config with 60 projects (requires 2 batches)
        projects = [f"group/project{i}" for i in range(60)]
        config = GitLabDataSource.get_default_configuration()
        config["token"]["value"] = "test-token-123"
        config["projects"]["value"] = projects
        source = GitLabDataSource(configuration=DataSourceConfiguration(config))

        # Mock responses for two batches - must return all requested projects
        source.gitlab_client._execute_graphql = AsyncMock(
            side_effect=[
                {
                    "projects": {
                        "nodes": [{"fullPath": f"group/project{i}"} for i in range(50)]
                    }
                },
                {
                    "projects": {
                        "nodes": [
                            {"fullPath": f"group/project{i}"} for i in range(50, 60)
                        ]
                    }
                },
            ]
        )

        await source._validate_configured_projects()

        # Should be called twice (2 batches)
        assert source.gitlab_client._execute_graphql.call_count == 2

    def test_should_sync_project_wildcard(self, mock_configuration_wildcard):
        """Test _should_sync_project returns True for wildcard."""
        source = GitLabDataSource(configuration=mock_configuration_wildcard)

        assert source._should_sync_project("any/project") is True

    def test_should_sync_project_empty(self, mock_configuration_empty_projects):
        """Test _should_sync_project returns True for empty list (syncs all when no projects configured)."""
        source = GitLabDataSource(configuration=mock_configuration_empty_projects)

        # Empty list evaluates to False, so "not self.configured_projects" is True -> sync all
        assert source._should_sync_project("any/project") is True

    def test_should_sync_project_configured(self, mock_configuration):
        """Test _should_sync_project returns True for configured projects."""
        source = GitLabDataSource(configuration=mock_configuration)

        assert source._should_sync_project("group/project1") is True
        assert source._should_sync_project("group/project2") is True
        assert source._should_sync_project("other/project") is False

    def test_extract_widget_description(
        self, mock_configuration, mock_gitlab_work_item
    ):
        """Test extracting description from work item widgets."""
        source = GitLabDataSource(configuration=mock_configuration)

        description = source._extract_widget_description(mock_gitlab_work_item)

        assert description == "Issue description"

    def test_extract_widget_assignees(self, mock_configuration, mock_gitlab_work_item):
        """Test extracting assignees from work item widgets."""
        source = GitLabDataSource(configuration=mock_configuration)

        assignees = source._extract_widget_assignees(mock_gitlab_work_item)

        assert len(assignees.nodes) == 1
        assert assignees.nodes[0].username == "assignee1"

    def test_extract_widget_labels(self, mock_configuration, mock_gitlab_work_item):
        """Test extracting labels from work item widgets."""
        source = GitLabDataSource(configuration=mock_configuration)

        labels = source._extract_widget_labels(mock_gitlab_work_item)

        assert len(labels.nodes) == 1
        assert labels.nodes[0].title == "bug"

    def test_extract_widget_discussions(
        self, mock_configuration, mock_gitlab_work_item
    ):
        """Test extracting discussions from work item widgets."""
        source = GitLabDataSource(configuration=mock_configuration)

        discussions = source._extract_widget_discussions(mock_gitlab_work_item)

        assert len(discussions.nodes) == 0
        assert discussions.page_info.has_next_page is False

    def test_format_project_doc(self, mock_configuration, mock_gitlab_project):
        """Test formatting project document."""
        source = GitLabDataSource(configuration=mock_configuration)

        doc = source._format_project_doc(mock_gitlab_project)

        assert doc["_id"] == "project_123"
        assert doc["type"] == "Project"
        assert doc["name"] == "Test Project"
        assert doc["full_path"] == "group/test-project"
        assert doc["visibility"] == "public"
        assert doc["star_count"] == 10
        assert doc["default_branch"] == "main"

    def test_format_work_item_doc_project_level(
        self, mock_configuration, mock_gitlab_work_item, mock_gitlab_project
    ):
        """Test formatting work item document for project-level item."""
        source = GitLabDataSource(configuration=mock_configuration)

        doc = source._format_work_item_doc(
            mock_gitlab_work_item,
            project=mock_gitlab_project,
            assignees_data=source._extract_widget_assignees(mock_gitlab_work_item),
            labels_data=source._extract_widget_labels(mock_gitlab_work_item),
        )

        assert doc["_id"] == "issue_123_1"
        assert doc["type"] == "Issue"
        assert doc["project_id"] == "123"
        assert doc["project_path"] == "group/test-project"
        assert doc["title"] == "Test Issue"
        assert doc["description"] == "Issue description"
        assert doc["assignees"] == ["assignee1"]
        assert doc["labels"] == ["bug"]

    def test_format_merge_request_doc(
        self, mock_configuration, mock_gitlab_merge_request, mock_gitlab_project
    ):
        """Test formatting merge request document."""
        source = GitLabDataSource(configuration=mock_configuration)

        doc = source._format_merge_request_doc(
            mock_gitlab_merge_request, mock_gitlab_project
        )

        assert doc["_id"] == "mr_123_1"
        assert doc["type"] == "Merge Request"
        assert doc["title"] == "Test MR"
        assert doc["source_branch"] == "feature"
        assert doc["target_branch"] == "main"
        assert doc["author"] == "testuser"

    def test_format_release_doc(
        self, mock_configuration, mock_gitlab_release, mock_gitlab_project
    ):
        """Test formatting release document."""
        source = GitLabDataSource(configuration=mock_configuration)

        doc = source._format_release_doc(mock_gitlab_release, mock_gitlab_project)

        assert doc["_id"] == "release_123_v1.0.0"
        assert doc["type"] == "Release"
        assert doc["tag_name"] == "v1.0.0"
        assert doc["name"] == "Release 1.0.0"
        assert doc["milestones"] == ["v1.0"]
        assert doc["commit_sha"] == "abc123"

    @pytest.mark.asyncio
    async def test_get_content_doit_false(self, mock_configuration):
        """Test get_content returns None when doit is False."""
        source = GitLabDataSource(configuration=mock_configuration)

        result = await source.get_content(
            attachment={
                "project_id": 123,
                "file_path": "README.md",
                "file_name": "README.md",
            },
            doit=False,
        )

        assert result is None


# Tests for GitLabClient
class TestGitLabClient:
    """Test suite for GitLabClient class."""

    def test_init(self):
        """Test GitLabClient initialization."""
        client = GitLabClient(token="test-token")

        assert client.token == "test-token"
        assert "gitlab.com" in client.api_url
        assert "gitlab.com" in client.graphql_url
        assert client._session is None

    @pytest.mark.asyncio
    async def test_get_session_creates_new(self):
        """Test _get_session creates new session when None."""
        client = GitLabClient(token="test-token")

        session = client._get_session()

        assert session is not None
        assert client._session is not None

    @pytest.mark.asyncio
    async def test_handle_rate_limit_429(self):
        """Test _handle_rate_limit sleeps on 429 status."""
        client = GitLabClient(token="test-token")

        mock_response = Mock()
        mock_response.status = 429
        mock_response.headers = {"Retry-After": "1"}

        # Mock asyncio.sleep to avoid actual waiting
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await client._handle_rate_limit(mock_response)

            assert result is True
            mock_sleep.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_rate_limit_non_429(self):
        """Test _handle_rate_limit returns False for non-429 status."""
        client = GitLabClient(token="test-token")

        mock_response = Mock()
        mock_response.status = 200

        result = await client._handle_rate_limit(mock_response)

        assert result is False

    def test_set_logger(self):
        """Test setting logger."""
        client = GitLabClient(token="test-token")
        mock_logger = Mock()

        client.set_logger(mock_logger)

        assert client._logger == mock_logger

    def test_extract_id_valid(self):
        """Test extracting ID from valid global ID."""
        client = GitLabClient(token="test-token")

        id_str = client._extract_id("gid://gitlab/Project/123")

        assert id_str == "123"

    def test_extract_id_invalid(self):
        """Test extracting ID from invalid format still returns last segment."""
        client = GitLabClient(token="test-token")

        id_str = client._extract_id("invalid-id")

        # Even invalid format returns the last segment
        assert id_str == "invalid-id"

    @pytest.mark.asyncio
    async def test_close(self):
        """Test close method."""
        client = GitLabClient(token="test-token")
        mock_session = AsyncMock()
        mock_session.closed = False
        client._session = mock_session

        await client.close()

        mock_session.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_ping_success(self):
        """Test successful ping."""
        client = GitLabClient(token="test-token")

        with patch.object(client, "_get_rest", new_callable=AsyncMock) as mock_get_rest:
            mock_get_rest.return_value = {"id": 1, "username": "test"}

            await client.ping()

            mock_get_rest.assert_called_once_with("user")

    @pytest.mark.asyncio
    async def test_ping_failure(self):
        """Test ping failure raises ConfigurableFieldValueError."""
        client = GitLabClient(token="test-token")

        with patch.object(client, "_get_rest", new_callable=AsyncMock) as mock_get_rest:
            mock_get_rest.side_effect = Exception("Connection failed")

            with pytest.raises(ConfigurableFieldValueError, match="Failed to connect"):
                await client.ping()


# Tests for Pydantic Models
class TestModels:
    """Test suite for Pydantic models."""

    def test_page_info_validation(self):
        """Test PageInfo model validation."""
        page_info = PageInfo(has_next_page=True, end_cursor="cursor123")

        assert page_info.has_next_page is True
        assert page_info.end_cursor == "cursor123"

    def test_paginated_list_defaults(self):
        """Test PaginatedList defaults."""
        paginated = PaginatedList[GitLabUser]()

        assert paginated.nodes == []
        assert paginated.page_info.has_next_page is False

    def test_gitlab_user_validation(self):
        """Test GitLabUser validation."""
        user = GitLabUser(username="testuser", name="Test User")

        assert user.username == "testuser"
        assert user.name == "Test User"

    def test_gitlab_user_optional_name(self):
        """Test GitLabUser with optional name."""
        user = GitLabUser(username="testuser")

        assert user.username == "testuser"
        assert user.name is None

    def test_gitlab_project_default_branch(self):
        """Test GitLabProject default_branch property."""
        project = GitLabProject(
            id="gid://gitlab/Project/1",
            name="Test",
            path="test",
            full_path="group/test",
            visibility="public",
            star_count=0,
            forks_count=0,
            created_at="2023-01-01T00:00:00Z",
            web_url="https://gitlab.com/test",
            repository=GitLabRepository(root_ref="main"),
        )

        assert project.default_branch == "main"

    def test_gitlab_project_no_repository(self):
        """Test GitLabProject with no repository."""
        project = GitLabProject(
            id="gid://gitlab/Project/1",
            name="Test",
            path="test",
            full_path="group/test",
            visibility="public",
            star_count=0,
            forks_count=0,
            created_at="2023-01-01T00:00:00Z",
            web_url="https://gitlab.com/test",
        )

        assert project.default_branch is None

    def test_work_item_type_name(self):
        """Test GitLabWorkItem type_name property."""
        work_item = GitLabWorkItem(
            id="gid://gitlab/WorkItem/1",
            iid=1,
            title="Test",
            state="opened",
            created_at="2023-01-01T00:00:00Z",
            updated_at="2023-01-01T00:00:00Z",
            web_url="https://gitlab.com/test",
            author=GitLabUser(username="test"),
            work_item_type=WorkItemTypeInfo(name="Issue"),
            widgets=[],
        )

        assert work_item.type_name == "Issue"

    def test_widget_discriminator_description(self):
        """Test widget discriminator for description widget."""
        widget_data = {
            "__typename": "WorkItemWidgetDescription",
            "description": "Test description",
        }

        widget = WorkItemWidgetDescription.model_validate(widget_data)

        assert isinstance(widget, WorkItemWidgetDescription)
        assert widget.description == "Test description"

    def test_widget_discriminator_assignees(self):
        """Test widget discriminator for assignees widget."""
        widget_data = {
            "__typename": "WorkItemWidgetAssignees",
            "assignees": {
                "nodes": [{"username": "test", "name": "Test"}],
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            },
        }

        widget = WorkItemWidgetAssignees.model_validate(widget_data)

        assert isinstance(widget, WorkItemWidgetAssignees)
        assert len(widget.assignees.nodes) == 1


# Additional tests for GitLabClient async generators
class TestGitLabClientAsyncGenerators:
    """Test suite for GitLabClient async generator methods."""

    @pytest.mark.asyncio
    async def test_get_projects(self):
        """Test get_projects fetches and yields projects with pagination."""
        client = GitLabClient(token="test-token")

        # Mock GraphQL responses for 2 pages
        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.side_effect = [
                {
                    "projects": {
                        "nodes": [
                            {
                                "id": "gid://gitlab/Project/1",
                                "name": "Project 1",
                                "path": "project1",
                                "fullPath": "group/project1",
                                "visibility": "public",
                                "starCount": 10,
                                "forksCount": 5,
                                "createdAt": "2023-01-01T00:00:00Z",
                                "webUrl": "https://gitlab.com/group/project1",
                            }
                        ],
                        "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"},
                    }
                },
                {
                    "projects": {
                        "nodes": [
                            {
                                "id": "gid://gitlab/Project/2",
                                "name": "Project 2",
                                "path": "project2",
                                "fullPath": "group/project2",
                                "visibility": "private",
                                "starCount": 20,
                                "forksCount": 10,
                                "createdAt": "2023-01-02T00:00:00Z",
                                "webUrl": "https://gitlab.com/group/project2",
                            }
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                },
            ]

            projects = []
            async for project in client.get_projects():
                projects.append(project)

            assert len(projects) == 2
            assert projects[0].name == "Project 1"
            assert projects[1].name == "Project 2"
            assert mock_graphql.call_count == 2

    @pytest.mark.asyncio
    async def test_get_projects_handles_errors(self):
        """Test get_projects handles GraphQL errors gracefully."""
        client = GitLabClient(token="test-token")

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with patch.object(
                client, "_execute_graphql", new_callable=AsyncMock
            ) as mock_graphql:
                mock_graphql.side_effect = Exception("GraphQL error")

                with pytest.raises(Exception, match="GraphQL error"):
                    async for _ in client.get_projects():
                        pass

    @pytest.mark.asyncio
    async def test_get_merge_requests(self):
        """Test get_merge_requests fetches MRs for a project."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {
                "project": {
                    "mergeRequests": {
                        "nodes": [
                            {
                                "iid": 1,
                                "title": "Test MR",
                                "state": "opened",
                                "createdAt": "2023-01-01T00:00:00Z",
                                "updatedAt": "2023-01-01T00:00:00Z",
                                "webUrl": "https://gitlab.com/group/project/merge_requests/1",
                                "sourceBranch": "feature",
                                "targetBranch": "main",
                                "assignees": {
                                    "nodes": [],
                                    "pageInfo": {"hasNextPage": False},
                                },
                                "reviewers": {
                                    "nodes": [],
                                    "pageInfo": {"hasNextPage": False},
                                },
                                "labels": {
                                    "nodes": [],
                                    "pageInfo": {"hasNextPage": False},
                                },
                                "discussions": {
                                    "nodes": [],
                                    "pageInfo": {"hasNextPage": False},
                                },
                                "approvedBy": {
                                    "nodes": [],
                                    "pageInfo": {"hasNextPage": False},
                                },
                            }
                        ],
                        "pageInfo": {"hasNextPage": False},
                    }
                }
            }

            mrs = []
            async for mr in client.get_merge_requests("group/project"):
                mrs.append(mr)

            assert len(mrs) == 1
            assert mrs[0].title == "Test MR"

    @pytest.mark.asyncio
    async def test_get_merge_requests_missing_project(self):
        """Test get_merge_requests handles missing project."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {}

            mrs = []
            async for mr in client.get_merge_requests("nonexistent/project"):
                mrs.append(mr)

            assert len(mrs) == 0

    @pytest.mark.asyncio
    async def test_get_work_items_project(self):
        """Test get_work_items_project fetches work items."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {
                "project": {
                    "workItems": {
                        "nodes": [
                            {
                                "id": "gid://gitlab/WorkItem/1",
                                "iid": 1,
                                "title": "Test Issue",
                                "state": "opened",
                                "createdAt": "2023-01-01T00:00:00Z",
                                "updatedAt": "2023-01-01T00:00:00Z",
                                "webUrl": "https://gitlab.com/group/project/issues/1",
                                "workItemType": {"name": "Issue"},
                                "widgets": [],
                            }
                        ],
                        "pageInfo": {"hasNextPage": False},
                    }
                }
            }

            work_items = []
            async for work_item in client.get_work_items_project(
                "group/project", [WorkItemType.ISSUE]
            ):
                work_items.append(work_item)

            assert len(work_items) == 1
            assert work_items[0].title == "Test Issue"

    @pytest.mark.asyncio
    async def test_get_releases(self):
        """Test get_releases fetches releases for a project."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {
                "project": {
                    "releases": {
                        "nodes": [
                            {
                                "tagName": "v1.0.0",
                                "name": "Release 1.0",
                                "createdAt": "2023-01-01T00:00:00Z",
                                "milestones": {"nodes": []},
                                "assets": {"count": 0, "links": {"nodes": []}},
                            }
                        ],
                        "pageInfo": {"hasNextPage": False},
                    }
                }
            }

            releases = []
            async for release in client.get_releases("group/project"):
                releases.append(release)

            assert len(releases) == 1
            assert releases[0].tag_name == "v1.0.0"

    @pytest.mark.asyncio
    async def test_get_file_content(self):
        """Test get_file_content retrieves file data."""
        client = GitLabClient(token="test-token")

        with patch.object(client, "_get_rest", new_callable=AsyncMock) as mock_rest:
            mock_rest.return_value = {
                "file_name": "README.md",
                "content": "SGVsbG8gV29ybGQ=",  # "Hello World" in base64
                "encoding": "base64",
            }

            file_data = await client.get_file_content(123, "README.md", "main")

            assert file_data is not None
            assert file_data["file_name"] == "README.md"
            assert file_data["content"] == "SGVsbG8gV29ybGQ="
            mock_rest.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_file_content_error(self):
        """Test get_file_content handles errors."""
        client = GitLabClient(token="test-token")

        with patch.object(client, "_get_rest", new_callable=AsyncMock) as mock_rest:
            mock_rest.side_effect = Exception("File not found")

            file_data = await client.get_file_content(123, "nonexistent.md", "main")

            assert file_data is None

    @pytest.mark.asyncio
    async def test_fetch_remaining_field_assignees(self):
        """Test fetch_remaining_field for assignees."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.side_effect = [
                {
                    "project": {
                        "issue": {
                            "assignees": {
                                "nodes": [{"username": "user1", "name": "User 1"}],
                                "pageInfo": {
                                    "hasNextPage": True,
                                    "endCursor": "cursor2",
                                },
                            }
                        }
                    }
                },
                {
                    "project": {
                        "issue": {
                            "assignees": {
                                "nodes": [{"username": "user2", "name": "User 2"}],
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                            }
                        }
                    }
                },
            ]

            items = []
            async for item in client.fetch_remaining_field(
                "group/project", 1, "assignees", "issue", "cursor1"
            ):
                items.append(item)

            assert len(items) == 2
            assert items[0]["username"] == "user1"
            assert items[1]["username"] == "user2"

    @pytest.mark.asyncio
    async def test_fetch_remaining_field_unknown_type(self):
        """Test fetch_remaining_field with unknown field type."""
        client = GitLabClient(token="test-token")

        items = []
        async for item in client.fetch_remaining_field(
            "group/project", 1, "unknown_field", "issue", "cursor1"
        ):
            items.append(item)

        assert len(items) == 0

    @pytest.mark.asyncio
    async def test_fetch_remaining_notes(self):
        """Test fetch_remaining_notes for discussion."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            # First page with notes
            mock_graphql.side_effect = [
                {
                    "project": {
                        "issue": {
                            "discussions": {
                                "nodes": [
                                    {
                                        "notes": {
                                            "nodes": [
                                                {
                                                    "id": "note1",
                                                    "body": "Note 1",
                                                    "createdAt": "2023-01-01T00:00:00Z",
                                                    "updatedAt": "2023-01-01T00:00:00Z",
                                                    "system": False,
                                                }
                                            ],
                                            "pageInfo": {
                                                "hasNextPage": True,
                                                "endCursor": "cursor2",
                                            },
                                        }
                                    }
                                ]
                            }
                        }
                    }
                },
                # Second page
                {
                    "project": {
                        "issue": {
                            "discussions": {
                                "nodes": [
                                    {
                                        "notes": {
                                            "nodes": [
                                                {
                                                    "id": "note2",
                                                    "body": "Note 2",
                                                    "createdAt": "2023-01-02T00:00:00Z",
                                                    "updatedAt": "2023-01-02T00:00:00Z",
                                                    "system": False,
                                                }
                                            ],
                                            "pageInfo": {
                                                "hasNextPage": False,
                                                "endCursor": None,
                                            },
                                        }
                                    }
                                ]
                            }
                        }
                    }
                },
            ]

            notes = []
            async for note in client.fetch_remaining_notes(
                "group/project", 1, "discussion123", "issue", "cursor1"
            ):
                notes.append(note)

            assert len(notes) == 2
            assert notes[0]["body"] == "Note 1"
            assert notes[1]["body"] == "Note 2"

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_assignees(self):
        """Test fetch_remaining_work_item_assignees."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.side_effect = [
                {
                    "project": {
                        "workItems": {
                            "nodes": [
                                {
                                    "widgets": [
                                        {
                                            "__typename": "WorkItemWidgetAssignees",
                                            "assignees": {
                                                "nodes": [
                                                    {
                                                        "username": "user1",
                                                        "name": "User 1",
                                                    }
                                                ],
                                                "pageInfo": {"hasNextPage": False},
                                            },
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
            ]

            assignees = []
            async for assignee in client.fetch_remaining_work_item_assignees(
                "group/project", 1, "ISSUE", "cursor1"
            ):
                assignees.append(assignee)

            assert len(assignees) == 1
            assert assignees[0]["username"] == "user1"

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_labels(self):
        """Test fetch_remaining_work_item_labels."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {
                "project": {
                    "workItems": {
                        "nodes": [
                            {
                                "widgets": [
                                    {
                                        "__typename": "WorkItemWidgetLabels",
                                        "labels": {
                                            "nodes": [{"title": "bug"}],
                                            "pageInfo": {"hasNextPage": False},
                                        },
                                    }
                                ]
                            }
                        ]
                    }
                }
            }

            labels = []
            async for label in client.fetch_remaining_work_item_labels(
                "group/project", 1, "ISSUE", "cursor1"
            ):
                labels.append(label)

            assert len(labels) == 1
            assert labels[0]["title"] == "bug"

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_discussions(self):
        """Test fetch_remaining_work_item_discussions."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {
                "project": {
                    "workItems": {
                        "nodes": [
                            {
                                "widgets": [
                                    {
                                        "__typename": "WorkItemWidgetNotes",
                                        "discussions": {
                                            "nodes": [{"id": "disc1"}],
                                            "pageInfo": {"hasNextPage": False},
                                        },
                                    }
                                ]
                            }
                        ]
                    }
                }
            }

            discussions = []
            async for discussion in client.fetch_remaining_work_item_discussions(
                "group/project", 1, "ISSUE", "cursor1"
            ):
                discussions.append(discussion)

            assert len(discussions) == 1
            assert discussions[0]["id"] == "disc1"

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_group_discussions(self):
        """Test fetch_remaining_work_item_group_discussions."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {
                "group": {
                    "workItems": {
                        "nodes": [
                            {
                                "widgets": [
                                    {
                                        "__typename": "WorkItemWidgetNotes",
                                        "discussions": {
                                            "nodes": [{"id": "disc1"}],
                                            "pageInfo": {"hasNextPage": False},
                                        },
                                    }
                                ]
                            }
                        ]
                    }
                }
            }

            discussions = []
            async for discussion in client.fetch_remaining_work_item_group_discussions(
                "group", 1, "EPIC", "cursor1"
            ):
                discussions.append(discussion)

            assert len(discussions) == 1
            assert discussions[0]["id"] == "disc1"

    @pytest.mark.asyncio
    async def test_get_work_items_group(self):
        """Test get_work_items_group for epics."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {
                "group": {
                    "workItems": {
                        "nodes": [
                            {
                                "id": "gid://gitlab/WorkItem/1",
                                "iid": 1,
                                "title": "Test Epic",
                                "state": "opened",
                                "createdAt": "2023-01-01T00:00:00Z",
                                "updatedAt": "2023-01-01T00:00:00Z",
                                "webUrl": "https://gitlab.com/group/epics/1",
                                "workItemType": {"name": "Epic"},
                                "widgets": [],
                            }
                        ],
                        "pageInfo": {"hasNextPage": False},
                    }
                }
            }

            epics = []
            async for epic in client.get_work_items_group("group", [WorkItemType.EPIC]):
                epics.append(epic)

            assert len(epics) == 1
            assert epics[0].title == "Test Epic"


# Tests for GitLabDataSource integration methods
class TestGitLabDataSourceIntegration:
    """Test suite for GitLabDataSource integration methods."""

    @pytest.mark.asyncio
    async def test_extract_notes_from_discussions(self, mock_configuration):
        """Test _extract_notes_from_discussions flattens notes."""
        source = GitLabDataSource(configuration=mock_configuration)

        discussions = PaginatedList[GitLabDiscussion](
            nodes=[
                GitLabDiscussion(
                    id="disc1",
                    notes=PaginatedList[GitLabNote](
                        nodes=[
                            GitLabNote(
                                id="note1",
                                body="Note body",
                                created_at="2023-01-01T00:00:00Z",
                                updated_at="2023-01-01T00:00:00Z",
                                system=False,
                            )
                        ],
                        page_info=PageInfo(has_next_page=False),
                    ),
                )
            ],
            page_info=PageInfo(has_next_page=False),
        )

        notes = await source._extract_notes_from_discussions(
            discussions, "group/project", 1, "issue"
        )

        assert len(notes) == 1
        assert notes[0]["body"] == "Note body"
        assert notes[0]["id"] == "note1"

    @pytest.mark.asyncio
    async def test_extract_notes_from_discussions_with_pagination(
        self, mock_configuration
    ):
        """Test _extract_notes_from_discussions handles paginated notes."""
        source = GitLabDataSource(configuration=mock_configuration)

        discussions = PaginatedList[GitLabDiscussion](
            nodes=[
                GitLabDiscussion(
                    id="disc1",
                    notes=PaginatedList[GitLabNote](
                        nodes=[
                            GitLabNote(
                                id="note1",
                                body="Note 1",
                                created_at="2023-01-01T00:00:00Z",
                                updated_at="2023-01-01T00:00:00Z",
                                system=False,
                            )
                        ],
                        page_info=PageInfo(has_next_page=True, end_cursor="cursor1"),
                    ),
                )
            ],
            page_info=PageInfo(has_next_page=False),
        )

        # Mock fetch_remaining_notes
        async def mock_remaining_notes(*args, **kwargs):
            yield {
                "id": "note2",
                "body": "Note 2",
                "createdAt": "2023-01-02T00:00:00Z",
                "updatedAt": "2023-01-02T00:00:00Z",
                "system": False,
            }

        source.gitlab_client.fetch_remaining_notes = mock_remaining_notes

        notes = await source._extract_notes_from_discussions(
            discussions, "group/project", 1, "issue"
        )

        assert len(notes) == 2
        assert notes[0]["body"] == "Note 1"
        assert notes[1]["body"] == "Note 2"

    @pytest.mark.asyncio
    async def test_fetch_readme_files(self, mock_configuration, mock_gitlab_project):
        """Test _fetch_readme_files yields README documents."""
        source = GitLabDataSource(configuration=mock_configuration)

        # Mock REST API response
        source.gitlab_client._get_rest = AsyncMock(
            return_value=[
                {"type": "blob", "name": "README.md", "path": "README.md"},
                {"type": "blob", "name": "README.rst", "path": "README.rst"},
                {"type": "blob", "name": "other.py", "path": "other.py"},
                {"type": "tree", "name": "src", "path": "src"},
            ]
        )

        readme_docs = []
        async for doc, _download_func in source._fetch_readme_files(
            123, mock_gitlab_project
        ):
            readme_docs.append(doc)

        # Should yield 2 READMEs (md and rst), skip other.py and tree
        assert len(readme_docs) == 2
        assert readme_docs[0]["file_name"] == "README.md"
        assert readme_docs[1]["file_name"] == "README.rst"

    @pytest.mark.asyncio
    async def test_fetch_readme_files_no_default_branch(self, mock_configuration):
        """Test _fetch_readme_files handles missing default branch."""
        source = GitLabDataSource(configuration=mock_configuration)

        project = GitLabProject(
            id="gid://gitlab/Project/123",
            name="Test",
            path="test",
            full_path="group/test",
            visibility="public",
            star_count=0,
            forks_count=0,
            created_at="2023-01-01T00:00:00Z",
            web_url="https://gitlab.com/test",
            repository=None,  # No repository means no default branch
        )

        readme_docs = []
        async for doc, _ in source._fetch_readme_files(123, project):
            readme_docs.append(doc)

        assert len(readme_docs) == 0

    @pytest.mark.asyncio
    async def test_fetch_readme_files_unsupported_extensions(
        self, mock_configuration, mock_gitlab_project
    ):
        """Test _fetch_readme_files skips unsupported extensions."""
        source = GitLabDataSource(configuration=mock_configuration)

        source.gitlab_client._get_rest = AsyncMock(
            return_value=[
                {"type": "blob", "name": "README.pdf", "path": "README.pdf"},
                {"type": "blob", "name": "README.docx", "path": "README.docx"},
            ]
        )

        readme_docs = []
        async for doc, _ in source._fetch_readme_files(123, mock_gitlab_project):
            readme_docs.append(doc)

        # Should skip unsupported extensions
        assert len(readme_docs) == 0

    @pytest.mark.asyncio
    async def test_get_content_with_doit_true(self, mock_configuration):
        """Test get_content downloads file when doit=True."""
        source = GitLabDataSource(configuration=mock_configuration)

        # Mock dependencies
        source.can_file_be_downloaded = Mock(return_value=True)
        source.download_and_extract_file = AsyncMock(
            return_value={
                "_id": "file_123_README.md",
                "_timestamp": "2023-01-01T00:00:00Z",
                "_attachment": "file content",
            }
        )

        attachment = {
            "project_id": 123,
            "file_path": "README.md",
            "file_name": "README.md",
            "ref": "main",
            "_timestamp": "2023-01-01T00:00:00Z",
        }

        result = await source.get_content(attachment, doit=True)

        assert result is not None
        assert result["_id"] == "file_123_README.md"
        source.download_and_extract_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_download_func(self, mock_configuration):
        """Test download_func yields decoded content."""
        source = GitLabDataSource(configuration=mock_configuration)

        source.gitlab_client.get_file_content = AsyncMock(
            return_value={"content": "SGVsbG8gV29ybGQ="}  # "Hello World" base64
        )

        content_chunks = []
        async for chunk in source.download_func(123, "README.md", "main"):
            if chunk:
                content_chunks.append(chunk)

        assert len(content_chunks) == 1
        assert content_chunks[0] == b"Hello World"

    @pytest.mark.asyncio
    async def test_download_func_no_content(self, mock_configuration):
        """Test download_func handles missing content field."""
        source = GitLabDataSource(configuration=mock_configuration)

        source.gitlab_client.get_file_content = AsyncMock(return_value={})

        content_chunks = []
        async for chunk in source.download_func(123, "README.md", "main"):
            content_chunks.append(chunk)

        # Should yield None when no content
        assert len(content_chunks) == 1
        assert content_chunks[0] is None

    @pytest.mark.asyncio
    async def test_get_docs_integration(self, mock_gitlab_work_item):
        """Test get_docs integrates all components."""
        # Create project that matches configured projects filter
        project = GitLabProject(
            id="gid://gitlab/Project/123",
            name="project1",
            path="project1",
            full_path="group/project1",  # Matches configured_projects
            description=None,
            visibility="public",
            star_count=0,
            forks_count=0,
            created_at="2023-01-01T00:00:00Z",
            last_activity_at=None,
            archived=False,
            web_url="https://gitlab.com/group/project1",
            repository=GitLabRepository(root_ref="main"),
            group=GitLabGroup(id="gid://gitlab/Group/456", full_path="group"),
        )

        config = GitLabDataSource.get_default_configuration()
        config["token"]["value"] = "test-token-123"
        config["projects"]["value"] = ["group/project1"]
        source = GitLabDataSource(configuration=DataSourceConfiguration(config))

        # Mock the client methods
        async def mock_get_projects():
            yield project

        async def mock_get_work_items(*args, **kwargs):
            yield mock_gitlab_work_item

        async def mock_get_merge_requests(*args, **kwargs):
            return
            yield  # Empty generator

        async def mock_get_releases(*args, **kwargs):
            return
            yield  # Empty generator

        async def mock_fetch_remaining_assignees(*args, **kwargs):
            return
            yield  # Empty generator

        async def mock_fetch_remaining_labels(*args, **kwargs):
            return
            yield  # Empty generator

        async def mock_fetch_remaining_discussions(*args, **kwargs):
            return
            yield  # Empty generator

        source.gitlab_client.get_projects = mock_get_projects
        source.gitlab_client.get_work_items_project = mock_get_work_items
        source.gitlab_client.get_merge_requests = mock_get_merge_requests
        source.gitlab_client.get_releases = mock_get_releases
        source.gitlab_client.fetch_remaining_work_item_assignees = (
            mock_fetch_remaining_assignees
        )
        source.gitlab_client.fetch_remaining_work_item_labels = (
            mock_fetch_remaining_labels
        )
        source.gitlab_client.fetch_remaining_work_item_discussions = (
            mock_fetch_remaining_discussions
        )
        source.gitlab_client._get_rest = AsyncMock(return_value=[])  # No README files

        docs = []
        async for doc, _ in source.get_docs():
            docs.append(doc)

        # Should yield project + work item
        assert len(docs) >= 2
        assert docs[0]["type"] == "Project"
        assert docs[1]["type"] == "Issue"

    @pytest.mark.asyncio
    async def test_get_docs_with_paginated_work_item_fields(self, mock_gitlab_project):
        """Test get_docs handles paginated assignees/labels/discussions in work items."""
        # Create work item with paginated assignees
        work_item = GitLabWorkItem.model_validate(
            {
                "id": "gid://gitlab/WorkItem/1",
                "iid": 1,
                "title": "Issue with many assignees",
                "state": "opened",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-01T00:00:00Z",
                "webUrl": "https://gitlab.com/group/project1/issues/1",
                "workItemType": {"name": "Issue"},
                "widgets": [
                    {
                        "__typename": "WorkItemWidgetAssignees",
                        "assignees": {
                            "nodes": [{"username": "user1", "name": "User 1"}],
                            "pageInfo": {"hasNextPage": True, "endCursor": "cursor1"},
                        },
                    },
                    {
                        "__typename": "WorkItemWidgetLabels",
                        "labels": {
                            "nodes": [{"title": "label1"}],
                            "pageInfo": {"hasNextPage": True, "endCursor": "cursor2"},
                        },
                    },
                    {
                        "__typename": "WorkItemWidgetNotes",
                        "discussions": {
                            "nodes": [],
                            "pageInfo": {"hasNextPage": True, "endCursor": "cursor3"},
                        },
                    },
                ],
            }
        )

        config = GitLabDataSource.get_default_configuration()
        config["token"]["value"] = "test-token-123"
        config["projects"]["value"] = ["group/test-project"]
        source = GitLabDataSource(configuration=DataSourceConfiguration(config))

        async def mock_get_projects():
            yield mock_gitlab_project

        async def mock_get_work_items(*args, **kwargs):
            yield work_item

        async def mock_remaining_assignees(*args, **kwargs):
            yield {"username": "user2", "name": "User 2"}

        async def mock_remaining_labels(*args, **kwargs):
            yield {"title": "label2"}

        async def mock_remaining_discussions(*args, **kwargs):
            yield {
                "id": "disc1",
                "notes": {"nodes": [], "pageInfo": {"hasNextPage": False}},
            }

        source.gitlab_client.get_projects = mock_get_projects
        source.gitlab_client.get_work_items_project = mock_get_work_items
        source.gitlab_client.get_merge_requests = lambda *a, **k: async_gen_empty()
        source.gitlab_client.get_releases = lambda *a, **k: async_gen_empty()
        source.gitlab_client.fetch_remaining_work_item_assignees = (
            mock_remaining_assignees
        )
        source.gitlab_client.fetch_remaining_work_item_labels = mock_remaining_labels
        source.gitlab_client.fetch_remaining_work_item_discussions = (
            mock_remaining_discussions
        )
        source.gitlab_client._get_rest = AsyncMock(return_value=[])

        docs = []
        async for doc, _ in source.get_docs():
            docs.append(doc)

        # Should have project + work item
        issue_doc = [d for d in docs if d["type"] == "Issue"][0]
        # Should have fetched paginated assignees and labels
        assert len(issue_doc["assignees"]) == 2  # user1 + user2
        assert len(issue_doc["labels"]) == 2  # label1 + label2

    @pytest.mark.asyncio
    async def test_get_docs_with_epics_and_pagination(self):
        """Test get_docs handles epics with paginated fields."""
        project = GitLabProject(
            id="gid://gitlab/Project/123",
            name="project1",
            path="project1",
            full_path="group/project1",
            visibility="public",
            star_count=0,
            forks_count=0,
            created_at="2023-01-01T00:00:00Z",
            web_url="https://gitlab.com/group/project1",
            repository=GitLabRepository(root_ref="main"),
            group=GitLabGroup(id="gid://gitlab/Group/456", full_path="group"),
        )

        epic = GitLabWorkItem.model_validate(
            {
                "id": "gid://gitlab/WorkItem/999",
                "iid": 1,
                "title": "Epic with pagination",
                "state": "opened",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-01T00:00:00Z",
                "webUrl": "https://gitlab.com/groups/group/-/epics/1",
                "workItemType": {"name": "Epic"},
                "widgets": [
                    {
                        "__typename": "WorkItemWidgetAssignees",
                        "assignees": {
                            "nodes": [{"username": "epicuser1"}],
                            "pageInfo": {
                                "hasNextPage": True,
                                "endCursor": "epiccursor1",
                            },
                        },
                    },
                    {
                        "__typename": "WorkItemWidgetLabels",
                        "labels": {
                            "nodes": [{"title": "epiclabel1"}],
                            "pageInfo": {
                                "hasNextPage": True,
                                "endCursor": "epiccursor2",
                            },
                        },
                    },
                    {
                        "__typename": "WorkItemWidgetNotes",
                        "discussions": {
                            "nodes": [],
                            "pageInfo": {"hasNextPage": False},
                        },
                    },
                ],
            }
        )

        config = GitLabDataSource.get_default_configuration()
        config["token"]["value"] = "test-token-123"
        config["projects"]["value"] = ["group/project1"]
        source = GitLabDataSource(configuration=DataSourceConfiguration(config))

        async def mock_get_projects():
            yield project

        async def mock_get_epics(*args, **kwargs):
            yield epic

        async def mock_remaining_epic_assignees(*args, **kwargs):
            yield {"username": "epicuser2", "name": "Epic User 2"}

        async def mock_remaining_epic_labels(*args, **kwargs):
            yield {"title": "epiclabel2"}

        source.gitlab_client.get_projects = mock_get_projects
        source.gitlab_client.get_work_items_project = lambda *a, **k: async_gen_empty()
        source.gitlab_client.get_work_items_group = mock_get_epics
        source.gitlab_client.get_merge_requests = lambda *a, **k: async_gen_empty()
        source.gitlab_client.get_releases = lambda *a, **k: async_gen_empty()
        source.gitlab_client.fetch_remaining_work_item_assignees_group = (
            mock_remaining_epic_assignees
        )
        source.gitlab_client.fetch_remaining_work_item_labels_group = (
            mock_remaining_epic_labels
        )
        source.gitlab_client.fetch_remaining_work_item_group_discussions = (
            lambda *a, **k: async_gen_empty()
        )
        source.gitlab_client._get_rest = AsyncMock(return_value=[])

        docs = []
        async for doc, _ in source.get_docs():
            docs.append(doc)

        # Should have project + epic
        epic_doc = [d for d in docs if d["type"] == "Epic"][0]
        # Should have fetched paginated assignees and labels
        assert len(epic_doc["assignees"]) == 2  # epicuser1 + epicuser2
        assert len(epic_doc["labels"]) == 2  # epiclabel1 + epiclabel2

    @pytest.mark.asyncio
    async def test_get_docs_epic_fetch_failure_graceful(self):
        """Test get_docs handles epic fetch failures gracefully (Free tier)."""
        project = GitLabProject(
            id="gid://gitlab/Project/123",
            name="project1",
            path="project1",
            full_path="group/project1",
            visibility="public",
            star_count=0,
            forks_count=0,
            created_at="2023-01-01T00:00:00Z",
            web_url="https://gitlab.com/group/project1",
            repository=GitLabRepository(root_ref="main"),
            group=GitLabGroup(id="gid://gitlab/Group/456", full_path="group"),
        )

        config = GitLabDataSource.get_default_configuration()
        config["token"]["value"] = "test-token-123"
        config["projects"]["value"] = ["group/project1"]
        source = GitLabDataSource(configuration=DataSourceConfiguration(config))

        async def mock_get_projects():
            yield project

        async def mock_get_epics(*args, **kwargs):
            # Simulate Premium/Ultimate tier error
            msg = "Epics require Premium tier"
            raise Exception(msg)
            yield  # Never reached

        source.gitlab_client.get_projects = mock_get_projects
        source.gitlab_client.get_work_items_project = lambda *a, **k: async_gen_empty()
        source.gitlab_client.get_work_items_group = mock_get_epics
        source.gitlab_client.get_merge_requests = lambda *a, **k: async_gen_empty()
        source.gitlab_client.get_releases = lambda *a, **k: async_gen_empty()
        source.gitlab_client._get_rest = AsyncMock(return_value=[])

        docs = []
        # Should not crash, just skip epics
        async for doc, _ in source.get_docs():
            docs.append(doc)

        # Should still have project doc
        assert len(docs) >= 1
        assert docs[0]["type"] == "Project"

    @pytest.mark.asyncio
    async def test_get_docs_epic_with_discussions_pagination(self):
        """Test get_docs handles epic discussions pagination."""
        project = GitLabProject(
            id="gid://gitlab/Project/123",
            name="project1",
            path="project1",
            full_path="group/project1",
            visibility="public",
            star_count=0,
            forks_count=0,
            created_at="2023-01-01T00:00:00Z",
            web_url="https://gitlab.com/group/project1",
            repository=GitLabRepository(root_ref="main"),
            group=GitLabGroup(id="gid://gitlab/Group/456", full_path="group"),
        )

        epic = GitLabWorkItem.model_validate(
            {
                "id": "gid://gitlab/WorkItem/999",
                "iid": 1,
                "title": "Epic with paginated discussions",
                "state": "opened",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-01T00:00:00Z",
                "webUrl": "https://gitlab.com/groups/group/-/epics/1",
                "workItemType": {"name": "Epic"},
                "widgets": [
                    {
                        "__typename": "WorkItemWidgetNotes",
                        "discussions": {
                            "nodes": [
                                {
                                    "id": "gid://gitlab/Discussion/1",
                                    "notes": {
                                        "nodes": [
                                            {
                                                "id": "gid://gitlab/Note/1",
                                                "body": "First note",
                                                "createdAt": "2023-01-01T00:00:00Z",
                                                "updatedAt": "2023-01-01T00:00:00Z",
                                                "author": {"username": "user1"},
                                            }
                                        ]
                                    },
                                }
                            ],
                            "pageInfo": {
                                "hasNextPage": True,
                                "endCursor": "disccursor1",
                            },
                        },
                    },
                ],
            }
        )

        config = GitLabDataSource.get_default_configuration()
        config["token"]["value"] = "test-token-123"
        config["projects"]["value"] = ["group/project1"]
        source = GitLabDataSource(configuration=DataSourceConfiguration(config))

        async def mock_get_projects():
            yield project

        async def mock_get_epics(*args, **kwargs):
            yield epic

        async def mock_remaining_discussions(*args, **kwargs):
            yield {
                "id": "gid://gitlab/Discussion/2",
                "notes": {
                    "nodes": [
                        {
                            "id": "gid://gitlab/Note/2",
                            "body": "Second note",
                            "createdAt": "2023-01-02T00:00:00Z",
                            "updatedAt": "2023-01-02T00:00:00Z",
                            "author": {"username": "user2"},
                        }
                    ]
                },
            }

        source.gitlab_client.get_projects = mock_get_projects
        source.gitlab_client.get_work_items_project = lambda *a, **k: async_gen_empty()
        source.gitlab_client.get_work_items_group = mock_get_epics
        source.gitlab_client.get_merge_requests = lambda *a, **k: async_gen_empty()
        source.gitlab_client.get_releases = lambda *a, **k: async_gen_empty()
        source.gitlab_client.fetch_remaining_work_item_group_discussions = (
            mock_remaining_discussions
        )
        source.gitlab_client._get_rest = AsyncMock(return_value=[])

        docs = []
        async for doc, _ in source.get_docs():
            docs.append(doc)

        epic_doc = [d for d in docs if d["type"] == "Epic"][0]
        # Should have 2 discussions (1 initial + 1 paginated)
        assert len(epic_doc["notes"]) == 2

    @pytest.mark.asyncio
    async def test_get_docs_epic_with_parent_hierarchy(self):
        """Test get_docs formats epic with parent hierarchy correctly."""
        project = GitLabProject(
            id="gid://gitlab/Project/123",
            name="project1",
            path="project1",
            full_path="group/project1",
            visibility="public",
            star_count=0,
            forks_count=0,
            created_at="2023-01-01T00:00:00Z",
            web_url="https://gitlab.com/group/project1",
            repository=GitLabRepository(root_ref="main"),
            group=GitLabGroup(id="gid://gitlab/Group/456", full_path="group"),
        )

        epic_with_parent = GitLabWorkItem.model_validate(
            {
                "id": "gid://gitlab/WorkItem/999",
                "iid": 2,
                "title": "Child Epic",
                "state": "opened",
                "createdAt": "2023-01-01T00:00:00Z",
                "updatedAt": "2023-01-01T00:00:00Z",
                "webUrl": "https://gitlab.com/groups/group/-/epics/2",
                "workItemType": {"name": "Epic"},
                "widgets": [
                    {
                        "__typename": "WorkItemWidgetHierarchy",
                        "parent": {
                            "id": "gid://gitlab/WorkItem/888",
                            "iid": 1,
                            "title": "Parent Epic",
                        },
                        "children": {
                            "nodes": [
                                {
                                    "id": "gid://gitlab/WorkItem/777",
                                    "iid": 3,
                                    "title": "Sub-epic",
                                }
                            ]
                        },
                    },
                ],
            }
        )

        config = GitLabDataSource.get_default_configuration()
        config["token"]["value"] = "test-token-123"
        config["projects"]["value"] = ["group/project1"]
        source = GitLabDataSource(configuration=DataSourceConfiguration(config))

        async def mock_get_projects():
            yield project

        async def mock_get_epics(*args, **kwargs):
            yield epic_with_parent

        source.gitlab_client.get_projects = mock_get_projects
        source.gitlab_client.get_work_items_project = lambda *a, **k: async_gen_empty()
        source.gitlab_client.get_work_items_group = mock_get_epics
        source.gitlab_client.get_merge_requests = lambda *a, **k: async_gen_empty()
        source.gitlab_client.get_releases = lambda *a, **k: async_gen_empty()
        source.gitlab_client._get_rest = AsyncMock(return_value=[])

        docs = []
        async for doc, _ in source.get_docs():
            docs.append(doc)

        epic_doc = [d for d in docs if d["type"] == "Epic"][0]
        # Should have parent info
        assert epic_doc["parent_epic_iid"] == 1
        assert epic_doc["parent_epic_title"] == "Parent Epic"
        # Should have children info
        assert epic_doc["children_count"] == 1
        assert len(epic_doc["children"]) == 1
        assert epic_doc["children"][0]["iid"] == 3

    @pytest.mark.asyncio
    async def test_get_docs_with_merge_requests_and_pagination(self):
        """Test get_docs handles MRs with paginated reviewers and approved_by."""
        project = GitLabProject(
            id="gid://gitlab/Project/123",
            name="project1",
            path="project1",
            full_path="group/project1",
            visibility="public",
            star_count=0,
            forks_count=0,
            created_at="2023-01-01T00:00:00Z",
            web_url="https://gitlab.com/group/project1",
            repository=GitLabRepository(root_ref="main"),
        )

        mr = GitLabMergeRequest(
            iid=1,
            title="Test MR",
            state="opened",
            web_url="https://gitlab.com/group/project1/merge_requests/1",
            created_at="2023-01-01T00:00:00Z",
            updated_at="2023-01-01T00:00:00Z",
            source_branch="feature",
            target_branch="main",
            assignees=PaginatedList(
                nodes=[GitLabUser(username="assignee1")],
                page_info=PageInfo(has_next_page=True, end_cursor="cursor1"),
            ),
            reviewers=PaginatedList(
                nodes=[GitLabUser(username="reviewer1")],
                page_info=PageInfo(has_next_page=True, end_cursor="cursor2"),
            ),
            labels=PaginatedList(nodes=[], page_info=PageInfo(has_next_page=False)),
            discussions=PaginatedList(
                nodes=[], page_info=PageInfo(has_next_page=False)
            ),
            approved_by=PaginatedList(
                nodes=[GitLabUser(username="approver1")],
                page_info=PageInfo(has_next_page=True, end_cursor="cursor3"),
            ),
        )

        config = GitLabDataSource.get_default_configuration()
        config["token"]["value"] = "test-token-123"
        config["projects"]["value"] = ["group/project1"]
        source = GitLabDataSource(configuration=DataSourceConfiguration(config))

        async def mock_get_projects():
            yield project

        async def mock_get_mrs(*args, **kwargs):
            yield mr

        async def mock_remaining_field(
            project_path, iid, field_name, issuable_type, cursor
        ):
            if field_name == "assignees":
                yield {"username": "assignee2", "name": "Assignee 2"}
            elif field_name == "reviewers":
                yield {"username": "reviewer2", "name": "Reviewer 2"}
            elif field_name == "approvedBy":
                yield {"username": "approver2", "name": "Approver 2"}

        source.gitlab_client.get_projects = mock_get_projects
        source.gitlab_client.get_work_items_project = lambda *a, **k: async_gen_empty()
        source.gitlab_client.get_merge_requests = mock_get_mrs
        source.gitlab_client.get_releases = lambda *a, **k: async_gen_empty()
        source.gitlab_client.fetch_remaining_field = mock_remaining_field
        source.gitlab_client._get_rest = AsyncMock(return_value=[])

        docs = []
        async for doc, _ in source.get_docs():
            docs.append(doc)

        # Find the MR doc
        mr_doc = [d for d in docs if d["type"] == "Merge Request"][0]
        # Should have fetched paginated fields
        assert len(mr_doc["assignees"]) == 2  # assignee1 + assignee2
        assert len(mr_doc["reviewers"]) == 2  # reviewer1 + reviewer2
        assert len(mr_doc["approved_by"]) == 2  # approver1 + approver2

    @pytest.mark.asyncio
    async def test_get_docs_with_releases(self):
        """Test get_docs yields release documents."""
        project = GitLabProject(
            id="gid://gitlab/Project/123",
            name="project1",
            path="project1",
            full_path="group/project1",
            visibility="public",
            star_count=0,
            forks_count=0,
            created_at="2023-01-01T00:00:00Z",
            web_url="https://gitlab.com/group/project1",
            repository=GitLabRepository(root_ref="main"),
        )

        release = GitLabRelease(
            tag_name="v1.0.0",
            name="Release 1.0.0",
            created_at="2023-01-01T00:00:00Z",
            milestones=PaginatedList(nodes=[], page_info=PageInfo(has_next_page=False)),
            assets=GitLabAssets(
                count=0,
                links=PaginatedList(nodes=[], page_info=PageInfo(has_next_page=False)),
            ),
        )

        config = GitLabDataSource.get_default_configuration()
        config["token"]["value"] = "test-token-123"
        config["projects"]["value"] = ["group/project1"]
        source = GitLabDataSource(configuration=DataSourceConfiguration(config))

        async def mock_get_projects():
            yield project

        async def mock_get_releases(*args, **kwargs):
            yield release

        source.gitlab_client.get_projects = mock_get_projects
        source.gitlab_client.get_work_items_project = lambda *a, **k: async_gen_empty()
        source.gitlab_client.get_merge_requests = lambda *a, **k: async_gen_empty()
        source.gitlab_client.get_releases = mock_get_releases
        source.gitlab_client._get_rest = AsyncMock(return_value=[])

        docs = []
        async for doc, _ in source.get_docs():
            docs.append(doc)

        # Should have project + release
        assert len(docs) >= 2
        release_doc = [d for d in docs if d["type"] == "Release"][0]
        assert release_doc["tag_name"] == "v1.0.0"

    @pytest.mark.asyncio
    async def test_extract_notes_with_multi_level_pagination(self, mock_configuration):
        """Test complex nested pagination: multiple discussions each with paginated notes.

        This tests the most complex scenario:
        - Multiple discussions (Discussion 1 and Discussion 2)
        - Each discussion has paginated notes (hasNextPage=True)
        - Need to fetch remaining notes for EACH discussion separately
        """
        source = GitLabDataSource(configuration=mock_configuration)

        # Create discussions with paginated notes inside
        discussions = PaginatedList[GitLabDiscussion](
            nodes=[
                # Discussion 1: has 2 initial notes + more to fetch
                GitLabDiscussion(
                    id="gid://gitlab/Discussion/1",
                    notes=PaginatedList[GitLabNote](
                        nodes=[
                            GitLabNote(
                                id="note_1_1",
                                body="Discussion 1, Note 1",
                                created_at="2023-01-01T00:00:00Z",
                                updated_at="2023-01-01T00:00:00Z",
                                system=False,
                                author=GitLabUser(username="user1"),
                            ),
                            GitLabNote(
                                id="note_1_2",
                                body="Discussion 1, Note 2",
                                created_at="2023-01-01T01:00:00Z",
                                updated_at="2023-01-01T01:00:00Z",
                                system=False,
                                author=GitLabUser(username="user2"),
                            ),
                        ],
                        page_info=PageInfo(
                            has_next_page=True, end_cursor="disc1_cursor"
                        ),
                    ),
                ),
                # Discussion 2: has 1 initial note + more to fetch
                GitLabDiscussion(
                    id="gid://gitlab/Discussion/2",
                    notes=PaginatedList[GitLabNote](
                        nodes=[
                            GitLabNote(
                                id="note_2_1",
                                body="Discussion 2, Note 1",
                                created_at="2023-01-02T00:00:00Z",
                                updated_at="2023-01-02T00:00:00Z",
                                system=False,
                                author=GitLabUser(username="user3"),
                            ),
                        ],
                        page_info=PageInfo(
                            has_next_page=True, end_cursor="disc2_cursor"
                        ),
                    ),
                ),
            ],
            page_info=PageInfo(has_next_page=False),  # No more discussions
        )

        # Mock fetch_remaining_notes to return different notes for different discussions
        async def mock_fetch_remaining_notes(
            project_path, iid, discussion_id, issuable_type, cursor
        ):
            """Mock fetching remaining notes for each discussion."""
            if discussion_id == "gid://gitlab/Discussion/1":
                # Discussion 1 has 2 more notes to fetch
                yield {
                    "id": "note_1_3",
                    "body": "Discussion 1, Note 3 (paginated)",
                    "createdAt": "2023-01-01T02:00:00Z",
                    "updatedAt": "2023-01-01T02:00:00Z",
                    "system": False,
                    "author": {"username": "user4", "name": "User 4"},
                }
                yield {
                    "id": "note_1_4",
                    "body": "Discussion 1, Note 4 (paginated)",
                    "createdAt": "2023-01-01T03:00:00Z",
                    "updatedAt": "2023-01-01T03:00:00Z",
                    "system": False,
                    "author": {"username": "user5", "name": "User 5"},
                }
            elif discussion_id == "gid://gitlab/Discussion/2":
                # Discussion 2 has 1 more note to fetch
                yield {
                    "id": "note_2_2",
                    "body": "Discussion 2, Note 2 (paginated)",
                    "createdAt": "2023-01-02T01:00:00Z",
                    "updatedAt": "2023-01-02T01:00:00Z",
                    "system": False,
                    "author": {"username": "user6", "name": "User 6"},
                }

        source.gitlab_client.fetch_remaining_notes = mock_fetch_remaining_notes

        # Extract notes - this should handle all pagination automatically
        notes = await source._extract_notes_from_discussions(
            discussions, "group/project", 1, "issue"
        )

        # Verify all notes were collected
        assert (
            len(notes) == 6
        ), "Should have 6 total notes (2+2 from disc1 + 1+1 from disc2)"

        # Verify notes from Discussion 1 (initial + paginated)
        disc1_notes = [n for n in notes if "Discussion 1" in n["body"]]
        assert len(disc1_notes) == 4, "Discussion 1 should have 4 notes total"
        assert disc1_notes[0]["id"] == "note_1_1"
        assert disc1_notes[1]["id"] == "note_1_2"
        assert disc1_notes[2]["id"] == "note_1_3"
        assert disc1_notes[3]["id"] == "note_1_4"

        # Verify notes from Discussion 2 (initial + paginated)
        disc2_notes = [n for n in notes if "Discussion 2" in n["body"]]
        assert len(disc2_notes) == 2, "Discussion 2 should have 2 notes total"
        assert disc2_notes[0]["id"] == "note_2_1"
        assert disc2_notes[1]["id"] == "note_2_2"

        # Verify all authors were properly extracted
        authors = [n["author"] for n in notes if n["author"]]
        assert len(authors) == 6
        assert "user1" in authors
        assert "user6" in authors


# Tests for error handling in client
class TestGitLabClientErrorHandling:
    """Test suite for GitLabClient error handling."""

    @pytest.mark.asyncio
    async def test_execute_graphql_rate_limit_in_response(self):
        """Test _execute_graphql detects and handles rate limit in GraphQL response."""
        from unittest.mock import MagicMock

        client = GitLabClient(token="test-token")

        # Create mock response
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={"errors": [{"message": "You have exceeded the rate limit"}]}
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        # Mock session
        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        client._session = mock_session

        # Should raise on rate limit error
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(Exception, match="rate limit"):
                await client._execute_graphql("query", {})

    @pytest.mark.asyncio
    async def test_execute_graphql_general_error(self):
        """Test _execute_graphql raises on general GraphQL errors."""
        from unittest.mock import MagicMock

        client = GitLabClient(token="test-token")

        # Create mock response with non-rate-limit error
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={"errors": [{"message": "Field 'invalid' doesn't exist"}]}
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        client._session = mock_session

        # Should raise on general GraphQL error
        with pytest.raises(Exception, match="GraphQL errors"):
            await client._execute_graphql("query", {})

    @pytest.mark.asyncio
    async def test_execute_graphql_http_rate_limit(self):
        """Test _execute_graphql handles HTTP 429 rate limit."""
        from unittest.mock import MagicMock

        client = GitLabClient(token="test-token")

        # Create mock response with 429 status
        mock_response = MagicMock()
        mock_response.status = 429
        mock_response.headers = {"Retry-After": "1"}
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        client._session = mock_session

        # Should handle rate limit and raise
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            with pytest.raises(Exception, match="Rate limit exceeded"):
                await client._execute_graphql("query", {})

            # Should have slept before raising
            mock_sleep.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_graphql_http_error(self):
        """Test _execute_graphql handles HTTP errors (non-200 status)."""
        from unittest.mock import MagicMock

        import aiohttp

        client = GitLabClient(token="test-token")

        # Create mock response with 500 error
        mock_response = MagicMock()
        mock_response.status = 500
        mock_response.raise_for_status = Mock(
            side_effect=aiohttp.ClientResponseError(
                request_info=Mock(),
                history=(),
                status=500,
                message="Internal Server Error",
            )
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        client._session = mock_session

        # Should raise HTTP error
        with pytest.raises(aiohttp.ClientResponseError):
            await client._execute_graphql("query", {})

    @pytest.mark.asyncio
    async def test_get_rest_rate_limit(self):
        """Test _get_rest handles rate limit."""
        from unittest.mock import MagicMock

        client = GitLabClient(token="test-token")

        # Create mock response with 429 status
        mock_response = MagicMock()
        mock_response.status = 429
        mock_response.headers = {"Retry-After": "1"}
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False

        client._session = mock_session

        # Should handle rate limit and raise
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            with pytest.raises(Exception, match="Rate limit exceeded"):
                await client._get_rest("projects")

            mock_sleep.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_rest_http_error(self):
        """Test _get_rest handles HTTP errors."""
        from unittest.mock import MagicMock

        import aiohttp

        client = GitLabClient(token="test-token")

        # Create mock response with 404 error
        mock_response = MagicMock()
        mock_response.status = 404
        mock_response.raise_for_status = Mock(
            side_effect=aiohttp.ClientResponseError(
                request_info=Mock(), history=(), status=404, message="Not Found"
            )
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False

        client._session = mock_session

        # Should raise HTTP error
        with pytest.raises(aiohttp.ClientResponseError):
            await client._get_rest("invalid/endpoint")

    @pytest.mark.asyncio
    async def test_handle_rate_limit_with_ratelimit_reset_header(self):
        """Test _handle_rate_limit uses RateLimit-Reset header when no Retry-After."""
        client = GitLabClient(token="test-token")

        mock_response = Mock()
        mock_response.status = 429
        mock_response.headers = {"RateLimit-Reset": "1234567890"}  # No Retry-After

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            with patch("time.time", return_value=1234567880):  # 10 seconds before reset
                result = await client._handle_rate_limit(mock_response)

                assert result is True
                # Should sleep for the time difference
                assert mock_sleep.called

    @pytest.mark.asyncio
    async def test_handle_rate_limit_default_sleep(self):
        """Test _handle_rate_limit uses default 60s when no headers."""
        client = GitLabClient(token="test-token")

        mock_response = Mock()
        mock_response.status = 429
        mock_response.headers = {}  # No rate limit headers

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await client._handle_rate_limit(mock_response)

            assert result is True
            # Should sleep for default 60 seconds
            assert mock_sleep.call_count == 1
            assert mock_sleep.call_args[0][0] == 60  # First positional arg


# Tests for error handling in async generators
class TestGitLabClientAsyncGeneratorErrors:
    """Test suite for error handling in async generator methods."""

    @pytest.mark.asyncio
    async def test_get_merge_requests_exception(self):
        """Test get_merge_requests handles exceptions."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.side_effect = Exception("Network error")

            mrs = []
            async for mr in client.get_merge_requests("group/project"):
                mrs.append(mr)

            assert len(mrs) == 0

    @pytest.mark.asyncio
    async def test_get_merge_requests_missing_project(self):
        """Test get_merge_requests handles missing project."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {"project": None}

            mrs = []
            async for mr in client.get_merge_requests("group/project"):
                mrs.append(mr)

            assert len(mrs) == 0

    @pytest.mark.asyncio
    async def test_get_releases_exception(self):
        """Test get_releases handles exceptions."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.side_effect = Exception("Permission denied")

            releases = []
            async for release in client.get_releases("group/project"):
                releases.append(release)

            assert len(releases) == 0

    @pytest.mark.asyncio
    async def test_get_work_items_project_exception(self):
        """Test get_work_items_project handles exceptions."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.side_effect = Exception("Query failed")

            items = []
            async for item in client.get_work_items_project(
                "group/project", [WorkItemType.ISSUE]
            ):
                items.append(item)

            assert len(items) == 0

    @pytest.mark.asyncio
    async def test_get_work_items_group_exception(self):
        """Test get_work_items_group handles exceptions."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.side_effect = Exception("Group not found")

            items = []
            async for item in client.get_work_items_group("group", [WorkItemType.EPIC]):
                items.append(item)

            assert len(items) == 0

    @pytest.mark.asyncio
    async def test_fetch_remaining_field_exception(self):
        """Test fetch_remaining_field handles exceptions."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.side_effect = Exception("Fetch failed")

            items = []
            async for item in client.fetch_remaining_field(
                "group/project", 1, "assignees", "issue", "cursor1"
            ):
                items.append(item)

            assert len(items) == 0

    @pytest.mark.asyncio
    async def test_fetch_remaining_field_missing_project(self):
        """Test fetch_remaining_field handles missing project."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {}

            items = []
            async for item in client.fetch_remaining_field(
                "group/project", 1, "assignees", "issue", "cursor1"
            ):
                items.append(item)

            assert len(items) == 0

    @pytest.mark.asyncio
    async def test_fetch_remaining_field_missing_issuable(self):
        """Test fetch_remaining_field handles missing issuable."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {"project": {"issue": None}}

            items = []
            async for item in client.fetch_remaining_field(
                "group/project", 1, "assignees", "issue", "cursor1"
            ):
                items.append(item)

            assert len(items) == 0

    @pytest.mark.asyncio
    async def test_fetch_remaining_notes_missing_discussion(self):
        """Test fetch_remaining_notes handles missing discussion."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            # Return empty discussions
            mock_graphql.return_value = {
                "project": {"issue": {"discussions": {"nodes": []}}}
            }

            notes = []
            async for note in client.fetch_remaining_notes(
                "group/project", 1, "disc123", "issue", "cursor1"
            ):
                notes.append(note)

            assert len(notes) == 0

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_assignees_exception(self):
        """Test fetch_remaining_work_item_assignees handles aiohttp exceptions."""
        import aiohttp

        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            # Use aiohttp.ClientError which is caught by the code
            mock_graphql.side_effect = aiohttp.ClientError("Fetch error")

            assignees = []
            async for assignee in client.fetch_remaining_work_item_assignees(
                "group/project", 1, "ISSUE", "cursor1"
            ):
                assignees.append(assignee)

            assert len(assignees) == 0

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_assignees_missing_widget(self):
        """Test fetch_remaining_work_item_assignees handles missing widget."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            # Return work item with no assignees widget
            mock_graphql.return_value = {
                "project": {"workItems": {"nodes": [{"widgets": []}]}}
            }

            assignees = []
            async for assignee in client.fetch_remaining_work_item_assignees(
                "group/project", 1, "ISSUE", "cursor1"
            ):
                assignees.append(assignee)

            assert len(assignees) == 0

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_labels_missing_widget(self):
        """Test fetch_remaining_work_item_labels handles missing widget."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {
                "project": {"workItems": {"nodes": [{"widgets": []}]}}
            }

            labels = []
            async for label in client.fetch_remaining_work_item_labels(
                "group/project", 1, "ISSUE", "cursor1"
            ):
                labels.append(label)

            assert len(labels) == 0

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_discussions_missing_widget(self):
        """Test fetch_remaining_work_item_discussions handles missing widget."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {
                "project": {"workItems": {"nodes": [{"widgets": []}]}}
            }

            discussions = []
            async for discussion in client.fetch_remaining_work_item_discussions(
                "group/project", 1, "ISSUE", "cursor1"
            ):
                discussions.append(discussion)

            assert len(discussions) == 0

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_group_discussions_missing_widget(self):
        """Test fetch_remaining_work_item_group_discussions handles missing widget."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {
                "group": {"workItems": {"nodes": [{"widgets": []}]}}
            }

            discussions = []
            async for discussion in client.fetch_remaining_work_item_group_discussions(
                "group", 1, "EPIC", "cursor1"
            ):
                discussions.append(discussion)

            assert len(discussions) == 0

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_assignees_group(self):
        """Test fetch_remaining_work_item_assignees_group pagination."""
        client = GitLabClient(token="test-token")

        # Mock two pages of assignees
        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.side_effect = [
                # First page
                {
                    "group": {
                        "workItems": {
                            "nodes": [
                                {
                                    "widgets": [
                                        {
                                            "__typename": "WorkItemWidgetAssignees",
                                            "assignees": {
                                                "pageInfo": {
                                                    "hasNextPage": True,
                                                    "endCursor": "cursor2",
                                                },
                                                "nodes": [
                                                    {
                                                        "username": "user1",
                                                        "name": "User 1",
                                                    },
                                                    {
                                                        "username": "user2",
                                                        "name": "User 2",
                                                    },
                                                ],
                                            },
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
                # Second page
                {
                    "group": {
                        "workItems": {
                            "nodes": [
                                {
                                    "widgets": [
                                        {
                                            "__typename": "WorkItemWidgetAssignees",
                                            "assignees": {
                                                "pageInfo": {
                                                    "hasNextPage": False,
                                                    "endCursor": None,
                                                },
                                                "nodes": [
                                                    {
                                                        "username": "user3",
                                                        "name": "User 3",
                                                    },
                                                ],
                                            },
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
            ]

            assignees = []
            async for assignee in client.fetch_remaining_work_item_assignees_group(
                "mygroup", 1, "EPIC", "cursor1"
            ):
                assignees.append(assignee)

            assert len(assignees) == 3
            assert assignees[0]["username"] == "user1"
            assert assignees[1]["username"] == "user2"
            assert assignees[2]["username"] == "user3"

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_labels_group(self):
        """Test fetch_remaining_work_item_labels_group pagination."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.side_effect = [
                # First page
                {
                    "group": {
                        "workItems": {
                            "nodes": [
                                {
                                    "widgets": [
                                        {
                                            "__typename": "WorkItemWidgetLabels",
                                            "labels": {
                                                "pageInfo": {
                                                    "hasNextPage": True,
                                                    "endCursor": "cursor2",
                                                },
                                                "nodes": [
                                                    {"title": "bug"},
                                                    {"title": "critical"},
                                                ],
                                            },
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
                # Second page (no more)
                {
                    "group": {
                        "workItems": {
                            "nodes": [
                                {
                                    "widgets": [
                                        {
                                            "__typename": "WorkItemWidgetLabels",
                                            "labels": {
                                                "pageInfo": {
                                                    "hasNextPage": False,
                                                    "endCursor": None,
                                                },
                                                "nodes": [
                                                    {"title": "feature"},
                                                ],
                                            },
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
            ]

            labels = []
            async for label in client.fetch_remaining_work_item_labels_group(
                "mygroup", 1, "EPIC", "cursor1"
            ):
                labels.append(label)

            assert len(labels) == 3
            assert labels[0]["title"] == "bug"
            assert labels[1]["title"] == "critical"
            assert labels[2]["title"] == "feature"

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_assignees_group_missing_group(self):
        """Test fetch_remaining_work_item_assignees_group handles missing group."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {}

            assignees = []
            async for assignee in client.fetch_remaining_work_item_assignees_group(
                "mygroup", 1, "EPIC", "cursor1"
            ):
                assignees.append(assignee)

            assert len(assignees) == 0

    @pytest.mark.asyncio
    async def test_fetch_remaining_work_item_labels_group_missing_group(self):
        """Test fetch_remaining_work_item_labels_group handles missing group."""
        client = GitLabClient(token="test-token")

        with patch.object(
            client, "_execute_graphql", new_callable=AsyncMock
        ) as mock_graphql:
            mock_graphql.return_value = {}

            labels = []
            async for label in client.fetch_remaining_work_item_labels_group(
                "mygroup", 1, "EPIC", "cursor1"
            ):
                labels.append(label)

            assert len(labels) == 0


# Helper for empty async generators
async def async_gen_empty():
    """Empty async generator."""
    return
    yield  # Never reached
