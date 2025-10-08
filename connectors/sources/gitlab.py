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

from enum import Enum
from functools import partial
from typing import Annotated, Any, AsyncGenerator, Generic, Literal, TypeVar, Union, get_args, get_origin
from urllib.parse import quote

import aiohttp
from pydantic import BaseModel, Discriminator, Field, Tag

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
NODE_SIZE = 100  # For projects, merge requests
WORK_ITEMS_NODE_SIZE = 50  # For work items (lower to avoid complexity limit with widgets)
WORK_ITEMS_NESTED_SIZE = 50  # For discussions/notes in work items (reduced to lower complexity)
NESTED_FIELD_SIZE = 100  # For assignees, labels, discussions in MRs/legacy

SUPPORTED_EXTENSION = [".md", ".rst", ".txt"]


class WorkItemType(str, Enum):
    """GitLab Work Item types for GraphQL queries."""

    ISSUE = "ISSUE"
    EPIC = "EPIC"
    TASK = "TASK"


# Pydantic models for GitLab GraphQL responses
class PageInfo(BaseModel):
    """GraphQL pagination info."""

    has_next_page: bool = Field(alias="hasNextPage")
    end_cursor: str | None = Field(alias="endCursor", default=None)

    model_config = {"populate_by_name": True}


# Generic type variable for paginated lists
T = TypeVar("T")


def _default_page_info() -> PageInfo:
    """Create default PageInfo instance."""
    return PageInfo.model_construct(has_next_page=False, end_cursor=None)


class PaginatedList(BaseModel, Generic[T]):
    """Generic paginated list with nodes and pageInfo."""

    nodes: list[T] = []
    page_info: PageInfo = Field(
        alias="pageInfo",
        default_factory=_default_page_info,
    )

    model_config = {"populate_by_name": True}


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
    system: bool = False  # System-generated notes (status changes, etc.)
    position: dict[str, Any] | None = None  # For diff/inline comments (newLine, oldLine, newPath, oldPath)

    model_config = {"populate_by_name": True}


class GitLabDiscussion(BaseModel):
    """GitLab discussion model."""

    id: str
    notes: PaginatedList[GitLabNote]

    model_config = {"populate_by_name": True}


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
    assignees: PaginatedList[GitLabUser]
    labels: PaginatedList[GitLabLabel]
    discussions: PaginatedList[GitLabDiscussion]

    model_config = {"populate_by_name": True}


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
    assignees: PaginatedList[GitLabUser]
    reviewers: PaginatedList[GitLabUser]
    labels: PaginatedList[GitLabLabel]
    discussions: PaginatedList[GitLabDiscussion]
    approved_by: PaginatedList[GitLabUser] = Field(alias="approvedBy")
    merged_by: GitLabUser | None = Field(alias="mergedBy", default=None)

    model_config = {"populate_by_name": True}


class WorkItemWidgetHierarchy(BaseModel):
    """Work item hierarchy widget (for Epics)."""

    type_name: Literal["WorkItemWidgetHierarchy"] = Field(alias="__typename")
    parent: dict[str, Any] | None = None
    children: PaginatedList[dict[str, Any]] = Field(default_factory=lambda: PaginatedList(nodes=[]))

    model_config = {"populate_by_name": True}


class WorkItemWidgetDescription(BaseModel):
    """Work item description widget."""

    type_name: Literal["WorkItemWidgetDescription"] = Field(alias="__typename")
    description: str | None = None

    model_config = {"populate_by_name": True}


class WorkItemWidgetAssignees(BaseModel):
    """Work item assignees widget."""

    type_name: Literal["WorkItemWidgetAssignees"] = Field(alias="__typename")
    assignees: PaginatedList[GitLabUser]

    model_config = {"populate_by_name": True}


class WorkItemWidgetLabels(BaseModel):
    """Work item labels widget."""

    type_name: Literal["WorkItemWidgetLabels"] = Field(alias="__typename")
    labels: PaginatedList[GitLabLabel]

    model_config = {"populate_by_name": True}


class WorkItemWidgetNotes(BaseModel):
    """Work item notes/discussions widget."""

    type_name: Literal["WorkItemWidgetNotes"] = Field(alias="__typename")
    discussions: PaginatedList[GitLabDiscussion]

    model_config = {"populate_by_name": True}


class WorkItemWidgetUnknown(BaseModel):
    """Fallback for unknown widget types.

    This catches any widget type not explicitly modeled above.
    Uses 'extra = allow' to accept any fields from unknown widgets.
    """

    type_name: str = Field(alias="__typename")

    model_config = {
        "populate_by_name": True,
        "extra": "allow",  # Allow any extra fields
    }


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
    group: dict[str, Any] | None = None  # Contains id, fullPath for group projects

    @property
    def default_branch(self) -> str | None:
        """Extract default branch from repository."""
        if self.repository:
            return self.repository.get("rootRef")
        return None

    model_config = {"populate_by_name": True}


# Extract tag values from Literal type annotations
# This ensures tags stay in sync with the model definitions
_TAG_DESCRIPTION = get_args(WorkItemWidgetDescription.model_fields["type_name"].annotation)[0]
_TAG_ASSIGNEES = get_args(WorkItemWidgetAssignees.model_fields["type_name"].annotation)[0]
_TAG_LABELS = get_args(WorkItemWidgetLabels.model_fields["type_name"].annotation)[0]
_TAG_NOTES = get_args(WorkItemWidgetNotes.model_fields["type_name"].annotation)[0]
_TAG_HIERARCHY = get_args(WorkItemWidgetHierarchy.model_fields["type_name"].annotation)[0]

# Build discriminator lookup set from extracted tags
_KNOWN_WIDGET_TAGS = {
    _TAG_DESCRIPTION,
    _TAG_ASSIGNEES,
    _TAG_LABELS,
    _TAG_NOTES,
    _TAG_HIERARCHY,
}


def widget_discriminator(v: dict[str, Any]) -> str:
    """Discriminate widget types based on __typename field.

    Returns the __typename directly for known widgets, or 'unknown' for fallback.
    """
    typename = v.get("__typename", "")
    return typename if typename in _KNOWN_WIDGET_TAGS else "unknown"


# Discriminated union of all widget types
# Tags are automatically extracted from Literal type annotations above
# To add a new widget type: create the model, extract its tag, add to both lists
WorkItemWidget = Annotated[
    Union[
        Annotated[WorkItemWidgetDescription, Tag(_TAG_DESCRIPTION)],
        Annotated[WorkItemWidgetAssignees, Tag(_TAG_ASSIGNEES)],
        Annotated[WorkItemWidgetLabels, Tag(_TAG_LABELS)],
        Annotated[WorkItemWidgetNotes, Tag(_TAG_NOTES)],
        Annotated[WorkItemWidgetHierarchy, Tag(_TAG_HIERARCHY)],
        Annotated[WorkItemWidgetUnknown, Tag("unknown")],  # Fallback
    ],
    Discriminator(widget_discriminator),
]


class GitLabWorkItem(BaseModel):
    """Unified Work Item model (Issues, Merge Requests, Epics)."""

    id: str
    iid: int
    title: str
    state: str
    created_at: str = Field(alias="createdAt")
    updated_at: str = Field(alias="updatedAt")
    closed_at: str | None = Field(alias="closedAt", default=None)
    web_url: str = Field(alias="webUrl")
    author: GitLabUser | None = None
    work_item_type: dict[str, str] = Field(alias="workItemType")  # {"name": "Issue"}
    widgets: list[WorkItemWidget] = []  # Typed widgets, fetched separately in two-phase approach

    @property
    def type_name(self) -> str:
        """Get the work item type name (Issue, Task, Epic, etc)."""
        return self.work_item_type.get("name", "Unknown")

    model_config = {"populate_by_name": True}


class GitLabRelease(BaseModel):
    """GitLab release model."""

    tag_name: str = Field(alias="tagName")
    name: str | None = None
    description: str | None = None
    created_at: str = Field(alias="createdAt")
    released_at: str | None = Field(alias="releasedAt", default=None)
    author: GitLabUser | None = None
    commit: dict[str, Any] | None = None  # Contains sha, title, message
    milestones: dict[str, Any]  # Contains nodes with milestone info
    assets: dict[str, Any]  # Contains count and links

    model_config = {"populate_by_name": True}


# GraphQL query to fetch projects
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
      group {{
        id
        fullPath
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

# GraphQL query to fetch work items (Issues, Tasks, etc.) with full widgets
# Using reduced NODE_SIZE (50 instead of 100) to stay under GitLab's 250 complexity limit
WORK_ITEMS_PROJECT_QUERY = f"""
query($projectPath: ID!, $types: [IssueType!], $cursor: String) {{
  project(fullPath: $projectPath) {{
    workItems(types: $types, first: {WORK_ITEMS_NODE_SIZE}, after: $cursor) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        id
        iid
        title
        state
        createdAt
        updatedAt
        closedAt
        webUrl
        author {{
          username
          name
        }}
        workItemType {{
          name
        }}
        widgets {{
          __typename
          ... on WorkItemWidgetDescription {{
            description
          }}
          ... on WorkItemWidgetAssignees {{
            assignees(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                username
                name
              }}
            }}
          }}
          ... on WorkItemWidgetLabels {{
            labels(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                title
              }}
            }}
          }}
          ... on WorkItemWidgetNotes {{
            discussions(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                id
                notes(first: {WORK_ITEMS_NESTED_SIZE}) {{
                  pageInfo {{ hasNextPage endCursor }}
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
  }}
}}
"""

# GraphQL query to fetch full widgets for a single work item
# This query is run ONCE PER WORK ITEM to get all widget data with full pagination support
WORK_ITEM_FULL_QUERY = f"""
query($projectPath: ID!, $iid: String!, $workItemType: IssueType!) {{
  project(fullPath: $projectPath) {{
    workItems(iid: $iid, types: [$workItemType]) {{
      nodes {{
        widgets {{
          __typename
          ... on WorkItemWidgetDescription {{
            description
          }}
          ... on WorkItemWidgetAssignees {{
            assignees(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                username
                name
              }}
            }}
          }}
          ... on WorkItemWidgetLabels {{
            labels(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                title
              }}
            }}
          }}
          ... on WorkItemWidgetNotes {{
            discussions(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                id
                notes(first: {WORK_ITEMS_NESTED_SIZE}) {{
                  pageInfo {{ hasNextPage endCursor }}
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
  }}
}}
"""

# GraphQL query to fetch work items (Epics) for a group
# Minimal query - widgets are fetched separately to avoid complexity limits
WORK_ITEMS_GROUP_QUERY = f"""
query($groupPath: ID!, $types: [IssueType!], $cursor: String) {{
  group(fullPath: $groupPath) {{
    workItems(types: $types, first: {NODE_SIZE}, after: $cursor) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        id
        iid
        title
        state
        createdAt
        updatedAt
        closedAt
        webUrl
        author {{
          username
          name
        }}
        workItemType {{
          name
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch full widgets for a single group-level work item (e.g., Epic)
# This query is run ONCE PER WORK ITEM to get all widget data with full pagination support
WORK_ITEM_GROUP_FULL_QUERY = f"""
query($groupPath: ID!, $iid: String!, $workItemType: IssueType!) {{
  group(fullPath: $groupPath) {{
    workItems(iid: $iid, types: [$workItemType]) {{
      nodes {{
        widgets {{
          __typename
          ... on WorkItemWidgetDescription {{
            description
          }}
          ... on WorkItemWidgetAssignees {{
            assignees(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                username
                name
              }}
            }}
          }}
          ... on WorkItemWidgetLabels {{
            labels(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                title
              }}
            }}
          }}
          ... on WorkItemWidgetNotes {{
            discussions(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                id
                notes(first: {WORK_ITEMS_NESTED_SIZE}) {{
                  pageInfo {{ hasNextPage endCursor }}
                  nodes {{
                    id
                    body
                    createdAt
                    updatedAt
                    system
                    author {{
                      username
                      name
                    }}
                  }}
                }}
              }}
            }}
          }}
          ... on WorkItemWidgetHierarchy {{
            parent {{
              id
              iid
              title
            }}
            children(first: {NESTED_FIELD_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                id
                iid
                title
                workItemType {{
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

# GraphQL query to fetch remaining assignees for a work item
WORK_ITEM_ASSIGNEES_QUERY = f"""
query($projectPath: ID!, $iid: String!, $workItemType: IssueType!, $cursor: String) {{
  project(fullPath: $projectPath) {{
    workItems(iid: $iid, types: [$workItemType]) {{
      nodes {{
        widgets {{
          __typename
          ... on WorkItemWidgetAssignees {{
            assignees(first: {WORK_ITEMS_NESTED_SIZE}, after: $cursor) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
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
"""

# GraphQL query to fetch remaining labels for a work item
WORK_ITEM_LABELS_QUERY = f"""
query($projectPath: ID!, $iid: String!, $workItemType: IssueType!, $cursor: String) {{
  project(fullPath: $projectPath) {{
    workItems(iid: $iid, types: [$workItemType]) {{
      nodes {{
        widgets {{
          __typename
          ... on WorkItemWidgetLabels {{
            labels(first: {WORK_ITEMS_NESTED_SIZE}, after: $cursor) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                title
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch remaining discussions for a work item
WORK_ITEM_DISCUSSIONS_QUERY = f"""
query($projectPath: ID!, $iid: String!, $workItemType: IssueType!, $cursor: String) {{
  project(fullPath: $projectPath) {{
    workItems(iid: $iid, types: [$workItemType]) {{
      nodes {{
        widgets {{
          __typename
          ... on WorkItemWidgetNotes {{
            discussions(first: {NESTED_FIELD_SIZE}, after: $cursor) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                id
                notes(first: {NESTED_FIELD_SIZE}) {{
                  pageInfo {{ hasNextPage endCursor }}
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
  }}
}}
"""

# GraphQL query to fetch remaining discussions for a group-level work item (e.g., Epic)
WORK_ITEM_GROUP_DISCUSSIONS_QUERY = f"""
query($groupPath: ID!, $iid: String!, $workItemType: IssueType!, $cursor: String) {{
  group(fullPath: $groupPath) {{
    workItems(iid: $iid, types: [$workItemType]) {{
      nodes {{
        widgets {{
          __typename
          ... on WorkItemWidgetNotes {{
            discussions(first: {NESTED_FIELD_SIZE}, after: $cursor) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                id
                notes(first: {NESTED_FIELD_SIZE}) {{
                  pageInfo {{ hasNextPage endCursor }}
                  nodes {{
                    id
                    body
                    createdAt
                    updatedAt
                    system
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
  }}
}}
"""

# GraphQL query to fetch releases for a project
RELEASES_QUERY = f"""
query($projectPath: ID!, $cursor: String) {{
  project(fullPath: $projectPath) {{
    releases(first: {NODE_SIZE}, after: $cursor) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        tagName
        name
        description
        createdAt
        releasedAt
        author {{
          username
          name
        }}
        commit {{
          sha
          title
          message
        }}
        milestones {{
          nodes {{
            id
            title
          }}
        }}
        assets {{
          count
          links {{
            nodes {{
              name
              url
              linkType
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""


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

    def _extract_numeric_id(self, gid: str) -> int | None:
        """Extract numeric ID from GitLab global ID format.

        GitLab GraphQL returns global IDs like 'gid://gitlab/Project/123'.
        This method extracts the numeric portion.

        Args:
            gid: GitLab global ID string

        Returns:
            Numeric ID or None if extraction fails
        """
        try:
            return int(gid.split("/")[-1])
        except (ValueError, AttributeError, IndexError):
            self._logger.warning(f"Failed to parse GitLab ID: {gid}")
            return None

    def _get_session(self):
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=aiohttp.ClientTimeout(total=300),
            )
        return self._session

    async def _handle_rate_limit(self, response) -> bool:
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

    async def close(self) -> None:
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
    async def get_work_items_project(self, project_path, work_item_types):
        """Fetch work items for a project using Work Items API.

        Args:
            project_path (str): Full path of the project (e.g., 'namespace/project')
            work_item_types (list): List of work item type names (e.g., ['ISSUE', 'TASK'])

        Yields:
            GitLabWorkItem: Validated work item data
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
            except Exception as e:
                self._logger.warning(
                    f"Failed to fetch work items for {project_path}: {e}"
                )
                return

            project_data = result.get("project")
            if not project_data:
                return

            work_items_data = project_data.get("workItems", {})
            work_items = work_items_data.get("nodes", [])

            for work_item_data in work_items:
                work_item = GitLabWorkItem.model_validate(work_item_data)
                yield work_item

            # Check pagination
            page_info_data = work_items_data.get("pageInfo", {})
            page_info = PageInfo.model_validate(page_info_data)
            if not page_info.has_next_page:
                break

            cursor = page_info.end_cursor

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def fetch_work_item_widgets(self, project_path, iid, work_item_type):
        """Fetch full widget data for a single work item.

        This method is used to get all widget data (description, assignees, labels,
        discussions/notes) for a work item after fetching the basic list.

        Args:
            project_path (str): Full path of the project (e.g., 'namespace/project')
            iid (int): Work item internal ID
            work_item_type (str): Work item type (e.g., 'ISSUE', 'TASK')

        Returns:
            list[dict]: List of widgets for the work item, or empty list if not found
        """
        variables = {
            "projectPath": project_path,
            "iid": str(iid),
            "workItemType": work_item_type,
        }

        try:
            result = await self._execute_graphql(WORK_ITEM_FULL_QUERY, variables)
        except Exception as e:
            self._logger.warning(
                f"Failed to fetch widgets for work item {iid} in {project_path}: {e}"
            )
            return []

        project_data = result.get("project")
        if not project_data:
            return []

        work_items = project_data.get("workItems", {}).get("nodes", [])
        if not work_items:
            return []

        # Should only be one work item (filtered by iid)
        work_item = work_items[0]
        return work_item.get("widgets", [])

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def fetch_work_item_widgets_group(self, group_path, iid, work_item_type):
        """Fetch full widget data for a single group-level work item (e.g., Epic).

        This method is used to get all widget data (description, assignees, labels,
        discussions/notes, hierarchy) for a group-level work item after fetching the basic list.

        Args:
            group_path (str): Full path of the group (e.g., 'namespace/group')
            iid (int): Work item internal ID
            work_item_type (str): Work item type (e.g., 'EPIC')

        Returns:
            list[dict]: List of widgets for the work item, or empty list if not found
        """
        variables = {
            "groupPath": group_path,
            "iid": str(iid),
            "workItemType": work_item_type,
        }

        try:
            result = await self._execute_graphql(WORK_ITEM_GROUP_FULL_QUERY, variables)
        except Exception as e:
            self._logger.warning(
                f"Failed to fetch widgets for group work item {iid} in {group_path}: {e}"
            )
            return []

        group_data = result.get("group")
        if not group_data:
            return []

        work_items = group_data.get("workItems", {}).get("nodes", [])
        if not work_items:
            return []

        # Should only be one work item (filtered by iid)
        work_item = work_items[0]
        return work_item.get("widgets", [])

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_work_items_group(self, group_path, work_item_types):
        """Fetch work items for a group using Work Items API (for Epics).

        Args:
            group_path (str): Full path of the group (e.g., 'group/subgroup')
            work_item_types (list): List of work item type names (e.g., ['EPIC'])

        Yields:
            GitLabWorkItem: Validated work item data
        """
        cursor = None

        while True:
            variables = {"groupPath": group_path, "types": work_item_types}
            if cursor:
                variables["cursor"] = cursor

            try:
                result = await self._execute_graphql(WORK_ITEMS_GROUP_QUERY, variables)
            except Exception as e:
                self._logger.warning(
                    f"Failed to fetch work items for group {group_path}: {e}"
                )
                return

            group_data = result.get("group")
            if not group_data:
                return

            work_items_data = group_data.get("workItems", {})
            work_items = work_items_data.get("nodes", [])

            for work_item_data in work_items:
                work_item = GitLabWorkItem.model_validate(work_item_data)
                yield work_item

            # Check pagination
            page_info_data = work_items_data.get("pageInfo", {})
            page_info = PageInfo.model_validate(page_info_data)
            if not page_info.has_next_page:
                break

            cursor = page_info.end_cursor

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_releases(self, project_path):
        """Fetch releases for a project using GraphQL.

        Args:
            project_path (str): Full path of the project (e.g., 'namespace/project')

        Yields:
            GitLabRelease: Validated release data
        """
        cursor = None

        while True:
            variables = {"projectPath": project_path}
            if cursor:
                variables["cursor"] = cursor

            try:
                result = await self._execute_graphql(RELEASES_QUERY, variables)
            except Exception as e:
                self._logger.warning(
                    f"Failed to fetch releases for {project_path}: {e}"
                )
                return

            project_data = result.get("project")
            if not project_data:
                return

            releases_data = project_data.get("releases", {})
            releases = releases_data.get("nodes", [])

            for release_data in releases:
                release = GitLabRelease.model_validate(release_data)
                yield release

            # Check pagination
            page_info_data = releases_data.get("pageInfo", {})
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

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def fetch_remaining_work_item_assignees(
        self, project_path, iid, work_item_type, cursor
    ):
        """Fetch remaining assignees for a work item.

        Args:
            project_path (str): Full path of the project
            iid (int): Work item internal ID
            work_item_type (str): Work item type (e.g., 'ISSUE', 'TASK')
            cursor (str): Pagination cursor

        Yields:
            dict: Assignee data
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
                return

            project_data = result.get("project")
            if not project_data:
                return

            work_items = project_data.get("workItems", {}).get("nodes", [])
            if not work_items:
                return

            # Should only be one work item (filtered by iid)
            work_item = work_items[0]
            widgets = work_item.get("widgets", [])

            # Find the Assignees widget
            assignees_data = None
            for widget in widgets:
                if widget.get("__typename") == "WorkItemWidgetAssignees":
                    assignees_data = widget.get("assignees", {})
                    break

            if not assignees_data:
                return

            assignees = assignees_data.get("nodes", [])
            for assignee in assignees:
                yield assignee

            # Check pagination
            page_info = assignees_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def fetch_remaining_work_item_labels(
        self, project_path, iid, work_item_type, cursor
    ):
        """Fetch remaining labels for a work item.

        Args:
            project_path (str): Full path of the project
            iid (int): Work item internal ID
            work_item_type (str): Work item type (e.g., 'ISSUE', 'TASK')
            cursor (str): Pagination cursor

        Yields:
            dict: Label data
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
                return

            project_data = result.get("project")
            if not project_data:
                return

            work_items = project_data.get("workItems", {}).get("nodes", [])
            if not work_items:
                return

            # Should only be one work item (filtered by iid)
            work_item = work_items[0]
            widgets = work_item.get("widgets", [])

            # Find the Labels widget
            labels_data = None
            for widget in widgets:
                if widget.get("__typename") == "WorkItemWidgetLabels":
                    labels_data = widget.get("labels", {})
                    break

            if not labels_data:
                return

            labels = labels_data.get("nodes", [])
            for label in labels:
                yield label

            # Check pagination
            page_info = labels_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def fetch_remaining_work_item_discussions(
        self, project_path, iid, work_item_type, cursor
    ):
        """Fetch remaining discussions for a work item.

        Args:
            project_path (str): Full path of the project
            iid (int): Work item internal ID
            work_item_type (str): Work item type (e.g., 'ISSUE', 'TASK')
            cursor (str): Pagination cursor

        Yields:
            dict: Discussion data with notes
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
                return

            project_data = result.get("project")
            if not project_data:
                return

            work_items = project_data.get("workItems", {}).get("nodes", [])
            if not work_items:
                return

            # Should only be one work item (filtered by iid)
            work_item = work_items[0]
            widgets = work_item.get("widgets", [])

            # Find the Notes widget
            discussions_data = None
            for widget in widgets:
                if widget.get("__typename") == "WorkItemWidgetNotes":
                    discussions_data = widget.get("discussions", {})
                    break

            if not discussions_data:
                return

            discussions = discussions_data.get("nodes", [])
            for discussion in discussions:
                yield discussion

            # Check pagination
            page_info = discussions_data.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def fetch_remaining_work_item_group_discussions(
        self, group_path, iid, work_item_type, cursor
    ):
        """Fetch remaining discussions for a group-level work item (e.g., Epic).

        Args:
            group_path (str): Full path of the group
            iid (int): Work item internal ID
            work_item_type (str): Work item type (e.g., 'EPIC')
            cursor (str): Pagination cursor

        Yields:
            dict: Discussion data with notes
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
                return

            group_data = result.get("group")
            if not group_data:
                return

            work_items = group_data.get("workItems", {}).get("nodes", [])
            if not work_items:
                return

            # Should only be one work item (filtered by iid)
            work_item = work_items[0]
            widgets = work_item.get("widgets", [])

            # Find the Notes widget
            discussions_data = None
            for widget in widgets:
                if widget.get("__typename") == "WorkItemWidgetNotes":
                    discussions_data = widget.get("discussions", {})
                    break

            if not discussions_data:
                return

            discussions = discussions_data.get("nodes", [])
            for discussion in discussions:
                yield discussion

            # Check pagination
            page_info = discussions_data.get("pageInfo", {})
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

    async def ping(self) -> None:
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

    def __init__(self, configuration) -> None:
        """Setup the connection to the GitLab instance.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
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
                "tooltip": "List of project paths (e.g., 'group/project'). Use '*' to sync all accessible projects.",
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
        # Test authentication
        try:
            await self.gitlab_client.ping()
        except Exception as e:
            msg = f"Failed to authenticate with GitLab: {e}"
            raise ConfigurableFieldValueError(msg) from e

        # If specific projects are configured, validate they exist and are accessible
        if self.configured_projects and self.configured_projects != ["*"]:
            await self._validate_configured_projects()

    async def _validate_configured_projects(self) -> None:
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
        # If no projects configured or wildcard, sync all
        if not self.configured_projects or "*" in self.configured_projects:
            return True

        # Check if project is in the configured list
        return project_path in self.configured_projects


    def _extract_widget_description(self, work_item: GitLabWorkItem) -> str | None:
        """Extract description from work item widgets."""
        for widget in work_item.widgets:
            if isinstance(widget, WorkItemWidgetDescription):
                return widget.description
        return None

    def _extract_widget_assignees(
        self, work_item: GitLabWorkItem
    ) -> PaginatedList[GitLabUser]:
        """Extract assignees data from work item widgets."""
        for widget in work_item.widgets:
            if isinstance(widget, WorkItemWidgetAssignees):
                return widget.assignees
        return PaginatedList[GitLabUser](nodes=[])

    def _extract_widget_labels(self, work_item: GitLabWorkItem) -> PaginatedList[GitLabLabel]:
        """Extract labels data from work item widgets."""
        for widget in work_item.widgets:
            if isinstance(widget, WorkItemWidgetLabels):
                return widget.labels
        return PaginatedList[GitLabLabel](nodes=[])

    def _extract_widget_discussions(self, work_item: GitLabWorkItem) -> PaginatedList[GitLabDiscussion]:
        """Extract discussions from work item widgets."""
        for widget in work_item.widgets:
            if isinstance(widget, WorkItemWidgetNotes):
                return widget.discussions
        # Return empty paginated list if not found
        return PaginatedList[GitLabDiscussion](nodes=[])

    def _extract_widget_hierarchy(
        self, work_item: GitLabWorkItem
    ) -> WorkItemWidgetHierarchy | None:
        """Extract hierarchy info from work item widgets (for Epics)."""
        for widget in work_item.widgets:
            if isinstance(widget, WorkItemWidgetHierarchy):
                return widget
        return None

    async def _fetch_remaining_assignees(
        self,
        issuable: GitLabIssue | GitLabMergeRequest,
        project_path: str,
        issuable_type: str,
    ) -> None:
        """Fetch remaining assignees for an issue or MR."""
        if issuable.assignees.page_info.has_next_page:
            cursor = issuable.assignees.page_info.end_cursor
            async for assignee in self.gitlab_client.fetch_remaining_field(
                project_path, issuable.iid, "assignees", issuable_type, cursor
            ):
                issuable.assignees.nodes.append(GitLabUser.model_validate(assignee))

    async def _fetch_remaining_labels(
        self,
        issuable: GitLabIssue | GitLabMergeRequest,
        project_path: str,
        issuable_type: str,
    ) -> None:
        """Fetch remaining labels for an issue or MR."""
        if issuable.labels.page_info.has_next_page:
            cursor = issuable.labels.page_info.end_cursor
            async for label in self.gitlab_client.fetch_remaining_field(
                project_path, issuable.iid, "labels", issuable_type, cursor
            ):
                issuable.labels.nodes.append(GitLabLabel.model_validate(label))

    async def _fetch_remaining_discussions(
        self,
        issuable: GitLabIssue | GitLabMergeRequest,
        project_path: str,
        issuable_type: str,
    ) -> None:
        """Fetch remaining discussions for an issue or MR."""
        if issuable.discussions.page_info.has_next_page:
            cursor = issuable.discussions.page_info.end_cursor
            async for discussion in self.gitlab_client.fetch_remaining_field(
                project_path, issuable.iid, "discussions", issuable_type, cursor
            ):
                issuable.discussions.nodes.append(GitLabDiscussion.model_validate(discussion))

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
        if mr.reviewers.page_info.has_next_page:
            cursor = mr.reviewers.page_info.end_cursor
            async for reviewer in self.gitlab_client.fetch_remaining_field(
                project_path, mr.iid, "reviewers", "mergeRequest", cursor
            ):
                mr.reviewers.nodes.append(GitLabUser.model_validate(reviewer))

        if mr.approved_by.page_info.has_next_page:
            cursor = mr.approved_by.page_info.end_cursor
            async for approver in self.gitlab_client.fetch_remaining_field(
                project_path, mr.iid, "approvedBy", "mergeRequest", cursor
            ):
                mr.approved_by.nodes.append(GitLabUser.model_validate(approver))

    async def get_docs(self, filtering=None):
        """Main method to fetch documents from GitLab using Work Items API.

        Args:
            filtering (Filtering, optional): Filtering rules. Defaults to None.

        Yields:
            tuple: (document dict, download function or None)
        """
        seen_groups = set()  # Track groups we've already fetched epics from

        # Fetch projects via GraphQL
        async for project in self.gitlab_client.get_projects():
            # Extract project ID (GraphQL returns global ID, need numeric ID)
            project_id = self.gitlab_client._extract_numeric_id(project.id)

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

            # Fetch Issues using Work Items API
            # Single query with reduced node size (50 instead of 100) to avoid complexity limits
            async for work_item in self.gitlab_client.get_work_items_project(
                project.full_path, [WorkItemType.ISSUE]
            ):
                # Extract widget data for assignees and labels
                assignees_data = self._extract_widget_assignees(work_item)
                labels_data = self._extract_widget_labels(work_item)

                # Fetch remaining assignees if paginated
                if assignees_data.page_info.has_next_page:
                    cursor = assignees_data.page_info.end_cursor
                    async for assignee in self.gitlab_client.fetch_remaining_work_item_assignees(
                        project.full_path, work_item.iid, WorkItemType.ISSUE, cursor
                    ):
                        assignees_data.nodes.append(GitLabUser.model_validate(assignee))

                # Fetch remaining labels if paginated
                if labels_data.page_info.has_next_page:
                    cursor = labels_data.page_info.end_cursor
                    async for label in self.gitlab_client.fetch_remaining_work_item_labels(
                        project.full_path, work_item.iid, WorkItemType.ISSUE, cursor
                    ):
                        labels_data.nodes.append(GitLabLabel.model_validate(label))

                # Update work item with complete data
                work_item_doc = self._format_work_item_doc(
                    work_item, project=project, assignees_data=assignees_data, labels_data=labels_data
                )

                # Extract notes from work item widgets
                discussions_data = self._extract_widget_discussions(work_item)

                # Check if there are more discussions to fetch
                if discussions_data.page_info.has_next_page:
                    cursor = discussions_data.page_info.end_cursor
                    # Fetch remaining discussions and append them
                    async for (
                        discussion
                    ) in self.gitlab_client.fetch_remaining_work_item_discussions(
                        project.full_path, work_item.iid, WorkItemType.ISSUE, cursor
                    ):
                        discussions_data.nodes.append(GitLabDiscussion.model_validate(discussion))

                notes = await self._extract_notes_from_discussions(
                    discussions_data,
                    project.full_path,
                    work_item.iid,
                    "issue",
                )
                work_item_doc["notes"] = notes

                yield work_item_doc, None

            # Note: Merge Requests are not work items
            async for mr in self.gitlab_client.get_merge_requests(project.full_path):
                await self._fetch_remaining_mr_fields(mr, project.full_path)
                mr_doc = self._format_merge_request_doc(mr, project)

                notes = await self._extract_notes_from_discussions(
                    mr.discussions,
                    project.full_path,
                    mr.iid,
                    "mergeRequest",
                )
                mr_doc["notes"] = notes

                yield mr_doc, None

            # Fetch Releases for this project
            async for release in self.gitlab_client.get_releases(project.full_path):
                release_doc = self._format_release_doc(release, project)
                yield release_doc, None

            # Fetch Epics if this project belongs to a group
            if project.group and project.group.get("fullPath"):
                group_path = project.group["fullPath"]

                # Only fetch epics once per group
                if group_path not in seen_groups:
                    seen_groups.add(group_path)
                    self._logger.debug(f"Fetching epics for group: {group_path}")

                    try:
                        # Two-phase approach for epics (same as issues)
                        # 1. Get list of epics (minimal query)
                        # 2. Fetch full widget data for each epic separately
                        async for epic in self.gitlab_client.get_work_items_group(
                            group_path, [WorkItemType.EPIC]
                        ):
                            # Fetch full widget data for this epic
                            widgets = (
                                await self.gitlab_client.fetch_work_item_widgets_group(
                                    group_path, epic.iid, WorkItemType.EPIC
                                )
                            )

                            # Update epic with full widget data
                            epic.widgets = widgets

                            epic_doc = self._format_work_item_doc(
                                epic, group_path=group_path
                            )

                            # Extract notes from epic widgets
                            discussions_data = self._extract_widget_discussions(epic)

                            # Check if there are more discussions to fetch
                            if discussions_data.page_info.has_next_page:
                                cursor = discussions_data.page_info.end_cursor
                                # Fetch remaining discussions and append them
                                async for (
                                    discussion
                                ) in self.gitlab_client.fetch_remaining_work_item_group_discussions(
                                    group_path, epic.iid, WorkItemType.EPIC, cursor
                                ):
                                    discussions_data.nodes.append(GitLabDiscussion.model_validate(discussion))

                            # Extract notes from discussions using the common method
                            notes = await self._extract_notes_from_discussions(
                                discussions_data,
                                group_path,
                                epic.iid,
                                "epic",
                            )
                            epic_doc["notes"] = notes
                            yield epic_doc, None
                    except Exception as e:
                        # Epics require Premium/Ultimate, may fail on Free tier
                        self._logger.warning(
                            f"Failed to fetch epics for group {group_path}: {e}. "
                            "This may be because epics require GitLab Premium or Ultimate tier."
                        )

            # Fetch README files via REST
            async for readme_doc, download_func in self._fetch_readme_files(
                project_id, project
            ):
                yield readme_doc, download_func

    def _format_project_doc(self, project: GitLabProject) -> dict[str, Any]:
        """Format project data into Elasticsearch document.

        Args:
            project (GitLabProject): Validated project model

        Returns:
            dict: Formatted project document
        """
        project_id = self.gitlab_client._extract_numeric_id(project.id) or project.id

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

            # Add all notes from first fetch
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
                if note.position:
                    note_dict["position"] = note.position
                notes.append(note_dict)

            # Check if there are more notes to fetch for this discussion
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

    def _format_merge_request_doc(self, mr: GitLabMergeRequest, project: GitLabProject):
        """Format merge request data into Elasticsearch document.

        Args:
            mr (GitLabMergeRequest): Validated merge request model
            project (GitLabProject): Parent project model

        Returns:
            dict: Formatted merge request document
        """
        project_id = self.gitlab_client._extract_numeric_id(project.id) or project.id

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
            "assignees": [a.username for a in mr.assignees.nodes],
            "reviewers": [r.username for r in mr.reviewers.nodes],
            "approved_by": [a.username for a in mr.approved_by.nodes],
            "merged_by": mr.merged_by.username if mr.merged_by else None,
            "labels": [label.title for label in mr.labels.nodes],
        }

    def _format_work_item_doc(
        self,
        work_item: GitLabWorkItem,
        project: GitLabProject | None = None,
        group_path: str | None = None,
        assignees_data: PaginatedList[GitLabUser] | None = None,
        labels_data: PaginatedList[GitLabLabel] | None = None,
    ) -> dict[str, Any]:
        """Format work item data into Elasticsearch document.

        Args:
            work_item: Work item model (Issue, Task, Epic, etc.)
            project: Parent project (for Issues/MRs) or None (for Epics)
            group_path: Group path (for Epics) or None (for Issues/MRs)
            assignees_data: Pre-fetched assignees data (with pagination) or None to extract from widgets
            labels_data: Pre-fetched labels data (with pagination) or None to extract from widgets

        Returns:
            Formatted work item document
        """
        # Extract widgets
        description = self._extract_widget_description(work_item)

        # Use provided data or extract from widgets
        if assignees_data is None:
            assignees_data = self._extract_widget_assignees(work_item)
        if labels_data is None:
            labels_data = self._extract_widget_labels(work_item)

        hierarchy = self._extract_widget_hierarchy(work_item)

        # Determine parent ID (project or group)
        if project:
            parent_id = self.gitlab_client._extract_numeric_id(project.id) or project.id
            parent_path = project.full_path
            parent_type = "project"
        elif group_path:
            # For epics, we need to extract group ID from somewhere
            # For now, use the group_path as the ID
            parent_id = group_path.replace("/", "_")
            parent_path = group_path
            parent_type = "group"
        else:
            parent_id = "unknown"
            parent_path = "unknown"
            parent_type = "unknown"

        doc = {
            "_id": f"{work_item.type_name.lower().replace(' ', '_')}_{parent_id}_{work_item.iid}",
            "_timestamp": work_item.updated_at,
            "type": work_item.type_name,
            f"{parent_type}_id": parent_id,
            f"{parent_type}_path": parent_path,
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
        }

        # Add hierarchy info for Epics
        if hierarchy:
            if hierarchy.parent:
                doc["parent_epic_iid"] = hierarchy.parent.get("iid")
                doc["parent_epic_title"] = hierarchy.parent.get("title")

            children = hierarchy.children.nodes
            doc["child_count"] = len(children)
            doc["child_issue_count"] = sum(
                1 for c in children if c.get("workItemType", {}).get("name") == "Issue"
            )
            doc["child_epic_count"] = sum(
                1 for c in children if c.get("workItemType", {}).get("name") == "Epic"
            )

        return doc

    def _format_release_doc(
        self, release: GitLabRelease, project: GitLabProject
    ) -> dict[str, Any]:
        """Format release data into Elasticsearch document.

        Args:
            release: Release model
            project: Parent project model

        Returns:
            Formatted release document
        """
        project_id = self.gitlab_client._extract_numeric_id(project.id) or project.id

        # Extract milestone info
        milestones = release.milestones.get("nodes", [])
        milestone_titles = [m.get("title") for m in milestones]

        # Extract asset links
        asset_links = release.assets.get("links", {}).get("nodes", [])

        doc = {
            "_id": f"release_{project_id}_{release.tag_name}",
            "_timestamp": release.released_at or release.created_at,
            "type": "Release",
            "project_id": project_id,
            "project_path": project.full_path,
            "tag_name": release.tag_name,
            "name": release.name,
            "description": release.description,
            "created_at": release.created_at,
            "released_at": release.released_at,
            "author": release.author.username if release.author else None,
            "author_name": release.author.name if release.author else None,
            "milestones": milestone_titles,
            "asset_count": release.assets.get("count", 0),
        }

        # Add commit info if available
        if release.commit:
            doc["commit_sha"] = release.commit.get("sha")
            doc["commit_title"] = release.commit.get("title")

        # Add asset links
        if asset_links:
            doc["asset_links"] = [
                {
                    "name": link.get("name"),
                    "url": link.get("url"),
                    "type": link.get("linkType"),
                }
                for link in asset_links
            ]

        return doc

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
