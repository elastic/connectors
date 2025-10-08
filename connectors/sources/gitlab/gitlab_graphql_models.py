#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Pydantic models for GitLab GraphQL API responses."""

from enum import Enum
from typing import Annotated, Any, Generic, Literal, TypeVar, Union, get_args

from pydantic import BaseModel, Discriminator, Field, Tag


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


class GitLabPosition(BaseModel):
    """GitLab note position for diff/inline comments."""

    new_line: int | None = Field(alias="newLine", default=None)
    old_line: int | None = Field(alias="oldLine", default=None)
    new_path: str | None = Field(alias="newPath", default=None)
    old_path: str | None = Field(alias="oldPath", default=None)
    position_type: str | None = Field(alias="positionType", default=None)

    model_config = {"populate_by_name": True}


class GitLabNote(BaseModel):
    """GitLab note (comment) model."""

    id: str
    body: str
    created_at: str = Field(alias="createdAt")
    updated_at: str = Field(alias="updatedAt")
    author: GitLabUser | None = None
    system: bool = False  # System-generated notes (status changes, etc.)
    position: GitLabPosition | None = None  # For diff/inline comments

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
    children: PaginatedList[dict[str, Any]] = Field(
        default_factory=lambda: PaginatedList(nodes=[])
    )

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


class GitLabRepository(BaseModel):
    """GitLab repository model for projects."""

    root_ref: str | None = Field(alias="rootRef", default=None)

    model_config = {"populate_by_name": True}


class GitLabGroup(BaseModel):
    """GitLab group model for projects."""

    id: str
    full_path: str = Field(alias="fullPath")

    model_config = {"populate_by_name": True}


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
    repository: GitLabRepository | None = None
    group: GitLabGroup | None = None

    @property
    def default_branch(self) -> str | None:
        """Extract default branch from repository."""
        if self.repository:
            return self.repository.root_ref
        return None

    model_config = {"populate_by_name": True}


# Extract tag values from Literal type annotations
# This ensures tags stay in sync with the model definitions
_TAG_DESCRIPTION = get_args(
    WorkItemWidgetDescription.model_fields["type_name"].annotation
)[0]
_TAG_ASSIGNEES = get_args(WorkItemWidgetAssignees.model_fields["type_name"].annotation)[
    0
]
_TAG_LABELS = get_args(WorkItemWidgetLabels.model_fields["type_name"].annotation)[0]
_TAG_NOTES = get_args(WorkItemWidgetNotes.model_fields["type_name"].annotation)[0]
_TAG_HIERARCHY = get_args(WorkItemWidgetHierarchy.model_fields["type_name"].annotation)[
    0
]

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


class WorkItemTypeInfo(BaseModel):
    """Work item type information."""

    name: str


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
    work_item_type: WorkItemTypeInfo = Field(alias="workItemType")
    widgets: list[
        WorkItemWidget
    ] = []  # Typed widgets, fetched separately in two-phase approach

    @property
    def type_name(self) -> str:
        """Get the work item type name (Issue, Task, Epic, etc)."""
        return self.work_item_type.name

    model_config = {"populate_by_name": True}


class GitLabCommit(BaseModel):
    """GitLab commit model for releases."""

    sha: str
    title: str | None = None
    message: str | None = None


class GitLabMilestone(BaseModel):
    """GitLab milestone model."""

    id: str
    title: str


class GitLabAssetLink(BaseModel):
    """GitLab release asset link model."""

    name: str
    url: str
    link_type: str | None = Field(alias="linkType", default=None)

    model_config = {"populate_by_name": True}


class GitLabAssets(BaseModel):
    """GitLab release assets model."""

    count: int
    links: PaginatedList[GitLabAssetLink]


class GitLabRelease(BaseModel):
    """GitLab release model."""

    tag_name: str = Field(alias="tagName")
    name: str | None = None
    description: str | None = None
    created_at: str = Field(alias="createdAt")
    released_at: str | None = Field(alias="releasedAt", default=None)
    author: GitLabUser | None = None
    commit: GitLabCommit | None = None
    milestones: PaginatedList[GitLabMilestone]
    assets: GitLabAssets

    model_config = {"populate_by_name": True}
